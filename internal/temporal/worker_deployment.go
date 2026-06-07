// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	temporalClient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	IgnoreLastModifierKey = "temporal.io/ignore-last-modifier"
)

// VersionInfo contains information about a specific version
type VersionInfo struct {
	DeploymentName string
	BuildID        string
	Status         temporaliov1alpha1.VersionStatus
	DrainedSince   *time.Time
	TaskQueues     []temporaliov1alpha1.TaskQueue
	TestWorkflows  []temporaliov1alpha1.WorkflowExecution

	// True if all task queues in this version have at least one unversioned poller.
	// False could just mean unknown / not checked / not checked successfully.
	// Only checked for Target Version when Current Version is nil and strategy is Progressive.
	// Used to decide whether to fast track the rollout; rollout will be AllAtOnce if:
	//   - Current Version is nil
	//   - Strategy is Progressive, and
	//   - Presence of unversioned pollers in all task queues of target version cannot be confirmed.
	AllTaskQueuesHaveUnversionedPoller bool
	// True if all task queues in this version have no versioned pollers.
	// False could just mean unknown / not checked / not checked successfully.
	// Only checked for Drained versions that don't have controller-managed Deployments.
	// Used to compute status.VersionCountIneligibleForDeletion.
	NoTaskQueuesHaveVersionedPoller bool

	// Backlog is the approximate number of pending workflow + activity tasks
	// across this version's task queues. Only collected for Draining versions
	// (used by the planner to decide whether the version can be scaled down
	// to a single poller). Nil means unknown / not collected.
	Backlog *int64
}

// TemporalWorkerState represents the state of a worker deployment in Temporal
type TemporalWorkerState struct {
	CurrentBuildID       string
	VersionConflictToken []byte
	RampingBuildID       string
	RampPercentage       float32
	// RampingSince is the time when the current ramping version was set.
	RampingSince       *metav1.Time
	RampLastModifiedAt *metav1.Time
	// Versions indexed by build ID
	Versions             map[string]*VersionInfo
	LastModifierIdentity string
	IgnoreLastModifier   bool
}

// GetWorkerDeploymentState queries Temporal to get the state of a worker deployment
func GetWorkerDeploymentState(
	ctx context.Context,
	client temporalClient.Client,
	workerDeploymentName string,
	namespace string,
	k8sDeployments map[string]*appsv1.Deployment,
	targetBuildId string,
	strategy temporaliov1alpha1.DefaultVersionUpdateStrategy,
	controllerIdentity string,
) (*TemporalWorkerState, error) {
	state := &TemporalWorkerState{
		Versions: make(map[string]*VersionInfo),
	}

	// Get deployment handler
	deploymentHandler := client.WorkerDeploymentClient().GetHandle(workerDeploymentName)

	// Describe the worker deployment
	resp, err := deploymentHandler.Describe(ctx, temporalClient.WorkerDeploymentDescribeOptions{})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			// If deployment not found, return empty state. Need to scale up workers in order to create Deployment Temporal-side
			return state, nil
		}
		return nil, fmt.Errorf("unable to describe worker deployment %s: %w", workerDeploymentName, err)
	}

	workerDeploymentInfo := resp.Info
	routingConfig := workerDeploymentInfo.RoutingConfig

	// Set basic information
	if routingConfig.CurrentVersion != nil {
		state.CurrentBuildID = routingConfig.CurrentVersion.BuildId
	}
	if routingConfig.RampingVersion != nil {
		state.RampingBuildID = routingConfig.RampingVersion.BuildId
	}
	state.RampPercentage = routingConfig.RampingVersionPercentage
	state.LastModifierIdentity = workerDeploymentInfo.LastModifierIdentity
	state.VersionConflictToken = resp.ConflictToken

	// Decide whether to ignore LastModifierIdentity
	if state.LastModifierIdentity != controllerIdentity && state.LastModifierIdentity != "" {
		state.IgnoreLastModifier, err = DeploymentShouldIgnoreLastModifier(ctx, deploymentHandler, routingConfig)
		if err != nil {
			return nil, err
		}
	}

	// TODO(jlegrone): Re-enable stats once available in versioning v3.

	// Set ramping since time if applicable
	if routingConfig.RampingVersion != nil {
		var (
			rampingSinceTime   = metav1.NewTime(routingConfig.RampingVersionChangedTime)
			lastRampUpdateTime = metav1.NewTime(routingConfig.RampingVersionPercentageChangedTime)
		)
		state.RampingSince = &rampingSinceTime
		state.RampLastModifiedAt = &lastRampUpdateTime
	}

	// Process each version
	for _, version := range workerDeploymentInfo.VersionSummaries {
		versionInfo := &VersionInfo{
			DeploymentName: version.Version.DeploymentName,
			BuildID:        version.Version.BuildId,
		}

		// Determine version status
		drainageStatus := version.DrainageStatus
		if routingConfig.CurrentVersion != nil &&
			version.Version.DeploymentName == routingConfig.CurrentVersion.DeploymentName &&
			version.Version.BuildId == routingConfig.CurrentVersion.BuildId {
			versionInfo.Status = temporaliov1alpha1.VersionStatusCurrent
		} else if routingConfig.RampingVersion != nil &&
			version.Version.DeploymentName == routingConfig.RampingVersion.DeploymentName &&
			version.Version.BuildId == routingConfig.RampingVersion.BuildId {
			versionInfo.Status = temporaliov1alpha1.VersionStatusRamping
		} else if drainageStatus == temporalClient.WorkerDeploymentVersionDrainageStatusDraining {
			versionInfo.Status = temporaliov1alpha1.VersionStatusDraining
		} else if drainageStatus == temporalClient.WorkerDeploymentVersionDrainageStatusDrained {
			versionInfo.Status = temporaliov1alpha1.VersionStatusDrained

			// Get drain time information
			var desc temporalClient.WorkerDeploymentVersionDescription
			describeVersionUntilDrainTime := func() error {
				desc, err = deploymentHandler.DescribeVersion(ctx, temporalClient.WorkerDeploymentDescribeVersionOptions{
					BuildID: version.Version.BuildId,
				})
				if err != nil {
					return err
				}
				if desc.Info.DrainageInfo == nil {
					return fmt.Errorf("drainage info nil for build %s", version.Version.BuildId)
				}
				if desc.Info.DrainageInfo.DrainageStatus != temporalClient.WorkerDeploymentVersionDrainageStatusDrained {
					return fmt.Errorf("version info does not say that build %s is drained", version.Version.BuildId)
				}
				return err
			}
			// At first, version is found in DeploymentInfo.VersionSummaries but may not have the full drainage info in
			// describe version, so we describe with backoff.
			// If the version was just deleted by the server, we may never succeed at describing it, and it should
			// be treated as NotRegistered, since it no longer exists in Temporal.
			var notFound *serviceerror.NotFound
			if err = withBackoff(10*time.Second, 1*time.Second, describeVersionUntilDrainTime); err == nil { //revive:disable-line:max-control-nesting
				drainedSince := desc.Info.DrainageInfo.LastChangedTime
				versionInfo.DrainedSince = &drainedSince
				// If the deployment exists and has replicas, we assume there are versioned pollers, no need to check
				deployment, ok := k8sDeployments[version.Version.BuildId]
				if !ok || deployment.Status.Replicas == 0 { //revive:disable-line:max-control-nesting
					versionInfo.NoTaskQueuesHaveVersionedPoller = noTaskQueuesHaveVersionedPollers(ctx, client, desc.Info.TaskQueuesInfos)
				}
			} else if errors.As(err, &notFound) { //revive:disable-line:max-control-nesting
				versionInfo.Status = temporaliov1alpha1.VersionStatusNotRegistered
			}
		} else {
			versionInfo.Status = temporaliov1alpha1.VersionStatusInactive
			// get unversioned poller info to decide whether to fast-track rollout
			if version.Version.BuildId == targetBuildId &&
				routingConfig.CurrentVersion == nil &&
				strategy == temporaliov1alpha1.UpdateProgressive {
				var desc temporalClient.WorkerDeploymentVersionDescription
				describeVersion := func() error {
					desc, err = deploymentHandler.DescribeVersion(ctx, temporalClient.WorkerDeploymentDescribeVersionOptions{
						BuildID: version.Version.BuildId,
					})
					return err
				}
				// At first, version is found in DeploymentInfo.VersionSummaries but not ready for describe, so we have
				// to describe with backoff.
				//
				// Note: We can only check whether the task queues that we know of have unversioned pollers.
				//       If, later on, a poll request arrives tying a new task queue to the target version, we
				//       don't know whether that task queue has unversioned pollers.
				if err = withBackoff(10*time.Second, 1*time.Second, describeVersion); err == nil { //revive:disable-line:max-control-nesting
					versionInfo.AllTaskQueuesHaveUnversionedPoller = allTaskQueuesHaveUnversionedPoller(ctx, client, desc.Info.TaskQueuesInfos)
				}
			}

		}

		state.Versions[version.Version.BuildId] = versionInfo
	}

	collectDrainingVersionBacklogs(ctx, client, deploymentHandler, state, k8sDeployments)

	return state, nil
}

// backlogCollectionBudget bounds how long a single reconcile may spend
// gathering draining-version backlogs. Backlog data is best-effort: when the
// budget is exceeded the affected versions keep a nil (unknown) Backlog and
// the planner conservatively leaves their replicas untouched until a later
// reconcile succeeds.
const backlogCollectionBudget = 2 * time.Second

// collectDrainingVersionBacklogs fetches the approximate task backlog for
// Draining versions so the planner can scale idle ones down to a single
// poller. Fetches run in parallel under a shared time budget so they can
// never stall the reconcile loop, and are skipped entirely for versions
// whose deployment is already at <=1 replica (nothing to scale down).
func collectDrainingVersionBacklogs(
	ctx context.Context,
	client temporalClient.Client,
	deploymentHandler temporalClient.WorkerDeploymentHandle,
	state *TemporalWorkerState,
	k8sDeployments map[string]*appsv1.Deployment,
) {
	budgetCtx, cancel := context.WithTimeout(ctx, backlogCollectionBudget)
	defer cancel()

	var wg sync.WaitGroup
	for buildID, versionInfo := range state.Versions {
		if versionInfo.Status != temporaliov1alpha1.VersionStatusDraining {
			continue
		}
		// Only relevant when there is something to scale down.
		d, exists := k8sDeployments[buildID]
		if !exists || d.Spec.Replicas == nil || *d.Spec.Replicas <= 1 {
			continue
		}

		wg.Add(1)
		go func(buildID string, vi *VersionInfo) {
			defer wg.Done()
			if backlog, err := getVersionBacklog(budgetCtx, client, deploymentHandler, buildID); err == nil {
				vi.Backlog = &backlog
			}
		}(buildID, versionInfo)
	}
	wg.Wait()
}

// getVersionBacklog sums the approximate pending workflow + activity task
// counts across all task queues of a specific worker deployment version.
// Zero backlog means the version's pinned workflows are idle (e.g. waiting
// on child workflows or timers) and the version can safely run on a single
// poller while it drains.
func getVersionBacklog(
	ctx context.Context,
	client temporalClient.Client,
	deploymentHandler temporalClient.WorkerDeploymentHandle,
	buildID string,
) (int64, error) {
	desc, err := deploymentHandler.DescribeVersion(ctx, temporalClient.WorkerDeploymentDescribeVersionOptions{
		BuildID: buildID,
	})
	if err != nil {
		return 0, fmt.Errorf("unable to describe version %q: %w", buildID, err)
	}

	seen := make(map[string]struct{})
	var total int64
	for _, tq := range desc.Info.TaskQueuesInfos {
		if _, ok := seen[tq.Name]; ok {
			continue
		}
		seen[tq.Name] = struct{}{}

		resp, err := client.DescribeTaskQueueEnhanced(ctx, temporalClient.DescribeTaskQueueEnhancedOptions{
			TaskQueue:   tq.Name,
			ReportStats: true,
			Versions: &temporalClient.TaskQueueVersionSelection{
				BuildIDs: []string{buildID},
			},
			TaskQueueTypes: []temporalClient.TaskQueueType{
				temporalClient.TaskQueueTypeWorkflow,
				temporalClient.TaskQueueTypeActivity,
			},
		})
		if err != nil {
			return 0, fmt.Errorf("unable to describe task queue %q: %w", tq.Name, err)
		}
		// VersionsInfo is deprecated in favor of VersioningInfo, but it is
		// the only surface that exposes per-version TaskQueueStats
		// (ApproximateBacklogCount) on this SDK version, and we query with
		// an explicit version selection above.
		for _, versionInfo := range resp.VersionsInfo { //nolint:staticcheck
			for _, typeInfo := range versionInfo.TypesInfo {
				if typeInfo.Stats != nil {
					total += typeInfo.Stats.ApproximateBacklogCount
				}
			}
		}
	}
	return total, nil
}

func withBackoff(timeout time.Duration, tick time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(tick)
	}
	return lastErr
}

// GetTestWorkflowStatus queries Temporal to get the status of test workflows for a version
func GetTestWorkflowStatus(
	ctx context.Context,
	client temporalClient.Client,
	workerDeploymentName string,
	buildID string,
	workerDeploy *temporaliov1alpha1.TemporalWorkerDeployment,
	temporalState *TemporalWorkerState,
) ([]temporaliov1alpha1.WorkflowExecution, error) {
	var results []temporaliov1alpha1.WorkflowExecution

	// Get deployment handler
	deploymentHandler := client.WorkerDeploymentClient().GetHandle(workerDeploymentName)

	// Get version info from temporal state to get deployment name
	versionInfo, exists := temporalState.Versions[buildID]
	if !exists {
		return results, nil
	}

	// Describe the version to get task queue information
	versionResp, err := deploymentHandler.DescribeVersion(ctx, temporalClient.WorkerDeploymentDescribeVersionOptions{
		BuildID: versionInfo.BuildID,
	})

	var notFound *serviceerror.NotFound
	if err != nil && !errors.As(err, &notFound) {
		// Ignore NotFound error, because if the version is not found, we know there are no test workflows running on it.
		return nil, fmt.Errorf("unable to describe worker deployment version for buildID %q: %w", buildID, err)
	}

	// Check test workflows for each task queue
	for _, tq := range versionResp.Info.TaskQueuesInfos {
		// Skip non-workflow task queues
		if tq.Type != temporalClient.TaskQueueTypeWorkflow {
			continue
		}

		// Adding task queue information to the current temporal state
		temporalState.Versions[buildID].TaskQueues = append(temporalState.Versions[buildID].TaskQueues, temporaliov1alpha1.TaskQueue{
			Name: tq.Name,
		})

		// Check if there is a test workflow for this task queue
		testWorkflowID := GetTestWorkflowID(versionInfo.DeploymentName, versionInfo.BuildID, tq.Name)
		wf, err := client.DescribeWorkflowExecution(
			ctx,
			testWorkflowID,
			"",
		)

		// Ignore "not found" errors
		if err != nil && !strings.Contains(err.Error(), "workflow not found") {
			return nil, fmt.Errorf("unable to describe test workflow: %w", err)
		}

		// Add workflow execution info
		if err == nil {
			info := wf.GetWorkflowExecutionInfo()
			workflowInfo := temporaliov1alpha1.WorkflowExecution{
				WorkflowID: info.GetExecution().GetWorkflowId(),
				RunID:      info.GetExecution().GetRunId(),
				TaskQueue:  info.GetTaskQueue(),
				Status:     mapWorkflowStatus(info.GetStatus()),
			}
			results = append(results, workflowInfo)
		}
	}

	return results, nil
}

// Helper functions

// mapWorkflowStatus converts Temporal workflow status to our CRD status
func mapWorkflowStatus(status enumspb.WorkflowExecutionStatus) temporaliov1alpha1.WorkflowExecutionStatus {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return temporaliov1alpha1.WorkflowExecutionStatusRunning
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return temporaliov1alpha1.WorkflowExecutionStatusCompleted
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return temporaliov1alpha1.WorkflowExecutionStatusFailed
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return temporaliov1alpha1.WorkflowExecutionStatusCanceled
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return temporaliov1alpha1.WorkflowExecutionStatusTerminated
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return temporaliov1alpha1.WorkflowExecutionStatusTimedOut
	default:
		// Default to running for unspecified or any other status
		return temporaliov1alpha1.WorkflowExecutionStatusRunning
	}
}

// GetTestWorkflowID generates a workflowID for test workflows
func GetTestWorkflowID(deploymentName, buildID, taskQueue string) string {
	return fmt.Sprintf("test-%s:%s-%s", deploymentName, buildID, taskQueue)
}

func HasUnversionedPoller(ctx context.Context,
	client temporalClient.Client,
	taskQueueInfo temporalClient.WorkerDeploymentTaskQueueInfo,
) (bool, error) {
	pollers, err := getPollers(ctx, client, taskQueueInfo)
	if err != nil {
		return false, fmt.Errorf("unable to confirm presence of unversioned pollers: %w", err)
	}
	for _, p := range pollers {
		switch p.GetDeploymentOptions().GetWorkerVersioningMode() {
		case temporalClient.WorkerVersioningModeUnversioned, temporalClient.WorkerVersioningModeUnspecified:
			return true, nil
		case temporalClient.WorkerVersioningModeVersioned:
		}
	}
	return false, nil
}

func DeploymentShouldIgnoreLastModifier(
	ctx context.Context,
	deploymentHandler temporalClient.WorkerDeploymentHandle,
	routingConfig temporalClient.WorkerDeploymentRoutingConfig,
) (shouldIgnore bool, err error) {
	if routingConfig.CurrentVersion != nil {
		shouldIgnore, err = getShouldIgnoreLastModifier(ctx, deploymentHandler, routingConfig.CurrentVersion.BuildId)
		if err != nil {
			return false, err
		}
	}
	if !shouldIgnore && // if someone has a non-nil Current Version, but only set the metadata in their Ramping Version, also count that
		routingConfig.RampingVersion != nil {
		return getShouldIgnoreLastModifier(ctx, deploymentHandler, routingConfig.CurrentVersion.BuildId)
	}
	return shouldIgnore, nil
}

func getShouldIgnoreLastModifier(
	ctx context.Context,
	deploymentHandler temporalClient.WorkerDeploymentHandle,
	buildId string,
) (bool, error) {
	desc, err := deploymentHandler.DescribeVersion(ctx, temporalClient.WorkerDeploymentDescribeVersionOptions{
		BuildID: buildId,
	})
	if err != nil {
		return false, fmt.Errorf("unable to describe version: %w", err)
	}
	for k, v := range desc.Info.Metadata {
		if k == IgnoreLastModifierKey {
			var s string
			err = converter.GetDefaultDataConverter().FromPayload(v, &s)
			if err != nil {
				return false, fmt.Errorf("unable to decode metadata payload for key %s: %w", IgnoreLastModifierKey, err)
			}
			return s == "true", nil
		}
	}
	return false, nil
}

func HasNoVersionedPollers(ctx context.Context,
	client temporalClient.Client,
	taskQueueInfo temporalClient.WorkerDeploymentTaskQueueInfo,
) (bool, error) {
	pollers, err := getPollers(ctx, client, taskQueueInfo)
	if err != nil {
		return false, fmt.Errorf("unable to confirm absence of versioned pollers: %w", err)
	}
	for _, p := range pollers {
		switch p.GetDeploymentOptions().GetWorkerVersioningMode() {
		case temporalClient.WorkerVersioningModeUnversioned, temporalClient.WorkerVersioningModeUnspecified:
		case temporalClient.WorkerVersioningModeVersioned:
			return false, nil
		}
	}
	return true, nil
}

func getPollers(ctx context.Context,
	client temporalClient.Client,
	taskQueueInfo temporalClient.WorkerDeploymentTaskQueueInfo,
) ([]*taskqueuepb.PollerInfo, error) {
	var resp *workflowservice.DescribeTaskQueueResponse
	var err error
	switch taskQueueInfo.Type {
	case temporalClient.TaskQueueTypeWorkflow:
		resp, err = client.DescribeTaskQueue(ctx, taskQueueInfo.Name, temporalClient.TaskQueueTypeWorkflow)
	case temporalClient.TaskQueueTypeActivity:
		resp, err = client.DescribeTaskQueue(ctx, taskQueueInfo.Name, temporalClient.TaskQueueTypeActivity)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to describe task queue %s: %w", taskQueueInfo.Name, err)
	}
	return resp.GetPollers(), nil
}

func noTaskQueuesHaveVersionedPollers(
	ctx context.Context,
	client temporalClient.Client,
	tqs []temporalClient.WorkerDeploymentTaskQueueInfo,
) bool {
	countHasNoVersionedPollers := 0
	for _, tqInfo := range tqs {
		hasNoVersionedPollers, _ := HasNoVersionedPollers(ctx, client, tqInfo) // TODO(carlydf): consider logging this error
		if hasNoVersionedPollers {
			countHasNoVersionedPollers++
		}
	}
	return countHasNoVersionedPollers == len(tqs)
}

func allTaskQueuesHaveUnversionedPoller(
	ctx context.Context,
	client temporalClient.Client,
	tqs []temporalClient.WorkerDeploymentTaskQueueInfo,
) bool {
	countHasUnversionedPoller := 0
	for _, tqInfo := range tqs {
		hasUnversionedPoller, _ := HasUnversionedPoller(ctx, client, tqInfo) // TODO(carlydf): consider logging this error
		if hasUnversionedPoller {
			countHasUnversionedPoller++
		}
	}
	return countHasUnversionedPoller == len(tqs)
}
