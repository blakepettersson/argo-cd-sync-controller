package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	synccommon "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/common"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

// These tests commit a run to Git as an Argo CD sync hook of a parent app, the way to manage runs from Git that the
// proposal recommends: each sync of the parent creates one run, and Argo CD neither prunes nor recreates it.

// hookRun is a run committed as a Sync hook, with generateName, that syncs app.
func hookRun(prefix, app string) string {
	return fmt.Sprintf(`apiVersion: argoproj.io/v1alpha1
kind: ApplicationSync
metadata:
  generateName: %s
  namespace: %s
  annotations:
    argocd.argoproj.io/hook: Sync
spec:
  groups:
  - apps: [{name: %s}]
`, prefix, argoCDNamespace, app)
}

// syncArgoCDApp starts a sync of the app named name, as kubectl can today, and returns when it was requested.
func syncArgoCDApp(t *testing.T, name string) time.Time {
	t.Helper()
	requested := time.Now().Truncate(time.Second)
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		app := argoCDApp(t, name)
		app.Operation = &argov1alpha1.Operation{Sync: &argov1alpha1.SyncOperation{}, InitiatedBy: argov1alpha1.OperationInitiator{Username: "e2e"}}
		return k8s.Update(t.Context(), app)
	}))
	return requested
}

// waitArgoCDSync waits for the sync of name requested at requested to finish, and returns its operation state.
func waitArgoCDSync(t *testing.T, name string, requested time.Time) *argov1alpha1.OperationState {
	t.Helper()
	var state *argov1alpha1.OperationState
	require.Eventually(t, func() bool {
		state = argoCDApp(t, name).Status.OperationState
		return state != nil && state.Phase.Completed() && !state.StartedAt.Time.Before(requested)
	}, argoCDTimeout, time.Second, "the sync of %s didn't finish", name)
	return state
}

// hookRuns lists the runs whose names start with prefix.
func hookRuns(t *testing.T, prefix string) []syncv1alpha1.ApplicationSync {
	t.Helper()
	var list syncv1alpha1.ApplicationSyncList
	if err := k8s.List(t.Context(), &list, client.InNamespace(argoCDNamespace)); err != nil {
		t.Errorf("listing runs: %v", err)
		return nil
	}
	var runs []syncv1alpha1.ApplicationSync
	for _, s := range list.Items {
		if strings.HasPrefix(s.Name, prefix) {
			runs = append(runs, s)
		}
	}
	return runs
}

func TestArgoCDHookRunHoldsItsParentsSync(t *testing.T) {
	repo.commit(t, map[string]string{
		"hook/web/cm.yaml":     configMap("hook-web", "v1"),
		"hook/parent/run.yaml": hookRun("hook-run-", "hook-web"),
	})
	createArgoCDApp(t, "hook-web", "hook/web")
	createArgoCDApp(t, "hook-parent", "hook/parent")

	state := waitArgoCDSync(t, "hook-parent", syncArgoCDApp(t, "hook-parent"))
	require.Equal(t, synccommon.OperationSucceeded, state.Phase, state.Message)
	runs := hookRuns(t, "hook-run-")
	require.Len(t, runs, 1, "the sync creates one run, named by Argo CD from generateName")
	run := runs[0]
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, run.Status.Phase, run.Status.Message)
	require.NotNil(t, run.Status.FinishedAt)
	require.NotNil(t, state.FinishedAt)
	assert.False(t, state.FinishedAt.Before(run.Status.FinishedAt),
		"the parent's sync finished at %s, before its run at %s", state.FinishedAt, run.Status.FinishedAt)
	assert.Equal(t, "v1", deployedValue(t, "hook-web"))

	// Hooks aren't tracked: deleting the run, as garbage collection would, doesn't make Argo CD recreate it, and
	// Argo CD's hook finalizer doesn't keep it from going.
	require.NoError(t, k8s.Delete(t.Context(), &run))
	require.Eventually(t, func() bool {
		_, ok := get(t, &run)
		return !ok
	}, argoCDTimeout, time.Second, "run %s wasn't deleted; finalizers: %v", run.Name, run.Finalizers)
	assert.Never(t, func() bool { return len(hookRuns(t, "hook-run-")) > 0 }, 15*time.Second, time.Second,
		"Argo CD recreated a deleted hook run")
	assert.Equal(t, argov1alpha1.SyncStatusCodeSynced, argoCDApp(t, "hook-parent").Status.Sync.Status)

	// Each sync of the parent creates a new run.
	state = waitArgoCDSync(t, "hook-parent", syncArgoCDApp(t, "hook-parent"))
	require.Equal(t, synccommon.OperationSucceeded, state.Phase, state.Message)
	runs = hookRuns(t, "hook-run-")
	require.Len(t, runs, 1)
	assert.NotEqual(t, run.Name, runs[0].Name)
}

func TestArgoCDFailedHookRunFailsItsParentsSync(t *testing.T) {
	repo.commit(t, map[string]string{
		// The API server rejects the name, so the run's sync fails.
		"hookfail/broken/cm.yaml":  configMap("Not_A_Valid_Name", "v1"),
		"hookfail/parent/run.yaml": hookRun("hookfail-run-", "hookfail-broken"),
	})
	createArgoCDApp(t, "hookfail-broken", "hookfail/broken")
	createArgoCDApp(t, "hookfail-parent", "hookfail/parent")

	state := waitArgoCDSync(t, "hookfail-parent", syncArgoCDApp(t, "hookfail-parent"))
	assert.NotEqual(t, synccommon.OperationSucceeded, state.Phase, "the parent's sync succeeded although its run failed")
	runs := hookRuns(t, "hookfail-run-")
	require.Len(t, runs, 1)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, runs[0].Status.Phase)
}
