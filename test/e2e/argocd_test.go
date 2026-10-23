package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

// These tests run against the real Argo CD in argoCDNamespace, which syncs apps from repo.

// argoCDTimeout allows for Argo CD's reconciliation timeout, repo-server fetches and the sync itself.
const argoCDTimeout = 3 * time.Minute

// createArgoCDApp creates an app in argoCDNamespace that deploys path of repo at HEAD into targetNamespace.
func createArgoCDApp(t *testing.T, name, path string) {
	t.Helper()
	app := &argov1alpha1.Application{
		Name: name, Namespace: argoCDNamespace,
		Spec: argov1alpha1.ApplicationSpec{
			Project:     "default",
			Source:      &argov1alpha1.ApplicationSource{RepoURL: repo.url, Path: path, TargetRevision: "HEAD"},
			Destination: argov1alpha1.ApplicationDestination{Server: "https://kubernetes.default.svc", Namespace: targetNamespace},
		},
	}
	require.NoError(t, k8s.Create(t.Context(), app))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), app)) })
}

func createArgoCDSync(t *testing.T, name string, spec syncv1alpha1.ApplicationSyncSpec) *syncv1alpha1.ApplicationSync {
	t.Helper()
	s := &syncv1alpha1.ApplicationSync{Name: name, Namespace: argoCDNamespace, Spec: spec}
	require.NoError(t, k8s.Create(t.Context(), s))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), s)) })
	return s
}

func waitFinished(t *testing.T, s *syncv1alpha1.ApplicationSync) *syncv1alpha1.ApplicationSync {
	t.Helper()
	require.Eventually(t, func() bool {
		s, _ = get(t, s)
		return s.Status.Phase.Completed()
	}, argoCDTimeout, time.Second, "run %s didn't finish", s.Name)
	return s
}

func argoCDApp(t *testing.T, name string) *argov1alpha1.Application {
	t.Helper()
	app, _ := get(t, &argov1alpha1.Application{Name: name, Namespace: argoCDNamespace})
	return app
}

// deployedValue returns data.value of the ConfigMap Argo CD deployed for name, or "" if there is none.
func deployedValue(t *testing.T, name string) string {
	t.Helper()
	cm, _ := get(t, &corev1.ConfigMap{Name: name, Namespace: targetNamespace})
	return cm.Data["value"]
}

func TestArgoCDSyncsGroupsInDependencyOrder(t *testing.T) {
	repo.commit(t, map[string]string{
		"order/db/cm.yaml":  configMap("order-db", "v1"),
		"order/api/cm.yaml": configMap("order-api", "v1"),
	})
	createArgoCDApp(t, "order-db", "order/db")
	createArgoCDApp(t, "order-api", "order/api")
	s := waitFinished(t, createArgoCDSync(t, "order", syncv1alpha1.ApplicationSyncSpec{Groups: []syncv1alpha1.ApplicationSyncGroup{
		{Name: "data", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "order-db"}}},
		{Name: "backend", DependsOn: []string{"data"}, Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "order-api"}}},
	}}))
	require.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, s.Status.Phase, s.Status.Message)

	db, api := argoCDApp(t, "order-db"), argoCDApp(t, "order-api")
	for _, app := range []*argov1alpha1.Application{db, api} {
		assert.Equal(t, argov1alpha1.SyncStatusCodeSynced, app.Status.Sync.Status, app.Name)
		assert.Equal(t, health.HealthStatusHealthy, app.Status.Health.Status, app.Name)
		require.NotNil(t, app.Status.OperationState, app.Name)
		assert.Contains(t, app.Status.OperationState.Operation.Info, &argov1alpha1.Info{Name: syncv1alpha1.ApplicationSyncOperationInfoName, Value: "order"})
	}
	require.NotNil(t, db.Status.OperationState.FinishedAt)
	assert.False(t, api.Status.OperationState.StartedAt.Before(db.Status.OperationState.FinishedAt),
		"api started at %s, before db finished at %s", api.Status.OperationState.StartedAt, db.Status.OperationState.FinishedAt)
	assert.Equal(t, "v1", deployedValue(t, "order-db"))
	assert.Equal(t, "v1", deployedValue(t, "order-api"))
}

func TestArgoCDFailedSyncSkipsDependents(t *testing.T) {
	repo.commit(t, map[string]string{
		// The API server rejects the name, so the sync fails.
		"failing/broken/cm.yaml": configMap("Not_A_Valid_Name", "v1"),
		"failing/after/cm.yaml":  configMap("failing-after", "v1"),
	})
	createArgoCDApp(t, "failing-broken", "failing/broken")
	createArgoCDApp(t, "failing-after", "failing/after")
	s := waitFinished(t, createArgoCDSync(t, "failing", syncv1alpha1.ApplicationSyncSpec{Groups: []syncv1alpha1.ApplicationSyncGroup{
		{Name: "first", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "failing-broken"}}},
		{Name: "second", DependsOn: []string{"first"}, Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "failing-after"}}},
	}}))

	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, appEntry(s, "failing-broken").Phase)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSkipped, appEntry(s, "failing-after").Phase)
	assert.Nil(t, argoCDApp(t, "failing-after").Status.OperationState, "a skipped app is never synced")
	assert.Empty(t, deployedValue(t, "failing-after"))
}

func TestArgoCDSyncsPinnedRevision(t *testing.T) {
	pinned := repo.commit(t, map[string]string{"pinned/app/cm.yaml": configMap("pinned-app", "v1")})
	repo.commit(t, map[string]string{"pinned/app/cm.yaml": configMap("pinned-app", "v2")})
	createArgoCDApp(t, "pinned-app", "pinned/app")
	s := waitFinished(t, createArgoCDSync(t, "pinned", syncv1alpha1.ApplicationSyncSpec{
		// The app stays OutOfSync with HEAD after syncing an older commit.
		DoneWhen: syncv1alpha1.ApplicationSyncDoneWhenHealthy,
		Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "default", Apps: []syncv1alpha1.ApplicationSyncApp{
			{Name: "pinned-app", Revisions: []syncv1alpha1.ApplicationSyncRevision{{Revision: pinned}}},
		}}},
	}))
	require.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, s.Status.Phase, s.Status.Message)

	app := argoCDApp(t, "pinned-app")
	require.NotNil(t, app.Status.OperationState)
	require.NotNil(t, app.Status.OperationState.SyncResult)
	assert.Equal(t, pinned, app.Status.OperationState.SyncResult.Revision)
	assert.Equal(t, "v1", deployedValue(t, "pinned-app"))
}

func TestArgoCDPolicySyncsNewCommits(t *testing.T) {
	first := repo.commit(t, map[string]string{"policy/web/cm.yaml": configMap("policy-web", "v1")})
	createArgoCDApp(t, "policy-web", "policy/web")
	policy := &syncv1alpha1.ApplicationSyncPolicy{
		Name: "policy-web", Namespace: argoCDNamespace,
		Spec: syncv1alpha1.ApplicationSyncPolicySpec{Template: syncv1alpha1.ApplicationSyncSpec{
			Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "default", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "policy-web"}}}},
		}},
	}
	require.NoError(t, k8s.Create(t.Context(), policy))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), policy)) })

	syncedTo := func(revision, value string) func() bool {
		return func() bool {
			app := argoCDApp(t, "policy-web")
			return app.Status.Sync.Status == argov1alpha1.SyncStatusCodeSynced && app.Status.Sync.Revision == revision &&
				deployedValue(t, "policy-web") == value
		}
	}
	require.Eventually(t, syncedTo(first, "v1"), argoCDTimeout, time.Second, "the policy didn't sync the first commit")
	policy, _ = get(t, policy)
	firstRun := policy.Status.CurrentRun

	second := repo.commit(t, map[string]string{"policy/web/cm.yaml": configMap("policy-web", "v2")})
	require.Eventually(t, syncedTo(second, "v2"), argoCDTimeout, time.Second, "the policy didn't sync the new commit")
	policy, _ = get(t, policy)
	assert.NotEqual(t, firstRun, policy.Status.CurrentRun, "a new commit starts a new run")
}
