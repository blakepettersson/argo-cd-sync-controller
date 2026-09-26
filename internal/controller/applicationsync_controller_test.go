package controller

import (
	"context"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/pkg/health"
	synccommon "github.com/argoproj/argo-cd/gitops-engine/pkg/sync/common"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	syncv1alpha1 "github.com/argoproj-labs/argocd-application-sync-crd/api/v1alpha1"
)

const testNamespace = "argocd"

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, argov1alpha1.AddToScheme(scheme))
	require.NoError(t, syncv1alpha1.AddToScheme(scheme))
	return scheme
}

func testProject(windows argov1alpha1.SyncWindows) *argov1alpha1.AppProject {
	return &argov1alpha1.AppProject{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
		Spec: argov1alpha1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argov1alpha1.ApplicationDestination{{Server: "*", Namespace: "*"}},
			SyncWindows:  windows,
		},
	}
}

// testApp is an app freshly compared against its current spec, OutOfSync.
func testApp(name string) *argov1alpha1.Application {
	app := &argov1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: argov1alpha1.ApplicationSpec{
			Project:     "default",
			Source:      &argov1alpha1.ApplicationSource{RepoURL: "https://github.com/argoproj/argocd-example-apps.git", Path: "guestbook", TargetRevision: "HEAD"},
			Destination: argov1alpha1.ApplicationDestination{Server: "https://kubernetes.default.svc", Namespace: "guestbook"},
		},
	}
	app.Status.Sync.ComparedTo = app.Spec.BuildComparedToStatus(app.Spec.GetSources())
	app.Status.Sync.Status = argov1alpha1.SyncStatusCodeOutOfSync
	reconciled := metav1.NewTime(time.Now().Add(time.Hour))
	app.Status.ReconciledAt = &reconciled
	return app
}

func newTestReconciler(t *testing.T, objs ...client.Object) *ApplicationSyncReconciler {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&syncv1alpha1.ApplicationSync{}, &syncv1alpha1.ApplicationSyncPolicy{}).
		Build()
	return &ApplicationSyncReconciler{Client: c, APIReader: c, ArgoCDNamespace: testNamespace, HistoryLimit: 20}
}

func withApps(objs []client.Object, apps ...*argov1alpha1.Application) []client.Object {
	for _, a := range apps {
		objs = append(objs, a)
	}
	return objs
}

func getTestApp(t *testing.T, c client.Client, name string) *argov1alpha1.Application {
	t.Helper()
	var app argov1alpha1.Application
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, &app))
	return &app
}

func updateTestApp(t *testing.T, c client.Client, name string, mutate func(*argov1alpha1.Application)) {
	t.Helper()
	app := getTestApp(t, c, name)
	mutate(app)
	require.NoError(t, c.Update(context.Background(), app))
}

// finishOperation simulates Argo CD's application controller completing the operation that was set.
func finishOperation(t *testing.T, c client.Client, name string, phase synccommon.OperationPhase, healthStatus health.HealthStatusCode) {
	t.Helper()
	updateTestApp(t, c, name, func(app *argov1alpha1.Application) {
		require.NotNil(t, app.Operation, "operation was never set on %s", name)
		now := metav1.Now()
		app.Status.OperationState = &argov1alpha1.OperationState{
			Operation:  *app.Operation,
			Phase:      phase,
			StartedAt:  now,
			FinishedAt: &now,
			SyncResult: &argov1alpha1.SyncOperationResult{Revision: "abc123"},
		}
		app.Operation = nil
		app.Status.ReconciledAt = &now
		app.Status.Health.Status = healthStatus
		app.Status.Sync.ComparedTo = app.Spec.BuildComparedToStatus(app.Spec.GetSources())
		app.Status.Sync.Revision = "abc123"
		if phase.Successful() {
			app.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
		}
	})
}

func group(name string, dependsOn []string, apps ...string) syncv1alpha1.ApplicationSyncGroup {
	g := syncv1alpha1.ApplicationSyncGroup{Name: name, DependsOn: dependsOn}
	for _, a := range apps {
		g.Apps = append(g.Apps, syncv1alpha1.ApplicationSyncApp{Name: a})
	}
	return g
}

func newSync(groups ...syncv1alpha1.ApplicationSyncGroup) *syncv1alpha1.ApplicationSync {
	return &syncv1alpha1.ApplicationSync{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "rollout",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
		Spec: syncv1alpha1.ApplicationSyncSpec{Groups: groups, Prune: true},
	}
}

func appPhases(s *syncv1alpha1.ApplicationSync) map[string]syncv1alpha1.ApplicationSyncPhase {
	phases := map[string]syncv1alpha1.ApplicationSyncPhase{}
	for _, g := range s.Status.Groups {
		for _, a := range g.Apps {
			phases[a.Name] = a.Phase
		}
	}
	return phases
}

func groupPhases(s *syncv1alpha1.ApplicationSync) map[string]syncv1alpha1.ApplicationSyncPhase {
	phases := map[string]syncv1alpha1.ApplicationSyncPhase{}
	for _, g := range s.Status.Groups {
		phases[g.Name] = g.Phase
	}
	return phases
}

func appStatus(t *testing.T, s *syncv1alpha1.ApplicationSync, name string) syncv1alpha1.ApplicationSyncAppStatus {
	t.Helper()
	for _, g := range s.Status.Groups {
		for _, a := range g.Apps {
			if a.Name == name {
				return a
			}
		}
	}
	require.FailNow(t, "no status for app", name)
	return syncv1alpha1.ApplicationSyncAppStatus{}
}

func TestGroupOrder(t *testing.T) {
	order, err := groupOrder([]syncv1alpha1.ApplicationSyncGroup{
		group("frontend", []string{"backend"}, "web"),
		group("backend", []string{"data"}, "api"),
		group("data", nil, "db"),
		group("workers", []string{"data"}, "worker"),
	})
	require.NoError(t, err)
	assert.Equal(t, []int{2, 1, 3, 0}, order)

	for name, tc := range map[string]struct {
		groups []syncv1alpha1.ApplicationSyncGroup
		err    string
	}{
		"unknown dependency": {[]syncv1alpha1.ApplicationSyncGroup{group("a", []string{"b"})}, `group "a" depends on "b"`},
		"cycle":              {[]syncv1alpha1.ApplicationSyncGroup{group("a", []string{"b"}), group("b", []string{"a"})}, "cycle"},
		"app in two groups":  {[]syncv1alpha1.ApplicationSyncGroup{group("a", nil, "db"), group("b", nil, "db")}, `application "db" is listed in groups "a" and "b"`},
		"duplicate group":    {[]syncv1alpha1.ApplicationSyncGroup{group("a", nil), group("a", nil)}, `group "a" is listed more than once`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := groupOrder(tc.groups)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestDependencyOrder(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"))...)
	s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"))

	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, s.Status.Phase)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Running", "backend": "Pending"}, groupPhases(s))
	assert.Equal(t, `waiting for group "data"`, s.Status.Groups[1].Message)
	db := getTestApp(t, r.Client, "db")
	require.NotNil(t, db.Operation)
	assert.True(t, db.Operation.Sync.Prune)
	assert.True(t, isOperationFor(s, db.Operation))

	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusProgressing)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"db": "Progressing", "api": "Pending"}, appPhases(s))

	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
		app.Status.Health.Status = health.HealthStatusHealthy
		now := metav1.Now()
		app.Status.ReconciledAt = &now
	})
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Succeeded", "backend": "Running"}, groupPhases(s))

	finishOperation(t, r.Client, "api", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, s.Status.Phase)
	assert.NotNil(t, s.Status.FinishedAt)
}

func TestFailureSkipsDependents(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"), testApp("web"), testApp("cache"))...)
	s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"), group("frontend", []string{"backend"}, "web"), group("caches", nil, "cache"))

	r.reconcileApplicationSync(t.Context(), s)
	finishOperation(t, r.Client, "db", synccommon.OperationFailed, health.HealthStatusHealthy)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Failed", "backend": "Skipped", "frontend": "Skipped", "caches": "Running"}, groupPhases(s))
	assert.Equal(t, `group "backend" skipped`, appStatus(t, s, "web").Message)

	finishOperation(t, r.Client, "cache", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
	assert.Equal(t, "3 of 4 applications did not succeed", s.Status.Message)
}

func TestGroupAppsRunInParallel(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("api"), testApp("worker"), testApp("web"))...)
	s := newSync(group("backend", nil, "api", "worker"), group("frontend", []string{"backend"}, "web"))

	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"api": "Running", "worker": "Running", "web": "Pending"}, appPhases(s))

	finishOperation(t, r.Client, "api", synccommon.OperationFailed, health.HealthStatusHealthy)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"backend": "Running", "frontend": "Pending"}, groupPhases(s), "the group waits for worker")

	finishOperation(t, r.Client, "worker", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"backend": "Failed", "frontend": "Skipped"}, groupPhases(s))
}

func TestEmptyGroupsAndInvalidSpec(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)

	s := newSync(group("empty", nil), group("data", []string{"empty"}, "db"))
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"empty": "Succeeded", "data": "Running"}, groupPhases(s))

	s = newSync(group("a", []string{"missing"}, "db"))
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
	assert.Contains(t, s.Status.Message, `group "a" depends on "missing"`)
}

func TestWaits(t *testing.T) {
	t.Run("busy app", func(t *testing.T) {
		app := testApp("db")
		app.Operation = &argov1alpha1.Operation{Sync: &argov1alpha1.SyncOperation{}}
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, app)...)
		s := newSync(group("data", nil, "db"))
		r.reconcileApplicationSync(t.Context(), s)
		assert.Equal(t, "waiting for another operation to finish", appStatus(t, s, "db").Message)
	})
	t.Run("missing app", func(t *testing.T) {
		r := newTestReconciler(t, testProject(nil))
		s := newSync(group("data", nil, "db"))
		r.reconcileApplicationSync(t.Context(), s)
		assert.Equal(t, "waiting for application to exist", appStatus(t, s, "db").Message)
	})
	t.Run("closed sync window", func(t *testing.T) {
		windows := argov1alpha1.SyncWindows{{Kind: "deny", Schedule: "* * * * *", Duration: "1h", Applications: []string{"*"}}}
		r := newTestReconciler(t, withApps([]client.Object{testProject(windows)}, testApp("db"))...)
		s := newSync(group("data", nil, "db"))
		r.reconcileApplicationSync(t.Context(), s)
		st := appStatus(t, s, "db")
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhasePending, st.Phase)
		assert.Equal(t, "waiting for a sync window to allow the sync", st.Message)
		assert.Nil(t, getTestApp(t, r.Client, "db").Operation, "no operation is written while the window denies the sync")
	})
}

func TestOnFailureWait(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"))...)
	s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"))
	s.Spec.OnFailure = syncv1alpha1.ApplicationSyncOnFailureWait

	r.reconcileApplicationSync(t.Context(), s)
	finishOperation(t, r.Client, "db", synccommon.OperationFailed, health.HealthStatusDegraded)
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseProgressing, appStatus(t, s, "db").Phase)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Running", "backend": "Pending"}, groupPhases(s))

	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
		app.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
		app.Status.Health.Status = health.HealthStatusHealthy
		reconciled := metav1.NewTime(time.Now().Add(time.Hour))
		app.Status.ReconciledAt = &reconciled
	})
	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Succeeded", "backend": "Running"}, groupPhases(s))
}

func TestDoneWhen(t *testing.T) {
	for _, tc := range []struct {
		name           string
		doneWhen       syncv1alpha1.ApplicationSyncDoneWhen
		syncedRevision string
		want           syncv1alpha1.ApplicationSyncPhase
	}{
		{"OutOfSync at the synced revision blocks", "", "abc123", syncv1alpha1.ApplicationSyncPhaseProgressing},
		{"doneWhen Healthy ignores OutOfSync", syncv1alpha1.ApplicationSyncDoneWhenHealthy, "abc123", syncv1alpha1.ApplicationSyncPhaseSucceeded},
		{"OutOfSync because targetRevision moved on does not block", "", "def456", syncv1alpha1.ApplicationSyncPhaseSucceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
			s := newSync(group("data", nil, "db"))
			s.Spec.DoneWhen = tc.doneWhen
			r.reconcileApplicationSync(t.Context(), s)
			finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
			updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
				app.Status.Sync.Status = argov1alpha1.SyncStatusCodeOutOfSync
				app.Status.Sync.Revision = tc.syncedRevision
			})
			r.reconcileApplicationSync(t.Context(), s)
			assert.Equal(t, tc.want, appStatus(t, s, "db").Phase)
		})
	}
}

func TestStaleStatusRequestsRefresh(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	s := newSync(group("data", nil, "db"))
	r.reconcileApplicationSync(t.Context(), s)
	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	// The spec changed after the last comparison, as with a Helm parameter change.
	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Spec.Source.Path = "other/path" })

	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, "waiting for application to be refreshed", appStatus(t, s, "db").Message)
	assert.Equal(t, "normal", getTestApp(t, r.Client, "db").Annotations[refreshAnnotation], "a refresh is requested the way the CLI does")
}

func TestSkipIfSynced(t *testing.T) {
	app := testApp("db")
	app.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	app.Status.Sync.Revision = "abc123"
	app.Status.Health.Status = health.HealthStatusHealthy
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, app)...)
	s := newSync(group("data", nil, "db"))
	s.Spec.SkipIfSynced = true

	r.reconcileApplicationSync(t.Context(), s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, s.Status.Phase)
	assert.Nil(t, getTestApp(t, r.Client, "db").Operation)
}

func TestMaxParallelAndPause(t *testing.T) {
	for _, tc := range []struct {
		maxParallel intstr.IntOrString
		running     int
		message     string
	}{
		{intstr.FromInt32(1), 1, "waiting for one of 1 parallel slots"},
		{intstr.FromString("10%"), 1, "waiting for one of 1 parallel slots"},
		{intstr.FromInt32(0), 0, "paused by maxParallel"},
	} {
		t.Run(tc.maxParallel.String(), func(t *testing.T) {
			r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("a"), testApp("b"))...)
			g := group("all", nil, "a", "b")
			g.MaxParallel = &tc.maxParallel
			s := newSync(g)
			r.reconcileApplicationSync(t.Context(), s)
			running := 0
			for _, st := range s.Status.Groups[0].Apps {
				if st.Phase == syncv1alpha1.ApplicationSyncPhaseRunning {
					running++
				}
			}
			assert.Equal(t, tc.running, running)
			assert.Equal(t, tc.message, appStatus(t, s, "b").Message)
		})
	}

	t.Run("spec.paused", func(t *testing.T) {
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
		s := newSync(group("data", nil, "db"))
		s.Spec.Paused = true
		r.reconcileApplicationSync(t.Context(), s)
		assert.Equal(t, "paused", s.Status.Message)
		assert.Nil(t, getTestApp(t, r.Client, "db").Operation)

		s.Spec.Paused = false
		r.reconcileApplicationSync(t.Context(), s)
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, appStatus(t, s, "db").Phase)
	})
}

func TestOperationFields(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	g := group("data", nil, "db")
	g.Apps[0].Prune = new(false)
	g.Apps[0].Retry = &argov1alpha1.RetryStrategy{Limit: 5}
	g.Apps[0].Resources = []argov1alpha1.SyncOperationResource{{Kind: "Deployment", Name: "guestbook-ui"}}
	s := newSync(g)
	s.Spec.InitiatedBy = argov1alpha1.OperationInitiator{Username: "ci", Automated: true}
	s.Spec.Info = []*argov1alpha1.Info{{Name: "Reason", Value: "release"}}

	r.reconcileApplicationSync(t.Context(), s)
	op := getTestApp(t, r.Client, "db").Operation
	require.NotNil(t, op)
	assert.False(t, op.Sync.Prune, "the per-app prune overrides spec.prune")
	assert.Equal(t, int64(5), op.Retry.Limit)
	assert.Equal(t, g.Apps[0].Resources, op.Sync.Resources)
	assert.Equal(t, s.Spec.InitiatedBy, op.InitiatedBy)
	assert.Equal(t, s.Spec.Info, op.Info[1:], "spec.info follows the ApplicationSync tag")
}

func TestRevisions(t *testing.T) {
	const sha = "0123456789abcdef0123456789abcdef01234567"
	multiSource := func() *argov1alpha1.Application {
		app := testApp("db")
		app.Spec.Source = nil
		app.Spec.Sources = argov1alpha1.ApplicationSources{
			{Name: "chart", RepoURL: "https://github.com/argoproj/argocd-example-apps.git", Path: "helm-guestbook", TargetRevision: "HEAD"},
			{RepoURL: "https://github.com/argoproj/argocd-example-apps.git", Path: "guestbook", TargetRevision: "HEAD"},
			{RepoURL: "https://github.com/argoproj/argocd-example-apps.git", Path: "kustomize-guestbook", TargetRevision: "HEAD"},
		}
		app.Status.Sync.ComparedTo = app.Spec.BuildComparedToStatus(app.Spec.GetSources())
		return app
	}
	run := func(t *testing.T, app *argov1alpha1.Application, revisions []syncv1alpha1.ApplicationSyncRevision, extra ...client.Object) (*ApplicationSyncReconciler, *syncv1alpha1.ApplicationSync) {
		t.Helper()
		r := newTestReconciler(t, append(withApps([]client.Object{testProject(nil)}, app), extra...)...)
		g := group("data", nil, "db")
		g.Apps[0].Revisions = revisions
		s := newSync(g)
		r.reconcileApplicationSync(t.Context(), s)
		return r, s
	}

	t.Run("single source", func(t *testing.T) {
		r, s := run(t, testApp("db"), []syncv1alpha1.ApplicationSyncRevision{{Revision: sha}})
		assert.Equal(t, []string{sha}, appStatus(t, s, "db").PinnedRevisions)
		assert.Equal(t, sha, getTestApp(t, r.Client, "db").Operation.Sync.Revision)
	})
	t.Run("multiple sources by name and position", func(t *testing.T) {
		r, _ := run(t, multiSource(), []syncv1alpha1.ApplicationSyncRevision{{Source: "chart", Revision: sha}, {Position: 3, Revision: sha}})
		// The second source isn't listed, so it floats: resolved from targetRevision when the sync starts.
		assert.Equal(t, []string{sha, "", sha}, getTestApp(t, r.Client, "db").Operation.Sync.Revisions)
	})
	for name, tc := range map[string]struct {
		app       *argov1alpha1.Application
		revisions []syncv1alpha1.ApplicationSyncRevision
		err       string
	}{
		"unknown source":                {multiSource(), []syncv1alpha1.ApplicationSyncRevision{{Source: "missing", Revision: sha}}, "no source with that name"},
		"position out of range":         {multiSource(), []syncv1alpha1.ApplicationSyncRevision{{Position: 4, Revision: sha}}, "the application has 3 sources"},
		"same source twice":             {multiSource(), []syncv1alpha1.ApplicationSyncRevision{{Source: "chart", Revision: sha}, {Position: 1, Revision: sha}}, "more than one revision"},
		"multi-source without selector": {multiSource(), []syncv1alpha1.ApplicationSyncRevision{{Revision: sha}}, "must set source or position"},
		"single source with selector":   {testApp("db"), []syncv1alpha1.ApplicationSyncRevision{{Position: 1, Revision: sha}}, "can't set source or position"},
	} {
		t.Run(name, func(t *testing.T) {
			_, s := run(t, tc.app, tc.revisions)
			st := appStatus(t, s, "db")
			assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, st.Phase)
			assert.Contains(t, st.Message, tc.err)
		})
	}
	t.Run("override setting allows only targetRevision", func(t *testing.T) {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: argoCDConfigMap, Namespace: testNamespace}, Data: map[string]string{requireOverrideKey: "true"}}
		_, s := run(t, testApp("db"), []syncv1alpha1.ApplicationSyncRevision{{Revision: sha}}, cm)
		assert.Contains(t, appStatus(t, s, "db").Message, "allows only targetRevision")

		// The revision targetRevision last resolved to counts as targetRevision.
		app := testApp("db")
		app.Status.Sync.Revision = sha
		_, s = run(t, app, []syncv1alpha1.ApplicationSyncRevision{{Revision: sha}}, cm)
		assert.Equal(t, []string{sha}, appStatus(t, s, "db").PinnedRevisions)
	})
	t.Run("auto-sync conflicts with a different revision", func(t *testing.T) {
		app := testApp("db")
		app.Spec.SyncPolicy = &argov1alpha1.SyncPolicy{Automated: &argov1alpha1.SyncPolicyAutomated{}}
		_, s := run(t, app, []syncv1alpha1.ApplicationSyncRevision{{Revision: sha}})
		assert.Contains(t, appStatus(t, s, "db").Message, "auto-sync currently set to HEAD")
	})
}

func TestReconcileWritesStatusAndGarbageCollects(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	completed := func(name string, minute int, apps ...string) *syncv1alpha1.ApplicationSync {
		s := newSync(group("all", nil, apps...))
		s.Name = name
		s.UID = types.UID(name)
		s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
		finished := metav1.NewTime(base.Add(time.Duration(minute) * time.Minute))
		s.Status.FinishedAt = &finished
		return s
	}
	app := testApp("db")
	app.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	app.Status.Health.Status = health.HealthStatusHealthy
	current := newSync(group("data", nil, "db"))
	current.Name = "current"
	current.Spec.SkipIfSynced = true
	r := newTestReconciler(t, testProject(nil), app, current, completed("old-1", 1, "db"), completed("old-2", 2, "db"), completed("pair", 1, "db", "web"))
	r.HistoryLimit = 2

	_, err := r.Reconcile(t.Context(), ctrlRequest(current))
	require.NoError(t, err)

	var got syncv1alpha1.ApplicationSync
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(current), &got))
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, got.Status.Phase, "status is written through the status subresource")

	var list syncv1alpha1.ApplicationSyncList
	require.NoError(t, r.List(t.Context(), &list))
	var names []string
	for _, s := range list.Items {
		names = append(names, s.Name)
	}
	// {db} keeps the newest two (current, old-2); {db, web} is a different set of apps.
	assert.ElementsMatch(t, []string{"current", "old-2", "pair"}, names)
}
