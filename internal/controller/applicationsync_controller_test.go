package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/pkg/health"
	synccommon "github.com/argoproj/argo-cd/gitops-engine/pkg/sync/common"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
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

// reconcileSync runs one pass for s, creating it in the fake client first if needed, since the controller reads the
// ApplicationSync live before it starts a sync.
func reconcileSync(t *testing.T, r *ApplicationSyncReconciler, s *syncv1alpha1.ApplicationSync) {
	t.Helper()
	if err := r.Create(t.Context(), s.DeepCopy()); err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
	r.reconcileApplicationSync(t.Context(), s)
}

func newTestReconciler(t *testing.T, objs ...client.Object) *ApplicationSyncReconciler {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&syncv1alpha1.ApplicationSync{}, &syncv1alpha1.ApplicationSyncPolicy{}).
		// The fake client doesn't assign UIDs, and policies identify their runs by the UID a create returns.
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(uuid.NewUUID())
			}
			return c.Create(ctx, obj, opts...)
		}}).
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
			UID:               "rollout-uid",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute)),
		},
		Spec: syncv1alpha1.ApplicationSyncSpec{Groups: groups, Prune: true},
	}
}

// ownByPolicy makes s a policy's run, which is what makes a run automatic.
func ownByPolicy(s *syncv1alpha1.ApplicationSync) {
	s.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: syncv1alpha1.GroupVersion.String(), Kind: "ApplicationSyncPolicy", Name: "platform", UID: "policy-uid",
		Controller: new(true),
	}}
}

func TestOnlyPolicyRunsAreAutomatic(t *testing.T) {
	s := newSync(group("data", nil, "db"))
	assert.False(t, isAutomated(s), "no owner")
	s.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "x", Controller: new(true)}}
	assert.False(t, isAutomated(s), "owned by something else")
	ownByPolicy(s)
	assert.True(t, isAutomated(s))

	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	reconcileSync(t, r, s)
	assert.True(t, getTestApp(t, r.Client, "db").Operation.InitiatedBy.Automated, "the operation is marked automatic")
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

	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, s.Status.Phase)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Running", "backend": "Pending"}, groupPhases(s))
	assert.Equal(t, `waiting for group "data"`, s.Status.Groups[1].Message)
	db := getTestApp(t, r.Client, "db")
	require.NotNil(t, db.Operation)
	assert.True(t, db.Operation.Sync.Prune)
	assert.True(t, isOperationFor(s, db.Operation))

	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusProgressing)
	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"db": "Progressing", "api": "Pending"}, appPhases(s))

	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
		app.Status.Health.Status = health.HealthStatusHealthy
		now := metav1.Now()
		app.Status.ReconciledAt = &now
	})
	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Succeeded", "backend": "Running"}, groupPhases(s))

	finishOperation(t, r.Client, "api", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, s.Status.Phase)
	assert.NotNil(t, s.Status.FinishedAt)
}

func TestFailureSkipsDependents(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"), testApp("web"), testApp("cache"))...)
	s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"), group("frontend", []string{"backend"}, "web"), group("caches", nil, "cache"))

	reconcileSync(t, r, s)
	finishOperation(t, r.Client, "db", synccommon.OperationFailed, health.HealthStatusHealthy)
	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Failed", "backend": "Skipped", "frontend": "Skipped", "caches": "Running"}, groupPhases(s))
	assert.Equal(t, `group "backend" skipped`, appStatus(t, s, "web").Message)

	finishOperation(t, r.Client, "cache", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
	assert.Equal(t, "3 of 4 applications did not succeed", s.Status.Message)
}

func TestGroupAppsRunInParallel(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("api"), testApp("worker"), testApp("web"))...)
	s := newSync(group("backend", nil, "api", "worker"), group("frontend", []string{"backend"}, "web"))

	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"api": "Running", "worker": "Running", "web": "Pending"}, appPhases(s))

	finishOperation(t, r.Client, "api", synccommon.OperationFailed, health.HealthStatusHealthy)
	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"backend": "Running", "frontend": "Pending"}, groupPhases(s), "the group waits for worker")

	finishOperation(t, r.Client, "worker", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"backend": "Failed", "frontend": "Skipped"}, groupPhases(s))
}

func TestEmptyGroupsAndInvalidSpec(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)

	s := newSync(group("empty", nil), group("data", []string{"empty"}, "db"))
	reconcileSync(t, r, s)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"empty": "Succeeded", "data": "Running"}, groupPhases(s))

	s = newSync(group("a", []string{"missing"}, "db"))
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
	assert.Contains(t, s.Status.Message, `group "a" depends on "missing"`)
}

func TestWaits(t *testing.T) {
	t.Run("busy app", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			mutate  func(*syncv1alpha1.ApplicationSync)
			phase   syncv1alpha1.ApplicationSyncPhase
			message string
		}{
			{"fails, as the Sync API rejects", func(*syncv1alpha1.ApplicationSync) {}, syncv1alpha1.ApplicationSyncPhaseFailed, "another operation is already in progress"},
			{
				"waits with onFailure: Wait", func(s *syncv1alpha1.ApplicationSync) { s.Spec.OnFailure = syncv1alpha1.ApplicationSyncOnFailureWait },
				syncv1alpha1.ApplicationSyncPhasePending, "another operation is already in progress" + waitingToRecoverSuffix,
			},
			{"waits in a policy run", ownByPolicy, syncv1alpha1.ApplicationSyncPhasePending, "waiting for another operation to finish"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				app := testApp("db")
				app.Operation = &argov1alpha1.Operation{Sync: &argov1alpha1.SyncOperation{}}
				r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, app)...)
				s := newSync(group("data", nil, "db"))
				tc.mutate(s)
				reconcileSync(t, r, s)
				st := appStatus(t, s, "db")
				assert.Equal(t, tc.phase, st.Phase)
				assert.Equal(t, tc.message, st.Message)
			})
		}
	})
	t.Run("missing app", func(t *testing.T) {
		r := newTestReconciler(t, testProject(nil))
		s := newSync(group("data", nil, "db"))
		reconcileSync(t, r, s)
		assert.Equal(t, "waiting for application to exist", appStatus(t, s, "db").Message)
	})
	t.Run("closed sync window", func(t *testing.T) {
		windows := argov1alpha1.SyncWindows{{Kind: "deny", Schedule: "* * * * *", Duration: "1h", Applications: []string{"*"}}}
		r := newTestReconciler(t, withApps([]client.Object{testProject(windows)}, testApp("db"))...)
		s := newSync(group("data", nil, "db"))
		reconcileSync(t, r, s)
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

	reconcileSync(t, r, s)
	finishOperation(t, r.Client, "db", synccommon.OperationFailed, health.HealthStatusDegraded)
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseProgressing, appStatus(t, s, "db").Phase)
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Running", "backend": "Pending"}, groupPhases(s))
	assert.Contains(t, s.Status.Message, "application db: sync failed", "the run says what it waits on")
	assert.True(t, strings.HasSuffix(s.Status.Message, waitingToRecoverSuffix))

	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
		app.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
		app.Status.Health.Status = health.HealthStatusHealthy
		reconciled := metav1.NewTime(time.Now().Add(time.Hour))
		app.Status.ReconciledAt = &reconciled
	})
	reconcileSync(t, r, s)
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
			reconcileSync(t, r, s)
			finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
			updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
				app.Status.Sync.Status = argov1alpha1.SyncStatusCodeOutOfSync
				app.Status.Sync.Revision = tc.syncedRevision
			})
			reconcileSync(t, r, s)
			assert.Equal(t, tc.want, appStatus(t, s, "db").Phase)
			if tc.want == syncv1alpha1.ApplicationSyncPhaseSucceeded {
				assert.Equal(t, argov1alpha1.SyncStatusCodeOutOfSync, appStatus(t, s, "db").SyncStatus, "the sync status it ended with is recorded")
			}
		})
	}
}

func TestStaleStatusRequestsRefresh(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	s := newSync(group("data", nil, "db"))
	reconcileSync(t, r, s)
	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	// The spec changed after the last comparison, as with a Helm parameter change.
	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Spec.Source.Path = "other/path" })

	reconcileSync(t, r, s)
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

	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, s.Status.Phase)
	assert.Nil(t, getTestApp(t, r.Client, "db").Operation)
}

func TestMaxParallel(t *testing.T) {
	for _, tc := range []struct {
		maxParallel intstr.IntOrString
		running     int
		message     string
	}{
		{intstr.FromInt32(1), 1, "waiting for one of 1 parallel slots"},
		{intstr.FromString("10%"), 1, "waiting for one of 1 parallel slots"},
		{intstr.FromInt32(0), 1, "waiting for one of 1 parallel slots"}, // rejected by the CRD; never a hold
	} {
		t.Run(tc.maxParallel.String(), func(t *testing.T) {
			r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("a"), testApp("b"))...)
			g := group("all", nil, "a", "b")
			g.MaxParallel = &tc.maxParallel
			s := newSync(g)
			reconcileSync(t, r, s)
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
}

func TestOperationFields(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	g := group("data", nil, "db")
	g.Apps[0].Prune = new(false)
	g.Apps[0].Retry = &argov1alpha1.RetryStrategy{Limit: 5}
	g.Apps[0].Resources = []argov1alpha1.SyncOperationResource{{Kind: "Deployment", Name: "guestbook-ui"}}
	s := newSync(g)
	s.Spec.InitiatedBy = syncv1alpha1.ApplicationSyncInitiator{Username: "ci"}
	s.Spec.Info = []*argov1alpha1.Info{{Name: "Reason", Value: "release"}}

	reconcileSync(t, r, s)
	op := getTestApp(t, r.Client, "db").Operation
	require.NotNil(t, op)
	assert.False(t, op.Sync.Prune, "the per-app prune overrides spec.prune")
	assert.Equal(t, int64(5), op.Retry.Limit)
	assert.Equal(t, g.Apps[0].Resources, op.Sync.Resources)
	assert.Equal(t, argov1alpha1.OperationInitiator{Username: "ci"}, op.InitiatedBy, "a run no policy owns is manual")
	assert.Equal(t, s.Spec.Info, op.Info[2:], "spec.info follows the ApplicationSync name and UID tags")
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
		reconcileSync(t, r, s)
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
	// policy-old is among neither app's newest two, but is its policy's latest run.
	policyRun := completed("policy-old", -1, "db")
	ownByPolicy(policyRun)
	policy := newTestPolicy("")
	policy.Status.CurrentRunUID = policyRun.UID
	current := newSync(group("data", nil, "db"))
	current.Name = "current"
	current.Spec.SkipIfSynced = true
	r := newTestReconciler(t, testProject(nil), app, current, policy, completed("old-1", 1, "db"), completed("old-2", 2, "db"),
		completed("pair", 1, "db", "web"), completed("web-only", 3, "web"), completed("pair-old", 0, "db", "web"),
		completed("no-apps", 0), policyRun)
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
	// Each app keeps its newest two: db keeps current and old-2, web keeps web-only and pair. pair-old is among
	// neither app's newest two, even though it is one of only two runs for the set {db, web}. no-apps counts toward
	// no app, so only a TTL deletes it.
	assert.ElementsMatch(t, []string{"current", "old-2", "pair", "web-only", "no-apps", "policy-old"}, names)
}

func TestOperationFromRecreatedApplicationSyncIsNotAdopted(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	old := newSync(group("data", nil, "db"))
	old.UID = "old-uid"
	reconcileSync(t, r, old)
	require.True(t, isOperationFor(old, getTestApp(t, r.Client, "db").Operation))
	// The application controller starts the old run's operation only after the same-named run was recreated.
	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)

	recreated := newSync(group("data", nil, "db"))
	db := getTestApp(t, r.Client, "db")
	assert.False(t, isOperationFor(recreated, &db.Status.OperationState.Operation))
	assert.Nil(t, operationStateFor(recreated, db), "the result belongs to the deleted run")
}

func TestDeletedApplicationSyncStartsNoSync(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	s := newSync(group("data", nil, "db"))
	require.NoError(t, r.Create(t.Context(), s.DeepCopy()))
	require.NoError(t, r.Delete(t.Context(), s.DeepCopy()))

	// s is the copy a stale cache would still hand to a reconcile.
	r.reconcileApplicationSync(t.Context(), s)
	assert.Nil(t, getTestApp(t, r.Client, "db").Operation)
}

func TestReconcileRecordsObservedGeneration(t *testing.T) {
	s := newSync(group("data", nil, "db"))
	// The fake client doesn't manage metadata.generation, so the test sets it as the API server would.
	s.Generation = 1
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil), s}, testApp("db"))...)
	get := func() *syncv1alpha1.ApplicationSync {
		t.Helper()
		var got syncv1alpha1.ApplicationSync
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(s), &got))
		return &got
	}
	_, err := r.Reconcile(t.Context(), ctrlRequest(s))
	require.NoError(t, err)
	got := get()
	assert.Equal(t, int64(1), got.Status.ObservedGeneration)

	// A finished ApplicationSync still records that a later TTL edit was seen.
	got.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
	require.NoError(t, r.Status().Update(t.Context(), got))
	got.Spec.TTLSecondsAfterFinished = new(int32(3600))
	got.Generation = 2
	require.NoError(t, r.Update(t.Context(), got))
	_, err = r.Reconcile(t.Context(), ctrlRequest(s))
	require.NoError(t, err)
	got = get()
	assert.Equal(t, int64(2), got.Status.ObservedGeneration)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, got.Status.Phase)
}

func TestAllowEmpty(t *testing.T) {
	prunable := argov1alpha1.ResourceStatus{Kind: "ConfigMap", Name: "old", Status: argov1alpha1.SyncStatusCodeOutOfSync, RequiresPruning: true}
	kept := argov1alpha1.ResourceStatus{Kind: "ConfigMap", Name: "kept", Status: argov1alpha1.SyncStatusCodeSynced}
	for _, tc := range []struct {
		name       string
		resources  []argov1alpha1.ResourceStatus
		automated  bool
		allowEmpty bool
		want       syncv1alpha1.ApplicationSyncPhase
	}{
		{"automated run pruning everything", []argov1alpha1.ResourceStatus{prunable}, true, false, syncv1alpha1.ApplicationSyncPhaseFailed},
		{"allowEmpty", []argov1alpha1.ResourceStatus{prunable}, true, true, syncv1alpha1.ApplicationSyncPhaseRunning},
		{"manual run", []argov1alpha1.ResourceStatus{prunable}, false, false, syncv1alpha1.ApplicationSyncPhaseRunning},
		{"a resource is kept", []argov1alpha1.ResourceStatus{prunable, kept}, true, false, syncv1alpha1.ApplicationSyncPhaseRunning},
		// Auto-sync blocks this case, which stalls RollingSync on empty apps (argoproj/argo-cd#15371).
		{"no resources", nil, true, false, syncv1alpha1.ApplicationSyncPhaseRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := testApp("db")
			app.Status.Resources = tc.resources
			r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, app)...)
			s := newSync(group("data", nil, "db"))
			if tc.automated {
				ownByPolicy(s)
			}
			s.Spec.AllowEmpty = tc.allowEmpty

			reconcileSync(t, r, s)
			st := appStatus(t, s, "db")
			assert.Equal(t, tc.want, st.Phase, st.Message)
			if tc.want == syncv1alpha1.ApplicationSyncPhaseFailed {
				assert.Contains(t, st.Message, "set allowEmpty")
				assert.Nil(t, getTestApp(t, r.Client, "db").Operation, "no sync is started")
			}
		})
	}
}

func TestRefusesAutoSyncedApp(t *testing.T) {
	app := testApp("db")
	app.Spec.SyncPolicy = &argov1alpha1.SyncPolicy{Automated: &argov1alpha1.SyncPolicyAutomated{SelfHeal: new(true)}}
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, app)...)

	s := newSync(group("data", nil, "db"))
	reconcileSync(t, r, s)
	st := appStatus(t, s, "db")
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, st.Phase)
	assert.Contains(t, st.Message, "auto-sync is enabled")
	assert.Nil(t, getTestApp(t, r.Client, "db").Operation, "no sync is started")

	// With onFailure: Wait, the app waits until auto-sync is turned off, here with automated.enabled: false.
	s = newSync(group("data", nil, "db"))
	s.Spec.OnFailure = syncv1alpha1.ApplicationSyncOnFailureWait
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhasePending, appStatus(t, s, "db").Phase)
	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Spec.SyncPolicy.Automated.Enabled = new(false) })
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, appStatus(t, s, "db").Phase)
}

func TestDeletedApplication(t *testing.T) {
	newApps := func() (*argov1alpha1.Application, *argov1alpha1.Application) {
		db, api := testApp("db"), testApp("api")
		db.UID, api.UID = "db-uid", "api-uid"
		return db, api
	}
	newWaitSync := func() *syncv1alpha1.ApplicationSync {
		s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"))
		s.Spec.OnFailure = syncv1alpha1.ApplicationSyncOnFailureWait
		return s
	}

	t.Run("deleted after its sync started fails, even with onFailure: Wait", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		reconcileSync(t, r, s)
		require.Equal(t, types.UID("db-uid"), appStatus(t, s, "db").UID, "the UID is recorded when the sync starts")

		require.NoError(t, r.Delete(t.Context(), db))
		reconcileSync(t, r, s)
		st := appStatus(t, s, "db")
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, st.Phase)
		assert.Equal(t, "application was deleted or recreated", st.Message)
	})
	t.Run("recreated after its sync started fails", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		reconcileSync(t, r, s)

		require.NoError(t, r.Delete(t.Context(), db))
		recreated := testApp("db")
		recreated.UID = "db-uid-2"
		require.NoError(t, r.Create(t.Context(), recreated))
		reconcileSync(t, r, s)
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, appStatus(t, s, "db").Phase)
	})
	t.Run("deleted before its sync starts waits", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		reconcileSync(t, r, s)
		require.Empty(t, appStatus(t, s, "api").UID)

		require.NoError(t, r.Delete(t.Context(), api))
		finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
		reconcileSync(t, r, s)
		st := appStatus(t, s, "api")
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhasePending, st.Phase)
		assert.Equal(t, waitingForAppMessage, st.Message)
	})
	t.Run("recreated before its sync starts is synced", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		reconcileSync(t, r, s)

		require.NoError(t, r.Delete(t.Context(), api))
		recreated := testApp("api")
		recreated.UID = "api-uid-2"
		require.NoError(t, r.Create(t.Context(), recreated))
		finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
		reconcileSync(t, r, s)
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, appStatus(t, s, "api").Phase)
		assert.Equal(t, types.UID("api-uid-2"), appStatus(t, s, "api").UID)
		assert.NotNil(t, getTestApp(t, r.Client, "api").Operation)
	})
	t.Run("recreated while the cache still holds the old app fails, even with onFailure: Wait", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		reconcileSync(t, r, s)
		require.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, appStatus(t, s, "db").Phase)

		// The cache still has the old db, without this run's operation; the API server has the recreated one.
		updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Operation = nil })
		recreated := testApp("db")
		recreated.UID = "db-uid-2"
		r.APIReader = fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(recreated).Build()
		reconcileSync(t, r, s)
		st := appStatus(t, s, "db")
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, st.Phase)
		assert.Equal(t, "application was deleted or recreated", st.Message)
	})
	t.Run("an app the cache doesn't show as recreated yet isn't synced until it does", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		recreated := testApp("db")
		recreated.UID = "db-uid-2"
		live := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(recreated, s.DeepCopy()).Build()
		r.APIReader = live
		reconcileSync(t, r, s)

		st := appStatus(t, s, "db")
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhasePending, st.Phase, "the checks ran against the old app")
		assert.Equal(t, "waiting for the cache to catch up with a recreated application", st.Message)
		var got argov1alpha1.Application
		require.NoError(t, live.Get(t.Context(), client.ObjectKeyFromObject(recreated), &got))
		assert.Nil(t, got.Operation)
	})
	t.Run("a finished app keeps its phase", func(t *testing.T) {
		db, api := newApps()
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db, api)...)
		s := newWaitSync()
		reconcileSync(t, r, s)
		finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
		reconcileSync(t, r, s)
		require.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, appStatus(t, s, "db").Phase)

		require.NoError(t, r.Delete(t.Context(), getTestApp(t, r.Client, "db")))
		reconcileSync(t, r, s)
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, appStatus(t, s, "db").Phase)
	})
}

func TestActiveDeadlineSeconds(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "missing"))
	deadline := int64(60)
	s.Spec.ActiveDeadlineSeconds = &deadline
	reconcileSync(t, r, s)
	require.Equal(t, syncv1alpha1.ApplicationSyncPhaseRunning, s.Status.Phase)

	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
	reconcileSync(t, r, s)
	require.Equal(t, waitingForAppMessage, appStatus(t, s, "missing").Message)
	assert.Equal(t, "application missing: waiting for application to exist", s.Status.Message, "the run says what it waits on")

	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	s.Status.StartedAt = &started
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
	assert.Equal(t, "exceeded activeDeadlineSeconds (60s)", s.Status.Message)
	assert.NotNil(t, s.Status.FinishedAt)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, appStatus(t, s, "db").Phase, "finished apps keep their phase")
	missing := appStatus(t, s, "missing")
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, missing.Phase)
	assert.Equal(t, "exceeded activeDeadlineSeconds (60s)", missing.Message)

	t.Run("a group whose dependency finishes after the deadline is Skipped", func(t *testing.T) {
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"))...)
		s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"))
		s.Spec.ActiveDeadlineSeconds = &deadline
		reconcileSync(t, r, s)
		finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)
		s.Status.StartedAt = &started
		reconcileSync(t, r, s)
		assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Succeeded", "backend": "Skipped"}, groupPhases(s))
	})
	t.Run("groups that never started are Skipped", func(t *testing.T) {
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"))...)
		s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"))
		s.Spec.ActiveDeadlineSeconds = &deadline
		reconcileSync(t, r, s)
		s.Status.StartedAt = &started
		reconcileSync(t, r, s)
		assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Failed", "backend": "Skipped"}, groupPhases(s))
		assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSkipped, appStatus(t, s, "api").Phase)
	})
}

func TestActiveDeadlineAfterReconcile(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"))...)
	s := newSync(group("data", nil, "db"), group("backend", []string{"data"}, "api"))
	deadline := int64(60)
	s.Spec.ActiveDeadlineSeconds = &deadline
	reconcileSync(t, r, s)
	finishOperation(t, r.Client, "db", synccommon.OperationSucceeded, health.HealthStatusHealthy)

	// db finishes in the same pass the deadline is found to have passed.
	started := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	s.Status.StartedAt = &started
	reconcileSync(t, r, s)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSucceeded, appStatus(t, s, "db").Phase, "an app that finished keeps its result")
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseSkipped, appStatus(t, s, "api").Phase)
	assert.Nil(t, getTestApp(t, r.Client, "api").Operation, "nothing new starts after the deadline")
	assert.Equal(t, map[string]syncv1alpha1.ApplicationSyncPhase{"data": "Succeeded", "backend": "Skipped"}, groupPhases(s))
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, s.Status.Phase)
}

func TestWaitMessagePrefersActionableWait(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("a"))...)
	g := group("all", nil, "missing", "a", "b")
	g.MaxParallel = new(intstr.FromInt32(1))
	s := newSync(g)
	reconcileSync(t, r, s)
	require.Contains(t, appStatus(t, s, "b").Message, waitingForSlotPrefix)
	assert.Equal(t, "application missing: waiting for application to exist", s.Status.Message)
}

func TestRecoveryWaitReportedOnce(t *testing.T) {
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"))...)
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	s := newSync(group("data", nil, "db"))
	s.Spec.OnFailure = syncv1alpha1.ApplicationSyncOnFailureWait
	require.NoError(t, r.Create(t.Context(), s))

	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
	}
	reconcile()
	finishOperation(t, r.Client, "db", synccommon.OperationFailed, health.HealthStatusDegraded)
	reconcile()
	// The next pass rewrites the message ("application is Degraded; ..."); the wait was already reported.
	reconcile()
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(s), s))
	require.Equal(t, "application is Degraded"+waitingToRecoverSuffix, appStatus(t, s, "db").Message)

	close(recorder.Events)
	var waits []string
	for e := range recorder.Events {
		if strings.Contains(e, "ApplicationWaiting") {
			waits = append(waits, e)
		}
	}
	require.Len(t, waits, 1)
	assert.True(t, strings.HasSuffix(waits[0], waitingToRecoverSuffix))
}

func TestGarbageCollectionBreaksTiesByCreation(t *testing.T) {
	finished := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	run := func(name string, created time.Time) *syncv1alpha1.ApplicationSync {
		s := newSync(group("all", nil, "db"))
		s.Name, s.UID = name, types.UID(name)
		s.CreationTimestamp = metav1.NewTime(created)
		s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
		s.Status.FinishedAt = &finished
		return s
	}
	// Same finishedAt; "a-newer" was created later but sorts first by name.
	older, newer := run("z-older", time.Now().Add(-time.Hour)), run("a-newer", time.Now().Add(-30*time.Minute))
	r := newTestReconciler(t, older, newer)
	r.HistoryLimit = 1
	r.gc(t.Context(), newer)

	var list syncv1alpha1.ApplicationSyncList
	require.NoError(t, r.List(t.Context(), &list))
	require.Len(t, list.Items, 1)
	assert.Equal(t, "a-newer", list.Items[0].Name)
}

func TestWaitEvents(t *testing.T) {
	r := newTestReconciler(t, testProject(nil))
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	s := newSync(group("data", nil, "db"))
	require.NoError(t, r.Create(t.Context(), s))

	_, err := r.Reconcile(t.Context(), ctrlRequest(s))
	require.NoError(t, err)
	_, err = r.Reconcile(t.Context(), ctrlRequest(s))
	require.NoError(t, err)
	require.Len(t, recorder.Events, 1, "an unchanged wait is reported once")
	assert.Equal(t, "Warning ApplicationWaiting application db: waiting for application to exist", <-recorder.Events)

	t.Run("waiting for a refresh isn't reported", func(t *testing.T) {
		db := testApp("db")
		db.Status.ReconciledAt = nil
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, db)...)
		recorder := events.NewFakeRecorder(10)
		r.Recorder = recorder
		s := newSync(group("data", nil, "db"))
		s.Name = "stale"
		s.Spec.SkipIfSynced = true
		require.NoError(t, r.Create(t.Context(), s))
		_, err := r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(s), s))
		require.Equal(t, waitingForRefreshMessage, appStatus(t, s, "db").Message)
		assert.Empty(t, recorder.Events)
	})
	t.Run("queueing behind maxParallel isn't reported", func(t *testing.T) {
		r := newTestReconciler(t, withApps([]client.Object{testProject(nil)}, testApp("db"), testApp("api"))...)
		recorder := events.NewFakeRecorder(10)
		r.Recorder = recorder
		g := group("data", nil, "db", "api")
		g.MaxParallel = new(intstr.FromInt32(1))
		s := newSync(g)
		s.Name = "queued"
		require.NoError(t, r.Create(t.Context(), s))
		_, err := r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(s), s))
		require.Equal(t, "waiting for one of 1 parallel slots", appStatus(t, s, "api").Message)
		close(recorder.Events)
		for e := range recorder.Events {
			assert.NotContains(t, e, "ApplicationWaiting")
		}
	})
}

func TestTTLSecondsAfterFinished(t *testing.T) {
	completed := func(ttl int32, finished time.Time) *syncv1alpha1.ApplicationSync {
		s := newSync(group("data", nil, "db"))
		s.Spec.TTLSecondsAfterFinished = &ttl
		s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
		f := metav1.NewTime(finished)
		s.Status.FinishedAt = &f
		return s
	}
	exists := func(t *testing.T, r *ApplicationSyncReconciler, s *syncv1alpha1.ApplicationSync) bool {
		t.Helper()
		err := r.Get(t.Context(), client.ObjectKeyFromObject(s), &syncv1alpha1.ApplicationSync{})
		if apierrors.IsNotFound(err) {
			return false
		}
		require.NoError(t, err)
		return true
	}

	t.Run("deleted once the TTL has passed", func(t *testing.T) {
		s := completed(60, time.Now().Add(-2*time.Minute))
		r := newTestReconciler(t, s)
		_, err := r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
		assert.False(t, exists(t, r, s))
	})
	t.Run("requeued until the TTL passes", func(t *testing.T) {
		s := completed(3600, time.Now())
		r := newTestReconciler(t, s)
		res, err := r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
		assert.True(t, exists(t, r, s))
		assert.InDelta(t, time.Hour, res.RequeueAfter, float64(time.Minute))
	})
	t.Run("a policy's current run is kept until a newer one replaces it", func(t *testing.T) {
		s := completed(0, time.Now().Add(-time.Minute))
		ownByPolicy(s)
		policy := newTestPolicy("")
		policy.Status.CurrentRunUID = s.UID
		r := newTestReconciler(t, s, policy)
		res, err := r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
		assert.True(t, exists(t, r, s))
		assert.Equal(t, resyncPeriod, res.RequeueAfter)

		var p syncv1alpha1.ApplicationSyncPolicy
		require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(policy), &p))
		p.Status.CurrentRunUID = "newer-run-uid"
		require.NoError(t, r.Status().Update(t.Context(), &p))
		_, err = r.Reconcile(t.Context(), ctrlRequest(s))
		require.NoError(t, err)
		assert.False(t, exists(t, r, s))
	})
}
