package e2e

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
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

const timeout = 30 * time.Second

func createApp(t *testing.T, name string) *argov1alpha1.Application {
	t.Helper()
	app := &argov1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: argov1alpha1.ApplicationSpec{
			Project:     "default",
			Source:      &argov1alpha1.ApplicationSource{RepoURL: "https://github.com/argoproj/argocd-example-apps.git", Path: "guestbook", TargetRevision: "HEAD"},
			Destination: argov1alpha1.ApplicationDestination{Server: "https://kubernetes.default.svc", Namespace: namespace},
		},
	}
	require.NoError(t, k8s.Create(t.Context(), app))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), app)) })
	return app
}

func createSync(t *testing.T, name string, spec syncv1alpha1.ApplicationSyncSpec) *syncv1alpha1.ApplicationSync {
	t.Helper()
	s := &syncv1alpha1.ApplicationSync{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: spec}
	require.NoError(t, k8s.Create(t.Context(), s))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), s)) })
	return s
}

// get returns a fresh copy of obj, leaving obj untouched, and whether it exists. It doesn't stop the test on an
// error, since it runs inside Eventually's goroutine, which can outlive the assertion.
func get[T client.Object](t *testing.T, obj T) (T, bool) {
	t.Helper()
	obj = obj.DeepCopyObject().(T)
	err := k8s.Get(t.Context(), client.ObjectKeyFromObject(obj), obj)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("getting %s: %v", obj.GetName(), err)
	}
	return obj, err == nil
}

func appEntry(s *syncv1alpha1.ApplicationSync, name string) syncv1alpha1.ApplicationSyncAppStatus {
	for _, g := range s.Status.Groups {
		for _, a := range g.Apps {
			if a.Name == name {
				return a
			}
		}
	}
	return syncv1alpha1.ApplicationSyncAppStatus{}
}

// syncedApp plays Argo CD's application controller: the app was compared now, at revision, and reports Synced
// and Healthy, finishing the operation it was given if there is one.
func syncedApp(t *testing.T, name string, status argov1alpha1.SyncStatusCode, revision string) {
	t.Helper()
	// The controller may write a refresh annotation at the same time.
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return updateSyncedApp(t, name, status, revision)
	}))
}

func updateSyncedApp(t *testing.T, name string, status argov1alpha1.SyncStatusCode, revision string) error {
	t.Helper()
	app, _ := get(t, &argov1alpha1.Application{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
	now := metav1.Now()
	if app.Operation != nil {
		app.Status.OperationState = &argov1alpha1.OperationState{
			Operation: *app.Operation, Phase: synccommon.OperationSucceeded, StartedAt: now, FinishedAt: &now,
			SyncResult: &argov1alpha1.SyncOperationResult{Revision: revision},
		}
		app.Operation = nil
	}
	app.Status.Sync = argov1alpha1.SyncStatus{
		Status: status, Revision: revision, ComparedTo: app.Spec.BuildComparedToStatus(app.Spec.GetSources()),
	}
	app.Status.Health.Status = health.HealthStatusHealthy
	app.Status.ReconciledAt = &now
	return k8s.Update(t.Context(), app)
}

func TestSpecIsImmutableExceptTTL(t *testing.T) {
	s := createSync(t, "immutable", syncv1alpha1.ApplicationSyncSpec{Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "empty"}}})

	ttl := int32(3600)
	patch := client.MergeFrom(s.DeepCopy())
	s.Spec.TTLSecondsAfterFinished = &ttl
	require.NoError(t, k8s.Patch(t.Context(), s, patch))

	deadline := int64(3600)
	for name, mutate := range map[string]func(*syncv1alpha1.ApplicationSync){
		"prune":                 func(s *syncv1alpha1.ApplicationSync) { s.Spec.Prune = true },
		"activeDeadlineSeconds": func(s *syncv1alpha1.ApplicationSync) { s.Spec.ActiveDeadlineSeconds = &deadline },
	} {
		changed := s.DeepCopy()
		mutate(changed)
		err := k8s.Patch(t.Context(), changed, client.MergeFrom(s))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "spec is immutable, except spec.ttlSecondsAfterFinished", name)
	}
}

func TestMaxParallelMustBePositive(t *testing.T) {
	for _, v := range []intstr.IntOrString{intstr.FromInt32(0), intstr.FromString("0%")} {
		s := &syncv1alpha1.ApplicationSync{
			ObjectMeta: metav1.ObjectMeta{Name: "max-parallel", Namespace: namespace},
			Spec:       syncv1alpha1.ApplicationSyncSpec{Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "g", MaxParallel: &v}}},
		}
		err := k8s.Create(t.Context(), s)
		require.Error(t, err, v.String())
		assert.Contains(t, err.Error(), "maxParallel must be at least 1 or 1%", v.String())
	}
	createSync(t, "max-parallel-ok", syncv1alpha1.ApplicationSyncSpec{
		Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "g", MaxParallel: new(intstr.FromString("1%"))}},
	})
}

func TestTTLSecondsAfterFinished(t *testing.T) {
	t.Run("deletes a finished run", func(t *testing.T) {
		ttl := int32(2)
		s := createSync(t, "ttl", syncv1alpha1.ApplicationSyncSpec{
			TTLSecondsAfterFinished: &ttl, Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "empty"}},
		})
		assert.Eventually(t, func() bool {
			_, ok := get(t, s)
			return !ok
		}, timeout, 500*time.Millisecond)
	})
	t.Run("an edited TTL applies to a finished run", func(t *testing.T) {
		ttl := int32(3600)
		s := createSync(t, "ttl-edit", syncv1alpha1.ApplicationSyncSpec{
			TTLSecondsAfterFinished: &ttl, Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "empty"}},
		})
		require.Eventually(t, func() bool {
			s, _ = get(t, s)
			return s.Status.Phase == syncv1alpha1.ApplicationSyncPhaseSucceeded
		}, timeout, 500*time.Millisecond)

		patch := client.MergeFrom(s.DeepCopy())
		ttl = 1
		s.Spec.TTLSecondsAfterFinished = &ttl
		require.NoError(t, k8s.Patch(t.Context(), s, patch))
		assert.Eventually(t, func() bool {
			_, ok := get(t, s)
			return !ok
		}, timeout, 500*time.Millisecond)
	})
}

func TestDeletedApplication(t *testing.T) {
	runFor := func(t *testing.T, name, app string) *syncv1alpha1.ApplicationSync {
		t.Helper()
		return createSync(t, name, syncv1alpha1.ApplicationSyncSpec{
			OnFailure: syncv1alpha1.ApplicationSyncOnFailureWait,
			Groups:    []syncv1alpha1.ApplicationSyncGroup{{Name: "default", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: app}}}},
		})
	}

	t.Run("deleted after its sync started fails, even with onFailure: Wait", func(t *testing.T) {
		app := createApp(t, "deleted")
		s := runFor(t, "deleted-app", app.Name)
		require.Eventually(t, func() bool {
			got, _ := get(t, s)
			return appEntry(got, app.Name).UID == app.UID
		}, timeout, 500*time.Millisecond, "the UID is recorded when the sync starts")

		require.NoError(t, k8s.Delete(t.Context(), app))
		require.Eventually(t, func() bool {
			got, _ := get(t, s)
			return got.Status.Phase == syncv1alpha1.ApplicationSyncPhaseFailed
		}, timeout, 500*time.Millisecond)
		s, _ = get(t, s)
		assert.Equal(t, "application was deleted or recreated", appEntry(s, app.Name).Message)
	})
	t.Run("recreated before its sync starts is synced", func(t *testing.T) {
		// The app's group waits for "gate", which doesn't exist yet, so its sync can't start before the gate opens.
		app := createApp(t, "recreated")
		s := createSync(t, "recreated-app", syncv1alpha1.ApplicationSyncSpec{
			SkipIfSynced: true,
			Groups: []syncv1alpha1.ApplicationSyncGroup{
				{Name: "gate", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "gate"}}},
				{Name: "app", DependsOn: []string{"gate"}, Apps: []syncv1alpha1.ApplicationSyncApp{{Name: app.Name}}},
			},
		})
		require.NoError(t, k8s.Delete(t.Context(), app))
		recreated := createApp(t, app.Name)
		syncedApp(t, app.Name, argov1alpha1.SyncStatusCodeOutOfSync, "rev1")
		createApp(t, "gate")
		syncedApp(t, "gate", argov1alpha1.SyncStatusCodeSynced, "rev1")
		if !assert.Eventually(t, func() bool {
			got, _ := get(t, s)
			return appEntry(got, app.Name).UID == recreated.UID
		}, timeout, 500*time.Millisecond) {
			got, _ := get(t, s)
			gate, _ := get(t, &argov1alpha1.Application{ObjectMeta: metav1.ObjectMeta{Name: "gate", Namespace: namespace}})
			t.Fatalf("run status: %+v\ngate: %+v %+v", got.Status, gate.Status.ReconciledAt, gate.Status.Sync)
		}
		recreated, _ = get(t, recreated)
		assert.NotNil(t, recreated.Operation)
	})
}

func TestActiveDeadlineSeconds(t *testing.T) {
	deadline := int64(3)
	s := createSync(t, "deadline", syncv1alpha1.ApplicationSyncSpec{
		ActiveDeadlineSeconds: &deadline,
		Groups:                []syncv1alpha1.ApplicationSyncGroup{{Name: "default", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "never-created"}}}},
	})

	require.Eventually(t, func() bool {
		got, _ := get(t, s)
		return got.Status.Message == "application never-created: waiting for application to exist"
	}, timeout, 500*time.Millisecond, "the run says what it waits on")
	require.Eventually(t, func() bool {
		var list eventsv1.EventList
		if err := k8s.List(t.Context(), &list, client.InNamespace(namespace)); err != nil {
			t.Errorf("listing events: %v", err)
			return false
		}
		for _, e := range list.Items {
			if e.Regarding.Name == s.Name && e.Reason == "ApplicationWaiting" && e.Type == corev1.EventTypeWarning {
				return true
			}
		}
		return false
	}, timeout, 500*time.Millisecond, "a waiting app is reported as an event")

	require.Eventually(t, func() bool {
		got, _ := get(t, s)
		return got.Status.Phase == syncv1alpha1.ApplicationSyncPhaseFailed
	}, timeout, 500*time.Millisecond)
	s, _ = get(t, s)
	assert.Equal(t, "exceeded activeDeadlineSeconds (3s)", s.Status.Message)
	assert.Equal(t, syncv1alpha1.ApplicationSyncPhaseFailed, appEntry(s, "never-created").Phase)
}

func TestPolicyKeepsItsCurrentRunPastTTL(t *testing.T) {
	createApp(t, "policy-db")
	syncedApp(t, "policy-db", argov1alpha1.SyncStatusCodeOutOfSync, "rev1")
	ttl := int32(0)
	policy := &syncv1alpha1.ApplicationSyncPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: namespace},
		Spec: syncv1alpha1.ApplicationSyncPolicySpec{Template: syncv1alpha1.ApplicationSyncSpec{
			TTLSecondsAfterFinished: &ttl,
			Groups:                  []syncv1alpha1.ApplicationSyncGroup{{Name: "default", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "policy-db"}}}},
		}},
	}
	require.NoError(t, k8s.Create(t.Context(), policy))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), policy)) })

	// Wait for the policy's first run to write the operation, then finish it.
	require.Eventually(t, func() bool {
		app, _ := get(t, &argov1alpha1.Application{ObjectMeta: metav1.ObjectMeta{Name: "policy-db", Namespace: namespace}})
		return app.Operation != nil
	}, timeout, 500*time.Millisecond)
	syncedApp(t, "policy-db", argov1alpha1.SyncStatusCodeSynced, "rev1")
	policy, _ = get(t, policy)
	first := &syncv1alpha1.ApplicationSync{ObjectMeta: metav1.ObjectMeta{Name: policy.Status.CurrentRun, Namespace: namespace}}
	require.Eventually(t, func() bool {
		first, _ = get(t, first)
		return first.Status.Phase == syncv1alpha1.ApplicationSyncPhaseSucceeded
	}, timeout, 500*time.Millisecond)

	// A TTL of 0 has long passed, but the policy still needs its latest run.
	assert.Never(t, func() bool {
		_, ok := get(t, first)
		return !ok
	}, 5*time.Second, 500*time.Millisecond)

	// A new revision starts a new run, after which the first is no longer the policy's and its TTL applies.
	syncedApp(t, "policy-db", argov1alpha1.SyncStatusCodeOutOfSync, "rev2")
	require.Eventually(t, func() bool {
		policy, _ = get(t, policy)
		return policy.Status.CurrentRunUID != "" && policy.Status.CurrentRunUID != first.UID
	}, timeout, 500*time.Millisecond)
	assert.Eventually(t, func() bool {
		_, ok := get(t, first)
		return !ok
	}, time.Minute, time.Second)
}

func TestPolicySuspend(t *testing.T) {
	createApp(t, "suspend-db")
	syncedApp(t, "suspend-db", argov1alpha1.SyncStatusCodeOutOfSync, "rev1")
	policy := &syncv1alpha1.ApplicationSyncPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "suspend", Namespace: namespace},
		Spec: syncv1alpha1.ApplicationSyncPolicySpec{Template: syncv1alpha1.ApplicationSyncSpec{
			Groups: []syncv1alpha1.ApplicationSyncGroup{{Name: "default", Apps: []syncv1alpha1.ApplicationSyncApp{{Name: "suspend-db"}}}},
		}},
	}
	require.NoError(t, k8s.Create(t.Context(), policy))
	t.Cleanup(func() { _ = client.IgnoreNotFound(k8s.Delete(context.Background(), policy)) })
	require.Eventually(t, func() bool {
		got, _ := get(t, policy)
		return got.Status.CurrentRun != ""
	}, timeout, 500*time.Millisecond)
	policy, _ = get(t, policy)
	first := policy.Status.CurrentRun

	// Suspending the policy and deleting its run stops the rollout: the policy doesn't start the change again.
	patch := client.MergeFrom(policy.DeepCopy())
	policy.Spec.Suspend = true
	require.NoError(t, k8s.Patch(t.Context(), policy, patch))
	require.Eventually(t, func() bool {
		got, _ := get(t, policy)
		return got.Status.Message == "suspended"
	}, timeout, 500*time.Millisecond)
	require.NoError(t, k8s.Delete(t.Context(), &syncv1alpha1.ApplicationSync{ObjectMeta: metav1.ObjectMeta{Name: first, Namespace: namespace}}))
	assert.Never(t, func() bool {
		var list syncv1alpha1.ApplicationSyncList
		if err := k8s.List(t.Context(), &list, client.InNamespace(namespace), client.MatchingLabels{"argocd.argoproj.io/application-sync-policy": policy.Name}); err != nil {
			t.Errorf("listing runs: %v", err)
			return false
		}
		return len(list.Items) > 0
	}, 5*time.Second, 500*time.Millisecond)

	// Resuming starts the change again.
	policy, _ = get(t, policy)
	patch = client.MergeFrom(policy.DeepCopy())
	policy.Spec.Suspend = false
	require.NoError(t, k8s.Patch(t.Context(), policy, patch))
	assert.Eventually(t, func() bool {
		got, _ := get(t, policy)
		return got.Status.CurrentRun != "" && got.Status.CurrentRun != first
	}, timeout, 500*time.Millisecond)
}
