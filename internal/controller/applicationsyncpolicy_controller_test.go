package controller

import (
	"testing"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

func ctrlRequest(obj client.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)}
}

func newTestPolicy(concurrency syncv1alpha1.ApplicationSyncConcurrencyPolicy) *syncv1alpha1.ApplicationSyncPolicy {
	return &syncv1alpha1.ApplicationSyncPolicy{
		Name: "platform", Namespace: testNamespace, UID: "policy-uid",
		Spec: syncv1alpha1.ApplicationSyncPolicySpec{
			ConcurrencyPolicy: concurrency,
			Template: syncv1alpha1.ApplicationSyncSpec{
				SkipIfSynced: true,
				Groups:       []syncv1alpha1.ApplicationSyncGroup{group("data", nil, "db"), group("backend", []string{"data"}, "api")},
			},
		},
	}
}

func newTestPolicyReconciler(t *testing.T, policy *syncv1alpha1.ApplicationSyncPolicy, apps ...*argov1alpha1.Application) *ApplicationSyncPolicyReconciler {
	t.Helper()
	r := newTestReconciler(t, withApps([]client.Object{testProject(nil), policy}, apps...)...)
	return &ApplicationSyncPolicyReconciler{Client: r.Client, APIReader: r.APIReader, ArgoCDNamespace: testNamespace}
}

func reconcilePolicy(t *testing.T, r *ApplicationSyncPolicyReconciler, p *syncv1alpha1.ApplicationSyncPolicy) *syncv1alpha1.ApplicationSyncPolicy {
	t.Helper()
	_, err := r.Reconcile(t.Context(), ctrlRequest(p))
	require.NoError(t, err)
	var got syncv1alpha1.ApplicationSyncPolicy
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(p), &got))
	return &got
}

func listRuns(t *testing.T, r *ApplicationSyncPolicyReconciler) []syncv1alpha1.ApplicationSync {
	t.Helper()
	var list syncv1alpha1.ApplicationSyncList
	require.NoError(t, r.List(t.Context(), &list))
	return list.Items
}

func TestPolicyNoDrift(t *testing.T) {
	db, api := testApp("db"), testApp("api")
	db.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	api.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	p := newTestPolicy("")
	r := newTestPolicyReconciler(t, p, db, api)

	got := reconcilePolicy(t, r, p)
	assert.Empty(t, listRuns(t, r))
	assert.Equal(t, "all applications are Synced", got.Status.Message)
}

func TestPolicyStartsRunOnDrift(t *testing.T) {
	db, api := testApp("db"), testApp("api")
	db.Status.Sync.Revision = "aaa111"
	api.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	api.Status.Sync.Revision = "aaa111"
	p := newTestPolicy("")
	r := newTestPolicyReconciler(t, p, db, api)

	got := reconcilePolicy(t, r, p)
	runs := listRuns(t, r)
	require.Len(t, runs, 1)
	run := runs[0]
	assert.Equal(t, run.Name, got.Status.CurrentRun)
	assert.Equal(t, "platform", run.Labels[PolicyLabel])
	require.Len(t, run.OwnerReferences, 1)
	assert.Equal(t, "ApplicationSyncPolicy", run.OwnerReferences[0].Kind)
	assert.True(t, isAutomated(&run), "policy runs are automatic syncs")
	assert.Equal(t, []syncv1alpha1.ApplicationSyncRevision{{Revision: "aaa111"}}, run.Spec.Groups[0].Apps[0].Revisions)
	assert.Equal(t, []syncv1alpha1.ApplicationSyncRevision{{Revision: "aaa111"}}, run.Spec.Groups[1].Apps[0].Revisions)

	// The same change doesn't start a second run, even once the first has failed.
	run.Status.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
	require.NoError(t, r.Status().Update(t.Context(), &run))
	got = reconcilePolicy(t, r, got)
	assert.Len(t, listRuns(t, r), 1)
	assert.Contains(t, got.Status.Message, "already handled this change")
}

func TestPolicyConcurrency(t *testing.T) {
	for _, policy := range []syncv1alpha1.ApplicationSyncConcurrencyPolicy{syncv1alpha1.ApplicationSyncConcurrencyForbid, syncv1alpha1.ApplicationSyncConcurrencyReplace} {
		t.Run(string(policy), func(t *testing.T) {
			db := testApp("db")
			db.Status.Sync.Revision = "aaa111"
			p := newTestPolicy(policy)
			r := newTestPolicyReconciler(t, p, db, testApp("api"))
			got := reconcilePolicy(t, r, p)
			first := got.Status.CurrentRun

			// A new commit lands while the first run is still going.
			updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Status.Sync.Revision = "bbb222" })
			got = reconcilePolicy(t, r, got)

			runs := listRuns(t, r)
			require.Len(t, runs, 1)
			if policy == syncv1alpha1.ApplicationSyncConcurrencyForbid {
				assert.Equal(t, first, got.Status.CurrentRun)
				assert.Contains(t, got.Status.Message, "waiting for run")
				return
			}
			assert.NotEqual(t, first, runs[0].Name, "Replace deletes the running run and starts a new one")
			assert.Equal(t, []syncv1alpha1.ApplicationSyncRevision{{Revision: "bbb222"}}, runs[0].Spec.Groups[0].Apps[0].Revisions)
		})
	}
}

func TestPolicyWaitsForFreshStatus(t *testing.T) {
	db := testApp("db")
	db.Status.Sync.ComparedTo.Source.Path = "old/path"
	p := newTestPolicy("")
	r := newTestPolicyReconciler(t, p, db, testApp("api"))

	got := reconcilePolicy(t, r, p)
	assert.Empty(t, listRuns(t, r))
	assert.Equal(t, `waiting for application "db" to be refreshed`, got.Status.Message)
}

func TestPolicyRecordsObservedGeneration(t *testing.T) {
	db, api := testApp("db"), testApp("api")
	db.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	api.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	p := newTestPolicy("")
	// The fake client doesn't manage metadata.generation, so the test sets it as the API server would.
	p.Generation = 1
	r := newTestPolicyReconciler(t, p, db, api)

	got := reconcilePolicy(t, r, p)
	assert.Equal(t, int64(1), got.Status.ObservedGeneration)

	got.Spec.ConcurrencyPolicy = syncv1alpha1.ApplicationSyncConcurrencyReplace
	got.Generation = 2
	require.NoError(t, r.Update(t.Context(), got))
	got = reconcilePolicy(t, r, got)
	assert.Equal(t, int64(2), got.Status.ObservedGeneration)
}

// finishRun marks the policy's current run finished, as the ApplicationSync reconciler would.
func finishRun(t *testing.T, r *ApplicationSyncPolicyReconciler, p *syncv1alpha1.ApplicationSyncPolicy, phase syncv1alpha1.ApplicationSyncPhase) {
	t.Helper()
	var run syncv1alpha1.ApplicationSync
	require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: p.Namespace, Name: p.Status.CurrentRun}, &run))
	run.Status.Phase = phase
	run.Status.FinishedAt = new(metav1.Now())
	require.NoError(t, r.Status().Update(t.Context(), &run))
}

func getRun(t *testing.T, r *ApplicationSyncPolicyReconciler, name string) syncv1alpha1.ApplicationSync {
	t.Helper()
	var run syncv1alpha1.ApplicationSync
	require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: testNamespace, Name: name}, &run))
	return run
}

func TestPolicySelfHeal(t *testing.T) {
	db, api := testApp("db"), testApp("api")
	db.Status.Sync.Revision = "aaa111"
	api.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
	api.Status.Sync.Revision = "aaa111"
	p := newTestPolicy("")
	p.Spec.SelfHeal = true
	r := newTestPolicyReconciler(t, p, db, api)

	got := reconcilePolicy(t, r, p)
	first := got.Status.CurrentRun
	finishRun(t, r, got, syncv1alpha1.ApplicationSyncPhaseSucceeded)

	// Someone edits a live resource: same revisions and sources, but db is OutOfSync again.
	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) {
		app.Status.Resources = []argov1alpha1.ResourceStatus{
			{Group: "apps", Kind: "Deployment", Name: "db", Status: argov1alpha1.SyncStatusCodeOutOfSync},
			{Kind: "Service", Name: "db", Status: argov1alpha1.SyncStatusCodeSynced},
		}
	})
	got = reconcilePolicy(t, r, got)
	require.NotEqual(t, first, got.Status.CurrentRun, "a drift after a successful run is self-healed")
	assert.Equal(t, int64(1), got.Status.SelfHealAttempts)
	heal := getRun(t, r, got.Status.CurrentRun)
	assert.True(t, heal.Spec.SkipIfSynced, "apps that are still Synced aren't synced again")
	assert.Equal(t, []syncv1alpha1.ApplicationSyncRevision{{Revision: "aaa111"}}, heal.Spec.Groups[0].Apps[0].Revisions, "the same change is pinned")
	assert.Equal(t, []argov1alpha1.SyncOperationResource{{Group: "apps", Kind: "Deployment", Name: "db"}}, heal.Spec.Groups[0].Apps[0].Resources, "only drifted resources are synced")

	// A failed self-heal is not retried, as auto-sync doesn't retry a failed sync of the same revision.
	finishRun(t, r, got, syncv1alpha1.ApplicationSyncPhaseFailed)
	got = reconcilePolicy(t, r, got)
	assert.Len(t, listRuns(t, r), 2)
	assert.Contains(t, got.Status.Message, "already handled this change")

	// A new change resets the count.
	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Status.Sync.Revision = "bbb222" })
	got = reconcilePolicy(t, r, got)
	assert.Len(t, listRuns(t, r), 3)
	assert.Equal(t, int64(0), got.Status.SelfHealAttempts)
}

func TestPolicySelfHealBackoff(t *testing.T) {
	db := testApp("db")
	db.Status.Sync.Revision = "aaa111"
	p := newTestPolicy("")
	p.Spec.SelfHeal = true
	params := &corev1.ConfigMap{
		Name: argoCDCmdParamsMap, Namespace: testNamespace,
		Data: map[string]string{selfHealBackoffTimeoutKey: "3600", selfHealBackoffFactorKey: "1", selfHealBackoffCapKey: "3600"},
	}
	base := newTestReconciler(t, withApps([]client.Object{testProject(nil), p, params}, db, testApp("api"))...)
	r := &ApplicationSyncPolicyReconciler{Client: base.Client, APIReader: base.APIReader, ArgoCDNamespace: testNamespace}

	got := reconcilePolicy(t, r, p)
	finishRun(t, r, got, syncv1alpha1.ApplicationSyncPhaseSucceeded)
	// The first self-heal starts right away, as auto-sync's does.
	got = reconcilePolicy(t, r, got)
	require.Len(t, listRuns(t, r), 2)
	finishRun(t, r, got, syncv1alpha1.ApplicationSyncPhaseSucceeded)

	// The next one waits for the backoff, and the policy is requeued for when it ends.
	res, err := r.Reconcile(t.Context(), ctrlRequest(got))
	require.NoError(t, err)
	assert.InDelta(t, time.Hour.Seconds(), res.RequeueAfter.Seconds(), 60)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(got), got))
	assert.Len(t, listRuns(t, r), 2)
	assert.Contains(t, got.Status.Message, "self-healing drift since run")
	assert.Equal(t, int64(1), got.Status.SelfHealAttempts)
}

func TestPolicySuspend(t *testing.T) {
	p := newTestPolicy("")
	p.Spec.Suspend = true
	r := newTestPolicyReconciler(t, p, testApp("db"), testApp("api"))

	got := reconcilePolicy(t, r, p)
	assert.Empty(t, listRuns(t, r), "a suspended policy starts no run, even with drift")
	assert.Equal(t, "suspended", got.Status.Message)

	got.Spec.Suspend = false
	require.NoError(t, r.Update(t.Context(), got))
	got = reconcilePolicy(t, r, got)
	assert.Len(t, listRuns(t, r), 1, "resuming starts the change that arrived meanwhile")
	assert.Contains(t, got.Status.Message, "started run")
}

func TestPolicyNoSelfHeal(t *testing.T) {
	db := testApp("db")
	db.Status.Sync.Revision = "aaa111"
	p := newTestPolicy("")
	r := newTestPolicyReconciler(t, p, db, testApp("api"))

	got := reconcilePolicy(t, r, p)
	finishRun(t, r, got, syncv1alpha1.ApplicationSyncPhaseSucceeded)
	got = reconcilePolicy(t, r, got)
	assert.Len(t, listRuns(t, r), 1, "without selfHeal, drift at the same change is left alone")
	assert.Contains(t, got.Status.Message, "already handled this change")
}

func TestPolicyRefusesAutoSyncedApp(t *testing.T) {
	db := testApp("db")
	db.Spec.SyncPolicy = &argov1alpha1.SyncPolicy{Automated: &argov1alpha1.SyncPolicyAutomated{}}
	p := newTestPolicy("")
	r := newTestPolicyReconciler(t, p, db, testApp("api"))

	got := reconcilePolicy(t, r, p)
	assert.Empty(t, listRuns(t, r))
	assert.Contains(t, got.Status.Message, `application "db": auto-sync is enabled`)
}

// TestPolicyIgnoresStaleCurrentRun covers a cached policy that hasn't caught up with the status update recording
// the run it just created: it must not start a second run for the same change.
func TestPolicyIgnoresStaleCurrentRun(t *testing.T) {
	db := testApp("db")
	db.Status.Sync.Revision = "aaa111"
	p := newTestPolicy("")
	r := newTestPolicyReconciler(t, p, db, testApp("api"))

	stale := p.DeepCopy()
	_, err := r.reconcilePolicy(t.Context(), p)
	require.NoError(t, err)
	require.Len(t, listRuns(t, r), 1)

	_, err = r.reconcilePolicy(t.Context(), stale)
	require.True(t, apierrors.IsConflict(err), "bumping runCount fails on the stale resourceVersion: %v", err)
	require.Len(t, listRuns(t, r), 1)
}

// TestPolicyIgnoresForgedRuns covers ApplicationSyncs made to look like the policy's run. Their label, owner
// reference and name can all be copied, and even a name in the policy's status isn't enough without its UID.
func TestPolicyIgnoresForgedRuns(t *testing.T) {
	db := testApp("db")
	db.Status.Sync.Revision = "aaa111"
	p := newTestPolicy(syncv1alpha1.ApplicationSyncConcurrencyForbid)
	r := newTestPolicyReconciler(t, p, db, testApp("api"))
	forge := func(name string) {
		run := newSync(group("data", nil, "db"))
		run.Name, run.UID = name, ""
		run.Labels = map[string]string{PolicyLabel: p.Name}
		require.NoError(t, controllerutil.SetControllerReference(p, run, r.Scheme()))
		require.NoError(t, r.Create(t.Context(), run))
	}
	forge("platform-forged")
	forge("platform-recorded")
	// As if the policy had recorded this name but not yet the UID its create returned.
	p.Status.CurrentRun = "platform-recorded"
	require.NoError(t, r.Status().Update(t.Context(), p))

	got := reconcilePolicy(t, r, p)
	require.NotContains(t, []string{"platform-forged", "platform-recorded"}, got.Status.CurrentRun)
	own := getRun(t, r, got.Status.CurrentRun)
	assert.Equal(t, own.UID, got.Status.CurrentRunUID)
	assert.Contains(t, got.Status.Message, "started run "+own.Name)

	// The unfinished forgeries don't hold up a Forbid policy: once its own run is done, the next change starts.
	own.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
	require.NoError(t, r.Status().Update(t.Context(), &own))
	updateTestApp(t, r.Client, "db", func(app *argov1alpha1.Application) { app.Status.Sync.Revision = "bbb222" })
	got = reconcilePolicy(t, r, got)
	assert.NotEqual(t, own.Name, got.Status.CurrentRun)
	assert.Contains(t, got.Status.Message, "started run")
}

// TestPolicyCancelsUnrecordedRuns covers a run the policy created but couldn't record before a restart, and a
// forged one: both name the policy as controller, so both are cancelled before the policy starts its own run.
func TestPolicyCancelsUnrecordedRuns(t *testing.T) {
	db := testApp("db")
	db.Status.Sync.Revision = "aaa111"
	p := newTestPolicy(syncv1alpha1.ApplicationSyncConcurrencyForbid)
	r := newTestPolicyReconciler(t, p, db, testApp("api"))
	create := func(name string, owned bool, phase syncv1alpha1.ApplicationSyncPhase) {
		run := newSync(group("data", nil, "db"))
		run.Name, run.UID = name, ""
		if owned {
			require.NoError(t, controllerutil.SetControllerReference(p, run, r.Scheme()))
		}
		require.NoError(t, r.Create(t.Context(), run))
		if phase != "" {
			run.Status.Phase = phase
			require.NoError(t, r.Status().Update(t.Context(), run))
		}
	}
	create("orphan", true, syncv1alpha1.ApplicationSyncPhaseRunning)
	create("forged", true, "")
	create("orphan-done", true, syncv1alpha1.ApplicationSyncPhaseSucceeded)
	create("unrelated", false, syncv1alpha1.ApplicationSyncPhaseRunning)

	got := reconcilePolicy(t, r, p)
	var names []string
	for _, run := range listRuns(t, r) {
		names = append(names, run.Name)
	}
	assert.ElementsMatch(t, []string{"orphan-done", "unrelated", got.Status.CurrentRun}, names,
		"unfinished runs naming the policy as controller are cancelled; finished ones stay as history")
}

// TestPolicySelfHealSkipsAppsLeftOutOfSync covers doneWhen: Healthy, where a run can succeed with an app still
// OutOfSync. That isn't drift, so self-healing it would start a new run on every backoff.
func TestPolicySelfHealSkipsAppsLeftOutOfSync(t *testing.T) {
	for _, tc := range []struct {
		name      string
		endedWith argov1alpha1.SyncStatusCode
		apiDrifts bool
		healed    []string // apps in the self-heal run; nil means no run
	}{
		{"left OutOfSync, nothing else drifted", argov1alpha1.SyncStatusCodeOutOfSync, false, nil},
		{"left OutOfSync, another app drifted", argov1alpha1.SyncStatusCodeOutOfSync, true, []string{"api"}},
		{"ended Synced and drifted", argov1alpha1.SyncStatusCodeSynced, false, []string{"db", "api"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, api := testApp("db"), testApp("api")
			db.Status.Sync.Revision = "aaa111"
			api.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced
			api.Status.Sync.Revision = "aaa111"
			p := newTestPolicy("")
			p.Spec.SelfHeal = true
			r := newTestPolicyReconciler(t, p, db, api)

			got := reconcilePolicy(t, r, p)
			var run syncv1alpha1.ApplicationSync
			require.NoError(t, r.Get(t.Context(), client.ObjectKey{Namespace: testNamespace, Name: got.Status.CurrentRun}, &run))
			run.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
			run.Status.FinishedAt = new(metav1.Now())
			run.Status.Groups = []syncv1alpha1.ApplicationSyncGroupStatus{
				{Name: "data", Phase: syncv1alpha1.ApplicationSyncPhaseSucceeded, Apps: []syncv1alpha1.ApplicationSyncAppStatus{
					{Name: "db", Phase: syncv1alpha1.ApplicationSyncPhaseSucceeded, SyncStatus: tc.endedWith},
				}},
				{Name: "backend", Phase: syncv1alpha1.ApplicationSyncPhaseSucceeded, Apps: []syncv1alpha1.ApplicationSyncAppStatus{
					{Name: "api", Phase: syncv1alpha1.ApplicationSyncPhaseSucceeded, SyncStatus: argov1alpha1.SyncStatusCodeSynced},
				}},
			}
			require.NoError(t, r.Status().Update(t.Context(), &run))
			if tc.apiDrifts {
				updateTestApp(t, r.Client, "api", func(app *argov1alpha1.Application) { app.Status.Sync.Status = argov1alpha1.SyncStatusCodeOutOfSync })
			}

			got = reconcilePolicy(t, r, got)
			if tc.healed == nil {
				assert.Len(t, listRuns(t, r), 1)
				assert.Contains(t, got.Status.Message, "left db OutOfSync; nothing else drifted")
				return
			}
			require.Len(t, listRuns(t, r), 2)
			var healed []string
			for _, g := range getRun(t, r, got.Status.CurrentRun).Spec.Groups {
				for _, a := range g.Apps {
					healed = append(healed, a.Name)
				}
			}
			assert.Equal(t, tc.healed, healed)
			if !tc.apiDrifts {
				return
			}

			// The heal run left db out, so it has no entry for db; db must still count as left OutOfSync.
			heal := getRun(t, r, got.Status.CurrentRun)
			heal.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
			heal.Status.FinishedAt = new(metav1.Now())
			heal.Status.Groups = []syncv1alpha1.ApplicationSyncGroupStatus{{Name: "backend", Phase: syncv1alpha1.ApplicationSyncPhaseSucceeded, Apps: []syncv1alpha1.ApplicationSyncAppStatus{
				{Name: "api", Phase: syncv1alpha1.ApplicationSyncPhaseSucceeded, SyncStatus: argov1alpha1.SyncStatusCodeSynced},
			}}}
			require.NoError(t, r.Status().Update(t.Context(), &heal))
			updateTestApp(t, r.Client, "api", func(app *argov1alpha1.Application) { app.Status.Sync.Status = argov1alpha1.SyncStatusCodeSynced })
			got = reconcilePolicy(t, r, got)
			assert.Len(t, listRuns(t, r), 2, "db isn't self-healed on the next round either")
			assert.Contains(t, got.Status.Message, "left db OutOfSync; nothing else drifted")
		})
	}
}
