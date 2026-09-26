package controller

import (
	"testing"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	syncv1alpha1 "github.com/argoproj-labs/argocd-application-sync-crd/api/v1alpha1"
)

func ctrlRequest(obj client.Object) reconcile.Request {
	return reconcile.Request{NamespacedName: client.ObjectKeyFromObject(obj)}
}

func newTestPolicy(concurrency syncv1alpha1.ApplicationSyncConcurrencyPolicy) *syncv1alpha1.ApplicationSyncPolicy {
	return &syncv1alpha1.ApplicationSyncPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "platform", Namespace: testNamespace, UID: "policy-uid"},
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
	assert.True(t, run.Spec.InitiatedBy.Automated, "policy runs are automatic syncs")
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
