package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

const (
	// policyAppIndex indexes ApplicationSyncPolicies by the name of each app in their template.
	policyAppIndex = "spec.template.groups.apps.name"
	// PolicyLabel names the policy that created an ApplicationSync.
	PolicyLabel = "argocd.argoproj.io/application-sync-policy"
)

// ApplicationSyncPolicyReconciler creates an ApplicationSync from a policy's template when one of its apps is
// OutOfSync. It is woken by app changes, which happen at least every timeout.reconciliation, the same period
// auto-sync runs on.
type ApplicationSyncPolicyReconciler struct {
	client.Client
	APIReader       client.Reader
	ArgoCDNamespace string
}

// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncpolicies/status,verbs=get;update;patch
// Runs carry a blocking controller owner reference to their policy, which clusters enforcing owner reference
// permissions only allow with update on the policy's finalizers.
// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncs,verbs=get;list;watch;create;delete

func (r *ApplicationSyncPolicyReconciler) argoCD() *argoCD {
	return &argoCD{client: r.Client, apiReader: r.APIReader, namespace: r.ArgoCDNamespace}
}

// SetupWithManager wakes a policy when one of its apps or its runs changes.
func (r *ApplicationSyncPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(context.Background(), &syncv1alpha1.ApplicationSyncPolicy{}, policyAppIndex, func(obj client.Object) []string {
		return appNames(obj.(*syncv1alpha1.ApplicationSyncPolicy).Spec.Template)
	})
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&syncv1alpha1.ApplicationSyncPolicy{}).
		Owns(&syncv1alpha1.ApplicationSync{}).
		Watches(&argov1alpha1.Application{}, handler.EnqueueRequestsFromMapFunc(r.policiesForApp)).
		Named("applicationsyncpolicy").
		Complete(r)
}

func (r *ApplicationSyncPolicyReconciler) policiesForApp(ctx context.Context, obj client.Object) []reconcile.Request {
	var list syncv1alpha1.ApplicationSyncPolicyList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace()), client.MatchingFields{policyAppIndex: obj.GetName()}); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for _, p := range list.Items {
		reqs = append(reqs, reconcile.Request{Namespace: p.Namespace, Name: p.Name})
	}
	return reqs
}

func (r *ApplicationSyncPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var p syncv1alpha1.ApplicationSyncPolicy
	if err := r.Get(ctx, req.NamespacedName, &p); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := p.Status.DeepCopy()
	requeueAfter, err := r.reconcilePolicy(ctx, &p)
	if err != nil {
		return ctrl.Result{}, err
	}
	p.Status.ObservedGeneration = p.Generation
	if !equality.Semantic.DeepEqual(*orig, p.Status) {
		if err := r.Status().Update(ctx, &p); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// reconcilePolicy returns how long to wait before looking again, when self-heal is backing off.
func (r *ApplicationSyncPolicyReconciler) reconcilePolicy(ctx context.Context, p *syncv1alpha1.ApplicationSyncPolicy) (time.Duration, error) {
	if _, err := groupOrder(p.Spec.Template.Groups); err != nil {
		p.Status.Message = "invalid template: " + err.Error()
		return 0, nil
	}
	names := appNames(p.Spec.Template)
	if len(names) == 0 {
		p.Status.Message = "the template lists no applications"
		return 0, nil
	}
	if p.Spec.Suspend {
		p.Status.Message = "suspended"
		return 0, nil
	}

	// Labels, owner references and annotations can be set by anyone who can create ApplicationSyncs, so the
	// policy's run is the one whose name and UID its own status records, as returned by the policy's create.
	var current *syncv1alpha1.ApplicationSync
	if p.Status.CurrentRun != "" && p.Status.CurrentRunUID != "" {
		var run syncv1alpha1.ApplicationSync
		err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: p.Namespace, Name: p.Status.CurrentRun}, &run)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			return 0, err
		case run.UID == p.Status.CurrentRunUID:
			current = &run
		}
	}

	argo := r.argoCD()
	apps := make(map[string]*argov1alpha1.Application, len(names))
	drifted := false
	for _, name := range names {
		app, err := argo.getApp(ctx, p.Namespace, name)
		if err != nil {
			continue
		}
		apps[name] = app
		if autoSyncEnabled(app) {
			p.Status.Message = fmt.Sprintf("application %q: %s", name, errAutoSyncEnabled)
			return 0, nil
		}
		// Only act on status computed against the app's current spec.
		if app.Status.ReconciledAt == nil || !comparedToCurrentSpec(app) {
			if err := argo.requestRefresh(ctx, app); err != nil {
				log.FromContext(ctx).Error(err, "requesting refresh", "application", name)
			}
			p.Status.Message = fmt.Sprintf("waiting for application %q to be refreshed", name)
			return 0, nil
		}
		if app.Status.Sync.Status == argov1alpha1.SyncStatusCodeOutOfSync {
			drifted = true
		}
	}
	if !drifted {
		p.Status.Message = "all applications are Synced"
		return 0, nil
	}

	change := policyChange(names, apps)
	selfHeal := false
	var left map[string]bool
	if current != nil && p.Status.CurrentRunChange == change {
		// Same revisions and sources as the last run: like auto-sync, don't retry what was already attempted,
		// and only self-heal drift after a run that succeeded.
		if !current.Status.Phase.Completed() {
			p.Status.Message = fmt.Sprintf("run %s is in progress", current.Name)
			return 0, nil
		}
		if !p.Spec.SelfHeal || current.Status.Phase != syncv1alpha1.ApplicationSyncPhaseSucceeded {
			p.Status.Message = fmt.Sprintf("run %s already handled this change (%s)", current.Name, current.Status.Phase)
			return 0, nil
		}
		// Only drift that appeared after the run is healed. An app the run left OutOfSync, as doneWhen: Healthy or a
		// template pin behind its targetRevision allows, would otherwise start a new run on every backoff forever, so
		// it is left out and the others are still healed.
		left = leftOutOfSync(current, apps)
		healable := false
		for name, app := range apps {
			if app.Status.Sync.Status == argov1alpha1.SyncStatusCodeOutOfSync && !left[name] {
				healable = true
			}
		}
		if !healable {
			p.Status.Message = fmt.Sprintf("run %s left %s OutOfSync; nothing else drifted", current.Name, strings.Join(slices.Sorted(maps.Keys(left)), ", "))
			return 0, nil
		}
		remaining, err := argo.selfHealDelay(ctx, current.Status.FinishedAt, int(p.Status.SelfHealAttempts))
		if err != nil {
			return 0, err
		}
		if remaining > 0 {
			p.Status.Message = fmt.Sprintf("self-healing drift since run %s in %s", current.Name, remaining.Round(time.Second))
			return remaining, nil
		}
		selfHeal = true
	}
	if current != nil && !current.Status.Phase.Completed() {
		if p.Spec.ConcurrencyPolicy != syncv1alpha1.ApplicationSyncConcurrencyReplace {
			p.Status.Message = fmt.Sprintf("waiting for run %s to finish before starting the next one", current.Name)
			return 0, nil
		}
		// Operations the old run already started finish; nothing new starts.
		if err := r.Delete(ctx, current); err != nil && !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("replacing run %s: %w", current.Name, err)
		}
	}

	run, err := r.createRun(ctx, p, change, func(run *syncv1alpha1.ApplicationSync) {
		run.Spec = *p.Spec.Template.DeepCopy()
		pinFromStatus(&run.Spec, apps)
		if selfHeal {
			limitToDrift(&run.Spec, apps, left)
		}
	})
	if err != nil {
		return 0, err
	}
	p.Status.Message = "started run " + run.Name
	// A new change resets the count, as a new revision does for auto-sync.
	if selfHeal {
		p.Status.SelfHealAttempts++
	} else {
		p.Status.SelfHealAttempts = 0
	}
	return 0, nil
}

// leftOutOfSync returns the apps that are OutOfSync now and weren't Synced when run finished with them. An app the
// run doesn't list was left out of a self-heal run for that reason, so it stays left out.
func leftOutOfSync(run *syncv1alpha1.ApplicationSync, apps map[string]*argov1alpha1.Application) map[string]bool {
	listed := map[string]bool{}
	for _, name := range appNames(run.Spec) {
		listed[name] = true
	}
	endedWith := map[string]argov1alpha1.SyncStatusCode{}
	for _, g := range run.Status.Groups {
		for _, st := range g.Apps {
			endedWith[st.Name] = st.SyncStatus
		}
	}
	left := map[string]bool{}
	for name, app := range apps {
		status, ok := endedWith[name]
		if app.Status.Sync.Status == argov1alpha1.SyncStatusCodeOutOfSync && (!listed[name] || ok && status != argov1alpha1.SyncStatusCodeSynced) {
			left[name] = true
		}
	}
	return left
}

// cancelUnrecordedRuns deletes every unfinished run that names p as its controller but isn't its recorded run: one
// left by a restart between creating a run and recording it, or a forgery. Naming p as controller hands it the run's
// lifecycle, as garbage collection does when p is deleted, so no other run is touched, and p never has two runs
// going. Operations a deleted run already started finish; nothing new starts.
func (r *ApplicationSyncPolicyReconciler) cancelUnrecordedRuns(ctx context.Context, p *syncv1alpha1.ApplicationSyncPolicy) error {
	var runs syncv1alpha1.ApplicationSyncList
	if err := r.APIReader.List(ctx, &runs, client.InNamespace(p.Namespace)); err != nil {
		return err
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if !metav1.IsControlledBy(run, p) || run.Status.Phase.Completed() || run.UID == p.Status.CurrentRunUID {
			continue
		}
		if err := r.Delete(ctx, run, client.Preconditions{UID: &run.UID}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cancelling unrecorded run %s: %w", run.Name, err)
		}
	}
	return nil
}

// createRun creates the next run and records the name and UID its create returned. It first bumps
// status.runCount, so a stale cached policy fails on its outdated resourceVersion instead of starting a duplicate
// run. If the controller stops before the run is recorded, the run is never adopted: the next pass cancels it
// before starting another.
func (r *ApplicationSyncPolicyReconciler) createRun(ctx context.Context, p *syncv1alpha1.ApplicationSyncPolicy, change string, fill func(*syncv1alpha1.ApplicationSync)) (*syncv1alpha1.ApplicationSync, error) {
	p.Status.RunCount++
	if err := r.Status().Update(ctx, p); err != nil {
		return nil, fmt.Errorf("recording the next run: %w", err)
	}
	// Only now that p's status is known to be current, or the status update above would have failed.
	if err := r.cancelUnrecordedRuns(ctx, p); err != nil {
		return nil, err
	}
	run := &syncv1alpha1.ApplicationSync{}
	run.GenerateName = p.Name + "-"
	run.Namespace = p.Namespace
	run.Labels = map[string]string{PolicyLabel: p.Name}
	fill(run)
	if err := controllerutil.SetControllerReference(p, run, r.Scheme()); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, run); err != nil {
		return nil, fmt.Errorf("creating run: %w", err)
	}
	p.Status.CurrentRun = run.Name
	p.Status.CurrentRunUID = run.UID
	p.Status.CurrentRunChange = change
	return run, nil
}

// limitToDrift makes a self-heal run sync only what drifted, as automated.selfHeal does: Synced apps are
// skipped, and the others sync only their resources that aren't Synced. Resources set in the template are kept.
// Apps in left, which the last run left OutOfSync, are dropped.
func limitToDrift(spec *syncv1alpha1.ApplicationSyncSpec, apps map[string]*argov1alpha1.Application, left map[string]bool) {
	spec.SkipIfSynced = true
	for gi := range spec.Groups {
		spec.Groups[gi].Apps = slices.DeleteFunc(spec.Groups[gi].Apps, func(a syncv1alpha1.ApplicationSyncApp) bool { return left[a.Name] })
		for ai := range spec.Groups[gi].Apps {
			a := &spec.Groups[gi].Apps[ai]
			app := apps[a.Name]
			if app == nil || len(a.Resources) > 0 {
				continue
			}
			for _, r := range app.Status.Resources {
				if r.Status != argov1alpha1.SyncStatusCodeSynced {
					a.Resources = append(a.Resources, argov1alpha1.SyncOperationResource{Group: r.Group, Kind: r.Kind, Name: r.Name})
				}
			}
		}
	}
}

// pinFromStatus pins every source of every app to the revision its latest comparison resolved, so the whole
// run rolls out one change. Revisions set in the template are kept.
func pinFromStatus(spec *syncv1alpha1.ApplicationSyncSpec, apps map[string]*argov1alpha1.Application) {
	for gi := range spec.Groups {
		for ai := range spec.Groups[gi].Apps {
			a := &spec.Groups[gi].Apps[ai]
			app := apps[a.Name]
			if app == nil || len(a.Revisions) > 0 || app.Spec.SourceHydrator != nil {
				continue
			}
			revisions := app.Status.GetRevisions()
			if !app.Spec.HasMultipleSources() {
				if len(revisions) == 1 && revisions[0] != "" {
					a.Revisions = []syncv1alpha1.ApplicationSyncRevision{{Revision: revisions[0]}}
				}
				continue
			}
			for i, rev := range revisions {
				if rev != "" {
					a.Revisions = append(a.Revisions, syncv1alpha1.ApplicationSyncRevision{Position: int32(i + 1), Revision: rev})
				}
			}
		}
	}
}

// policyChangeLength is how many hex characters of the hash policyChange keeps: 64 bits, so two different
// changes practically never collide, in a value short enough to read in the policy's status.
const policyChangeLength = 16

// policyChange identifies the revisions and sources the apps are currently compared against.
func policyChange(names []string, apps map[string]*argov1alpha1.Application) string {
	type appChange struct {
		Name      string                              `json:"name"`
		Revisions []string                            `json:"revisions"`
		Sources   argov1alpha1.ApplicationSources     `json:"sources"`
		Dest      argov1alpha1.ApplicationDestination `json:"destination"`
	}
	changes := make([]appChange, 0, len(names))
	for _, name := range names {
		if app := apps[name]; app != nil {
			changes = append(changes, appChange{Name: name, Revisions: app.Status.GetRevisions(), Sources: app.Spec.GetSources(), Dest: app.Spec.Destination})
		}
	}
	b, _ := json.Marshal(changes)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:policyChangeLength]
}
