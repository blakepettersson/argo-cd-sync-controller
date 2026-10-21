package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	// policyChangeAnnotation records, on each run, the change it was created for, so a policy doesn't start a
	// second run for the same revisions and sources (as auto-sync avoids re-syncing a revision it already
	// attempted).
	policyChangeAnnotation = "argocd.argoproj.io/application-sync-policy-change"
	// policyRunAnnotation numbers a policy's runs from 1, so the latest one is known without relying on
	// creationTimestamp, which only has one-second resolution.
	policyRunAnnotation = "argocd.argoproj.io/application-sync-policy-run"
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
		reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: p.Namespace, Name: p.Name}})
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

	// Found live rather than through status.currentRun: the cached policy can lag behind the status update that
	// recorded a run just created, or that update can fail, and either way a second run would start.
	var runs syncv1alpha1.ApplicationSyncList
	if err := r.APIReader.List(ctx, &runs, client.InNamespace(p.Namespace), client.MatchingLabels{PolicyLabel: p.Name}); err != nil {
		return 0, err
	}
	var current *syncv1alpha1.ApplicationSync
	for i := range runs.Items {
		run := &runs.Items[i]
		if metav1.IsControlledBy(run, p) && (current == nil || runNumber(current) < runNumber(run)) {
			current = run
		}
	}
	if current != nil {
		p.Status.CurrentRun = current.Name
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
	if current != nil && current.Annotations[policyChangeAnnotation] == change {
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
		// Only drift that appeared after the run is healed. An app the run left OutOfSync, as doneWhen: Healthy
		// allows, would otherwise start a new run on every backoff forever.
		if name := leftOutOfSync(current, apps); name != "" {
			p.Status.Message = fmt.Sprintf("run %s left application %q OutOfSync; not self-healing this change", current.Name, name)
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

	run := &syncv1alpha1.ApplicationSync{}
	run.GenerateName = p.Name + "-"
	run.Namespace = p.Namespace
	run.Labels = map[string]string{PolicyLabel: p.Name}
	next := 1
	if current != nil {
		next = runNumber(current) + 1
	}
	run.Annotations = map[string]string{policyChangeAnnotation: change, policyRunAnnotation: strconv.Itoa(next)}
	run.Spec = *p.Spec.Template.DeepCopy()
	pinFromStatus(&run.Spec, apps)
	if selfHeal {
		limitToDrift(&run.Spec, apps)
	}
	if err := controllerutil.SetControllerReference(p, run, r.Scheme()); err != nil {
		return 0, err
	}
	if err := r.Create(ctx, run); err != nil {
		return 0, fmt.Errorf("creating run: %w", err)
	}
	p.Status.CurrentRun = run.Name
	p.Status.Message = "started run " + run.Name
	// A new change resets the count, as a new revision does for auto-sync.
	if selfHeal {
		p.Status.SelfHealAttempts++
	} else {
		p.Status.SelfHealAttempts = 0
	}
	return 0, nil
}

// leftOutOfSync returns an app that is OutOfSync now and wasn't Synced when run finished with it.
func leftOutOfSync(run *syncv1alpha1.ApplicationSync, apps map[string]*argov1alpha1.Application) string {
	for _, g := range run.Status.Groups {
		for _, st := range g.Apps {
			if app := apps[st.Name]; app != nil && app.Status.Sync.Status == argov1alpha1.SyncStatusCodeOutOfSync && st.SyncStatus != argov1alpha1.SyncStatusCodeSynced {
				return st.Name
			}
		}
	}
	return ""
}

func runNumber(run *syncv1alpha1.ApplicationSync) int {
	n, _ := strconv.Atoi(run.Annotations[policyRunAnnotation])
	return n
}

// limitToDrift makes a self-heal run sync only what drifted, as automated.selfHeal does: Synced apps are
// skipped, and the others sync only their resources that aren't Synced. Resources set in the template are kept.
func limitToDrift(spec *syncv1alpha1.ApplicationSyncSpec, apps map[string]*argov1alpha1.Application) {
	spec.SkipIfSynced = true
	for gi := range spec.Groups {
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
	return hex.EncodeToString(sum[:])[:16]
}
