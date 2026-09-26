package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	syncv1alpha1 "github.com/argoproj-labs/argocd-application-sync-crd/api/v1alpha1"
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
	if err := r.reconcilePolicy(ctx, &p); err != nil {
		return ctrl.Result{}, err
	}
	if !equality.Semantic.DeepEqual(*orig, p.Status) {
		if err := r.Status().Update(ctx, &p); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *ApplicationSyncPolicyReconciler) reconcilePolicy(ctx context.Context, p *syncv1alpha1.ApplicationSyncPolicy) error {
	if _, err := groupOrder(p.Spec.Template.Groups); err != nil {
		p.Status.Message = "invalid template: " + err.Error()
		return nil
	}
	names := appNames(p.Spec.Template)
	if len(names) == 0 {
		p.Status.Message = "the template lists no applications"
		return nil
	}

	var current *syncv1alpha1.ApplicationSync
	if p.Status.CurrentRun != "" {
		var s syncv1alpha1.ApplicationSync
		err := r.Get(ctx, types.NamespacedName{Namespace: p.Namespace, Name: p.Status.CurrentRun}, &s)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if err == nil {
			current = &s
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
		// Only act on status computed against the app's current spec.
		if app.Status.ReconciledAt == nil || !comparedToCurrentSpec(app) {
			if err := argo.requestRefresh(ctx, app); err != nil {
				log.FromContext(ctx).Error(err, "requesting refresh", "application", name)
			}
			p.Status.Message = fmt.Sprintf("waiting for application %q to be refreshed", name)
			return nil
		}
		if app.Status.Sync.Status == argov1alpha1.SyncStatusCodeOutOfSync {
			drifted = true
		}
	}
	if !drifted {
		p.Status.Message = "all applications are Synced"
		return nil
	}

	change := policyChange(names, apps)
	if current != nil && current.Annotations[policyChangeAnnotation] == change {
		// Same revisions and sources as the last run: like auto-sync, don't retry what was already attempted.
		if current.Status.Phase.Completed() {
			p.Status.Message = fmt.Sprintf("run %s already handled this change (%s)", current.Name, current.Status.Phase)
		} else {
			p.Status.Message = fmt.Sprintf("run %s is in progress", current.Name)
		}
		return nil
	}
	if current != nil && !current.Status.Phase.Completed() {
		if p.Spec.ConcurrencyPolicy != syncv1alpha1.ApplicationSyncConcurrencyReplace {
			p.Status.Message = fmt.Sprintf("waiting for run %s to finish before starting the next one", current.Name)
			return nil
		}
		// Operations the old run already started finish; nothing new starts.
		if err := r.Delete(ctx, current); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("replacing run %s: %w", current.Name, err)
		}
	}

	run := &syncv1alpha1.ApplicationSync{}
	run.GenerateName = p.Name + "-"
	run.Namespace = p.Namespace
	run.Labels = map[string]string{PolicyLabel: p.Name}
	run.Annotations = map[string]string{policyChangeAnnotation: change}
	run.Spec = *p.Spec.Template.DeepCopy()
	run.Spec.InitiatedBy = argov1alpha1.OperationInitiator{Automated: true}
	pinFromStatus(&run.Spec, apps)
	if err := controllerutil.SetControllerReference(p, run, r.Scheme()); err != nil {
		return err
	}
	if err := r.Create(ctx, run); err != nil {
		return fmt.Errorf("creating run: %w", err)
	}
	p.Status.CurrentRun = run.Name
	p.Status.Message = "started run " + run.Name
	return nil
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
