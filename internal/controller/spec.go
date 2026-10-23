package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/text"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

// groupOrder returns indexes into groups in dependency order, or an error if dependsOn names an unknown group,
// the dependencies form a cycle, an app is listed in more than one group, or maxParallel is below 1 or 1%.
func groupOrder(groups []syncv1alpha1.ApplicationSyncGroup) ([]int, error) {
	index := make(map[string]int, len(groups))
	appGroup := map[string]string{}
	for i, g := range groups {
		if _, ok := index[g.Name]; ok {
			return nil, fmt.Errorf("group %q is listed more than once", g.Name)
		}
		index[g.Name] = i
		// The CRD can't check the string form without exceeding its CEL cost budget.
		if g.MaxParallel != nil {
			// Scaled against 100, a percentage is its own value.
			if v, err := intstr.GetScaledValueFromIntOrPercent(g.MaxParallel, 100, false); err != nil || v < 1 {
				return nil, fmt.Errorf("group %q: maxParallel %q is not a number or a percentage of at least 1", g.Name, g.MaxParallel.String())
			}
		}
		for _, a := range g.Apps {
			if other, ok := appGroup[a.Name]; ok {
				return nil, fmt.Errorf("application %q is listed in groups %q and %q", a.Name, other, g.Name)
			}
			appGroup[a.Name] = g.Name
		}
	}
	inDegree := make([]int, len(groups))
	dependents := make([][]int, len(groups))
	for i, g := range groups {
		for _, dep := range g.DependsOn {
			j, ok := index[dep]
			if !ok {
				return nil, fmt.Errorf("group %q depends on %q, which is not in spec.groups", g.Name, dep)
			}
			inDegree[i]++
			dependents[j] = append(dependents[j], i)
		}
	}
	order := make([]int, 0, len(groups))
	for i := range groups {
		if inDegree[i] == 0 {
			order = append(order, i)
		}
	}
	for k := 0; k < len(order); k++ {
		for _, d := range dependents[order[k]] {
			inDegree[d]--
			if inDegree[d] == 0 {
				order = append(order, d)
			}
		}
	}
	if len(order) != len(groups) {
		return nil, errors.New("spec.groups dependsOn contains a cycle")
	}
	return order, nil
}

// appNames returns the apps a spec syncs, in order across its groups.
func appNames(spec syncv1alpha1.ApplicationSyncSpec) []string {
	var names []string
	for _, g := range spec.Groups {
		for _, a := range g.Apps {
			names = append(names, a.Name)
		}
	}
	return names
}

// sourceRevisions maps spec revisions onto the app's sources, returning one entry per source (a single entry
// for a single-source app) with "" for sources that aren't pinned.
func sourceRevisions(app *argov1alpha1.Application, revisions []syncv1alpha1.ApplicationSyncRevision) ([]string, error) {
	if app.Spec.SourceHydrator != nil {
		return nil, errors.New("revisions can't be pinned for applications that use spec.sourceHydrator")
	}
	if !app.Spec.HasMultipleSources() {
		if len(revisions) > 1 {
			return nil, errors.New("a single-source application takes at most one revision")
		}
		if revisions[0].Source != "" || revisions[0].Position != 0 {
			return nil, errors.New("a single-source application's revision can't set source or position")
		}
		return []string{revisions[0].Revision}, nil
	}
	out := make([]string, len(app.Spec.Sources))
	for _, r := range revisions {
		i := -1
		switch {
		case r.Source != "":
			for j, src := range app.Spec.Sources {
				if src.Name == r.Source {
					i = j
				}
			}
			if i < 0 {
				return nil, fmt.Errorf("revision for source %q: the application has no source with that name", r.Source)
			}
		case r.Position > 0:
			if int(r.Position) > len(app.Spec.Sources) {
				return nil, fmt.Errorf("revision for position %d: the application has %d sources", r.Position, len(app.Spec.Sources))
			}
			i = int(r.Position) - 1
		default:
			return nil, fmt.Errorf("revision %q must set source or position, because the application has several sources", r.Revision)
		}
		if out[i] != "" {
			return nil, fmt.Errorf("source %d has more than one revision", i+1)
		}
		out[i] = r.Revision
	}
	return out, nil
}

// checkSources fails if the app's sources changed after its revisions were pinned, so a pin can't land on a
// different source.
func checkSources(app *argov1alpha1.Application, spec syncv1alpha1.ApplicationSyncApp, pinned []string) error {
	if len(spec.Revisions) == 0 {
		return nil
	}
	current, err := sourceRevisions(app, spec.Revisions)
	if err != nil {
		return err
	}
	if len(current) != len(pinned) {
		return errors.New("the application's sources changed after its revisions were pinned")
	}
	for i := range current {
		if (current[i] == "") != (pinned[i] == "") {
			return errors.New("the application's sources changed after its revisions were pinned")
		}
	}
	return nil
}

// pinRevisions matches an app's listed revisions to its sources the first time the app is seen, and records
// them in status. Revisions are passed to the sync as given; without Argo CD's repo server this controller
// can't resolve a branch to a commit.
func (r *ApplicationSyncReconciler) pinRevisions(ctx context.Context, s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncApp, st *syncv1alpha1.ApplicationSyncAppStatus) {
	if len(spec.Revisions) == 0 || st.PinnedRevisions != nil || st.Phase != syncv1alpha1.ApplicationSyncPhasePending {
		return
	}
	app, err := r.argoCD().getApp(ctx, s.Namespace, st.Name)
	if err != nil {
		return
	}
	pinned, err := r.checkRevisions(ctx, s, app, spec.Revisions)
	if err != nil {
		if s.Spec.OnFailure == syncv1alpha1.ApplicationSyncOnFailureWait {
			st.Message = err.Error() + "; retrying"
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
		st.Message = err.Error()
		return
	}
	st.PinnedRevisions = pinned
}

// checkRevisions applies the Sync API's rules for syncing to a revision other than targetRevision. A pin
// counts as targetRevision if it names it, or names the revision the app last resolved it to.
func (r *ApplicationSyncReconciler) checkRevisions(ctx context.Context, s *syncv1alpha1.ApplicationSync, app *argov1alpha1.Application, revisions []syncv1alpha1.ApplicationSyncRevision) ([]string, error) {
	requested, err := sourceRevisions(app, revisions)
	if err != nil {
		return nil, err
	}
	requireOverride, err := r.argoCD().requireOverride(ctx)
	if err != nil {
		return nil, err
	}
	sources := app.Spec.GetSources()
	resolved := app.Status.GetRevisions()
	for i, rev := range requested {
		if rev == "" {
			continue
		}
		target := text.FirstNonEmpty(sources[i].TargetRevision, "HEAD")
		if rev == target || (i < len(resolved) && rev == resolved[i]) {
			continue
		}
		// Kubernetes RBAC can't express the override privilege, so with the setting on, only targetRevision is
		// allowed.
		if requireOverride {
			return nil, fmt.Errorf("cannot sync source %s to %s: %s allows only targetRevision (%s)", sources[i].RepoURL, rev, requireOverrideKey, target)
		}
		if app.Spec.SyncPolicy != nil && app.Spec.SyncPolicy.IsAutomatedSyncEnabled() && !s.Spec.DryRun {
			return nil, fmt.Errorf("cannot sync to %s: auto-sync currently set to %s", rev, target)
		}
	}
	return requested, nil
}

// gc deletes completed ApplicationSyncs that are no longer among the newest HistoryLimit for any of their apps, so
// each app keeps its last HistoryLimit syncs and the total is bounded by HistoryLimit per app.
func (r *ApplicationSyncReconciler) gc(ctx context.Context, s *syncv1alpha1.ApplicationSync) {
	if r.HistoryLimit <= 0 {
		return
	}
	var list syncv1alpha1.ApplicationSyncList
	if err := r.List(ctx, &list, client.InNamespace(s.Namespace)); err != nil {
		return
	}
	var completed []*syncv1alpha1.ApplicationSync
	for i := range list.Items {
		if list.Items[i].Status.Phase.Completed() {
			completed = append(completed, &list.Items[i])
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		ti, tj := finishedAt(completed[i]), finishedAt(completed[j])
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		// finishedAt has second precision; fall back to creation order, then name.
		ci, cj := completed[i].CreationTimestamp, completed[j].CreationTimestamp
		if !ci.Equal(&cj) {
			return cj.Before(&ci)
		}
		return completed[i].Name > completed[j].Name
	})
	newer := map[string]int{} // completed runs seen so far, newest first, per app
	for _, c := range completed {
		names := slices.Compact(slices.Sorted(slices.Values(appNames(c.Spec))))
		// A run that lists no apps counts toward no app's history; only its TTL deletes it.
		keep := len(names) == 0
		for _, name := range names {
			if newer[name] < r.HistoryLimit {
				keep = true
			}
			newer[name]++
		}
		if keep {
			continue
		}
		current, err := r.isPolicyCurrentRun(ctx, c)
		if err != nil {
			log.FromContext(ctx).Error(err, "checking whether a policy still needs its run", "applicationsync", c.Name)
		}
		if current || err != nil {
			continue
		}
		err = r.Delete(ctx, c, client.Preconditions{UID: &c.UID})
		if err != nil && !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "garbage collecting ApplicationSync", "applicationsync", c.Name)
		}
	}
}

// expire deletes s once ttlSecondsAfterFinished has passed, and otherwise requeues it for then. A policy's latest
// run is kept whatever its TTL.
func (r *ApplicationSyncReconciler) expire(ctx context.Context, s *syncv1alpha1.ApplicationSync) (ctrl.Result, error) {
	if s.Spec.TTLSecondsAfterFinished == nil || s.Status.FinishedAt == nil {
		return ctrl.Result{}, nil
	}
	if remaining := time.Until(s.Status.FinishedAt.Add(time.Duration(*s.Spec.TTLSecondsAfterFinished) * time.Second)); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	current, err := r.isPolicyCurrentRun(ctx, s)
	if err != nil {
		return ctrl.Result{}, err
	}
	if current {
		return ctrl.Result{RequeueAfter: resyncPeriod}, nil
	}
	err = r.Delete(ctx, s, client.Preconditions{UID: &s.UID})
	return ctrl.Result{}, client.IgnoreNotFound(err)
}

// isPolicyCurrentRun reports whether s is the latest run of the policy that owns it, which neither the history
// limit nor a TTL deletes, because the policy's same-change and self-heal checks read it.
func (r *ApplicationSyncReconciler) isPolicyCurrentRun(ctx context.Context, s *syncv1alpha1.ApplicationSync) (bool, error) {
	if !isAutomated(s) {
		return false, nil
	}
	owner := metav1.GetControllerOf(s)
	var p syncv1alpha1.ApplicationSyncPolicy
	err := r.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: owner.Name}, &p)
	if err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return p.UID == owner.UID && p.Status.CurrentRunUID == s.UID, nil
}

func finishedAt(s *syncv1alpha1.ApplicationSync) time.Time {
	if s.Status.FinishedAt != nil {
		return s.Status.FinishedAt.Time
	}
	return s.CreationTimestamp.Time
}
