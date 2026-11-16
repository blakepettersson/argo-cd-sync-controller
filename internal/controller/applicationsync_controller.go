package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	synccommon "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/common"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

const (
	// appIndex indexes ApplicationSyncs by the name of each app they list.
	appIndex = "spec.groups.apps.name"
	// resyncPeriod is a safety net; app changes wake an ApplicationSync straight away.
	resyncPeriod = 30 * time.Second
	// waitingForAppMessage is the wait that only ends if someone creates the app, so its event is a warning.
	waitingForAppMessage = "waiting for application to exist"
	// Routine waits, which recordWaits doesn't report: they are expected, and would emit an event per app.
	waitingForSlotPrefix     = "waiting for one of "
	waitingForRefreshMessage = "waiting for application to be refreshed"
	// waitingToRecoverSuffix ends the message of an app that failed under onFailure: Wait.
	waitingToRecoverSuffix = "; waiting for the application to recover"
)

// ApplicationSyncReconciler runs ApplicationSyncs: it sets each app's operation in group order and reads the
// result back from the app's status.
type ApplicationSyncReconciler struct {
	client.Client
	APIReader client.Reader
	Recorder  events.EventRecorder
	// ArgoCDNamespace holds AppProjects and Argo CD's ConfigMaps.
	ArgoCDNamespace string
	// HistoryLimit caps completed ApplicationSyncs kept per app; 0 keeps them all.
	HistoryLimit int
}

// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncs,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=argoproj.io,resources=applications,verbs=get;list;watch;update;patch
// AppProjects and Argo CD's ConfigMaps live in Argo CD's namespace, so they get a namespaced Role there.
// +kubebuilder:rbac:groups=argoproj.io,namespace=argocd,resources=appprojects,verbs=get;list;watch
// +kubebuilder:rbac:groups="",namespace=argocd,resources=configmaps,verbs=get
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *ApplicationSyncReconciler) argoCD() *argoCD {
	return &argoCD{client: r.Client, apiReader: r.APIReader, namespace: r.ArgoCDNamespace}
}

// SetupWithManager wakes an ApplicationSync whenever one of its apps changes.
func (r *ApplicationSyncReconciler) SetupWithManager(mgr ctrl.Manager) error {
	err := mgr.GetFieldIndexer().IndexField(context.Background(), &syncv1alpha1.ApplicationSync{}, appIndex, func(obj client.Object) []string {
		return appNames(obj.(*syncv1alpha1.ApplicationSync).Spec)
	})
	if err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&syncv1alpha1.ApplicationSync{}).
		Watches(&argov1alpha1.Application{}, handler.EnqueueRequestsFromMapFunc(r.syncsForApp)).
		Named("applicationsync").
		Complete(r)
}

func (r *ApplicationSyncReconciler) syncsForApp(ctx context.Context, obj client.Object) []reconcile.Request {
	var list syncv1alpha1.ApplicationSyncList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace()), client.MatchingFields{appIndex: obj.GetName()}); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, s := range list.Items {
		if !s.Status.Phase.Completed() {
			reqs = append(reqs, reconcile.Request{Namespace: s.Namespace, Name: s.Name})
		}
	}
	return reqs
}

func (r *ApplicationSyncReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var s syncv1alpha1.ApplicationSync
	if err := r.Get(ctx, req.NamespacedName, &s); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	orig := s.Status.DeepCopy()
	// Nothing changes once finished, but a later TTL edit is still recorded as observed.
	if !orig.Phase.Completed() {
		r.reconcileApplicationSync(ctx, &s)
	}
	s.Status.ObservedGeneration = s.Generation
	if !equality.Semantic.DeepEqual(*orig, s.Status) {
		if err := r.Status().Update(ctx, &s); err != nil {
			return ctrl.Result{}, err
		}
		r.recordWaits(orig, &s)
	}
	if s.Status.Phase.Completed() {
		if !orig.Phase.Completed() {
			r.gc(ctx, &s)
		}
		return r.expire(ctx, &s)
	}
	requeue := resyncPeriod
	if d := s.Spec.ActiveDeadlineSeconds; d != nil && s.Status.StartedAt != nil {
		requeue = max(min(requeue, time.Until(s.Status.StartedAt.Add(time.Duration(*d)*time.Second))), time.Second)
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// recordWaits emits an event when an app starts waiting, or waits for a different reason, so kubectl describe and
// event-based alerting show what a run is blocked on.
func (r *ApplicationSyncReconciler) recordWaits(orig *syncv1alpha1.ApplicationSyncStatus, s *syncv1alpha1.ApplicationSync) {
	if r.Recorder == nil {
		return
	}
	before := map[string]string{}
	for _, g := range orig.Groups {
		for _, st := range g.Apps {
			before[st.Name] = st.Message
		}
	}
	for _, g := range s.Status.Groups {
		for _, st := range g.Apps {
			waiting := st.Phase == syncv1alpha1.ApplicationSyncPhasePending || strings.HasSuffix(st.Message, waitingToRecoverSuffix)
			// A recovery wait's message changes with every retry; report it once, when the app starts waiting.
			recovering := strings.HasSuffix(st.Message, waitingToRecoverSuffix) && strings.HasSuffix(before[st.Name], waitingToRecoverSuffix)
			if !waiting || recovering || st.Message == "" || st.Message == before[st.Name] {
				continue
			}
			if st.Message == waitingForRefreshMessage ||
				strings.HasPrefix(st.Message, waitingForSlotPrefix) {
				continue
			}
			eventType := corev1.EventTypeNormal
			if st.Message == waitingForAppMessage {
				eventType = corev1.EventTypeWarning
			}
			r.Recorder.Eventf(s, nil, eventType, "ApplicationWaiting", "Wait", "application %s: %s", st.Name, st.Message)
		}
	}
}

// reconcileApplicationSync advances s.Status.
func (r *ApplicationSyncReconciler) reconcileApplicationSync(ctx context.Context, s *syncv1alpha1.ApplicationSync) {
	now := metav1.Now()
	order, err := groupOrder(s.Spec.Groups)
	if err != nil {
		s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
		s.Status.Message = err.Error()
		s.Status.FinishedAt = &now
		return
	}
	if s.Status.Phase != syncv1alpha1.ApplicationSyncPhaseRunning {
		s.Status = syncv1alpha1.ApplicationSyncStatus{Phase: syncv1alpha1.ApplicationSyncPhaseRunning, StartedAt: &now}
		for _, g := range s.Spec.Groups {
			gs := syncv1alpha1.ApplicationSyncGroupStatus{Name: g.Name, Phase: syncv1alpha1.ApplicationSyncPhasePending}
			for _, a := range g.Apps {
				gs.Apps = append(gs.Apps, syncv1alpha1.ApplicationSyncAppStatus{Name: a.Name, Phase: syncv1alpha1.ApplicationSyncPhasePending})
			}
			s.Status.Groups = append(s.Status.Groups, gs)
		}
	}
	s.Status.Message = ""

	statuses := make(map[string]*syncv1alpha1.ApplicationSyncGroupStatus, len(s.Status.Groups))
	for i := range s.Status.Groups {
		statuses[s.Status.Groups[i].Name] = &s.Status.Groups[i]
	}
	for _, g := range s.Spec.Groups {
		for ai, a := range g.Apps {
			r.pinRevisions(ctx, s, a, &statuses[g.Name].Apps[ai])
		}
	}
	for _, i := range order {
		r.reconcileGroup(ctx, s, s.Spec.Groups[i], statuses, now.Time)
	}
	failed, total := 0, 0
	for _, gs := range s.Status.Groups {
		if !gs.Phase.Completed() {
			// Checked after the groups, so an app that finished in this pass keeps its result.
			if deadlinePassed(s, now.Time) {
				msg := fmt.Sprintf("exceeded activeDeadlineSeconds (%ds)", *s.Spec.ActiveDeadlineSeconds)
				for gi := range s.Status.Groups {
					g := &s.Status.Groups[gi]
					if g.Phase.Completed() {
						continue
					}
					// A group that never started is Skipped, as when a group it depends on fails.
					phase := syncv1alpha1.ApplicationSyncPhaseFailed
					if g.Phase == syncv1alpha1.ApplicationSyncPhasePending {
						phase = syncv1alpha1.ApplicationSyncPhaseSkipped
					}
					g.Phase, g.Message = phase, msg
					for ai := range g.Apps {
						if !g.Apps[ai].Phase.Completed() {
							g.Apps[ai].Phase, g.Apps[ai].Message = phase, msg
						}
					}
				}
				s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
				s.Status.Message = msg
				s.Status.FinishedAt = &now
				return
			}
			setWaitMessage(s)
			return
		}
		for _, st := range gs.Apps {
			total++
			if st.Phase != syncv1alpha1.ApplicationSyncPhaseSucceeded {
				failed++
			}
		}
	}
	s.Status.FinishedAt = &now
	if failed > 0 {
		s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
		s.Status.Message = fmt.Sprintf("%d of %d applications did not succeed", failed, total)
		return
	}
	s.Status.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
	s.Status.Message = ""
}

func deadlinePassed(s *syncv1alpha1.ApplicationSync, now time.Time) bool {
	d := s.Spec.ActiveDeadlineSeconds
	return d != nil && s.Status.StartedAt != nil && !now.Before(s.Status.StartedAt.Add(time.Duration(*d)*time.Second))
}

// setWaitMessage surfaces, in the run's own message, the first app in a running group that is waiting to sync or to
// recover, so kubectl get shows what a run is blocked on.
func setWaitMessage(s *syncv1alpha1.ApplicationSync) {
	if s.Status.Message != "" {
		return
	}
	// A routine wait (a refresh, a free slot) is only shown when nothing else is waiting.
	for _, routine := range []bool{false, true} {
		for _, gs := range s.Status.Groups {
			if gs.Phase != syncv1alpha1.ApplicationSyncPhaseRunning {
				continue
			}
			for _, st := range gs.Apps {
				waiting := st.Phase == syncv1alpha1.ApplicationSyncPhasePending && st.Message != "" || strings.HasSuffix(st.Message, waitingToRecoverSuffix)
				isRoutine := st.Message == waitingForRefreshMessage || strings.HasPrefix(st.Message, waitingForSlotPrefix)
				if waiting && isRoutine == routine {
					s.Status.Message = fmt.Sprintf("application %s: %s", st.Name, st.Message)
					return
				}
			}
		}
	}
}

func (r *ApplicationSyncReconciler) reconcileGroup(ctx context.Context, s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncGroup, statuses map[string]*syncv1alpha1.ApplicationSyncGroupStatus, now time.Time) {
	gs := statuses[spec.Name]
	if gs.Phase == syncv1alpha1.ApplicationSyncPhasePending {
		for _, dep := range spec.DependsOn {
			switch statuses[dep].Phase {
			case syncv1alpha1.ApplicationSyncPhaseSucceeded:
			case syncv1alpha1.ApplicationSyncPhaseFailed, syncv1alpha1.ApplicationSyncPhaseSkipped:
				gs.Phase = syncv1alpha1.ApplicationSyncPhaseSkipped
				gs.Message = fmt.Sprintf("group %q %s", dep, strings.ToLower(string(statuses[dep].Phase)))
				for i := range gs.Apps {
					gs.Apps[i].Phase = syncv1alpha1.ApplicationSyncPhaseSkipped
					gs.Apps[i].Message = gs.Message
				}
				return
			default:
				gs.Message = fmt.Sprintf("waiting for group %q", dep)
				return
			}
		}
		if deadlinePassed(s, now) {
			// Left Pending, so the deadline marks it Skipped: it never started.
			return
		}
		gs.Phase = syncv1alpha1.ApplicationSyncPhaseRunning
		gs.Message = ""
	}
	if gs.Phase != syncv1alpha1.ApplicationSyncPhaseRunning {
		return
	}

	apps := make(map[string]syncv1alpha1.ApplicationSyncApp, len(spec.Apps))
	for _, a := range spec.Apps {
		apps[a.Name] = a
	}
	// Advance the apps already in flight first, so slots they free can be used in the same pass.
	inFlight := 0
	for i := range gs.Apps {
		if gs.Apps[i].Phase == syncv1alpha1.ApplicationSyncPhasePending {
			continue
		}
		r.reconcileApp(ctx, s, apps[gs.Apps[i].Name], &gs.Apps[i])
		if inFlightPhase(gs.Apps[i].Phase) {
			inFlight++
		}
	}
	limit := groupLimit(spec.MaxParallel, len(spec.Apps))
	gs.Message = ""
	for i := range gs.Apps {
		st := &gs.Apps[i]
		if st.Phase != syncv1alpha1.ApplicationSyncPhasePending {
			continue
		}
		switch {
		case deadlinePassed(s, now):
			// The deadline check after the groups fails it; start nothing new.
			continue
		case limit >= 0 && inFlight >= limit:
			st.Message = fmt.Sprintf(waitingForSlotPrefix+"%d parallel slots", limit)
			continue
		}
		r.reconcileApp(ctx, s, apps[st.Name], st)
		if st.Phase == syncv1alpha1.ApplicationSyncPhaseProgressing {
			// Skipped because it was already synced: check its health in the same pass.
			r.reconcileApp(ctx, s, apps[st.Name], st)
		}
		if inFlightPhase(st.Phase) {
			inFlight++
		}
	}

	done, failed := true, 0
	for _, st := range gs.Apps {
		switch {
		case !st.Phase.Completed():
			done = false
		case st.Phase != syncv1alpha1.ApplicationSyncPhaseSucceeded:
			failed++
		}
	}
	if !done {
		return
	}
	if failed > 0 {
		gs.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
		gs.Message = fmt.Sprintf("%d of %d applications did not succeed", failed, len(gs.Apps))
		return
	}
	gs.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
	gs.Message = ""
}

func inFlightPhase(p syncv1alpha1.ApplicationSyncPhase) bool {
	return p == syncv1alpha1.ApplicationSyncPhaseRunning || p == syncv1alpha1.ApplicationSyncPhaseProgressing
}

// groupLimit returns how many apps of a group may be in flight at once, or -1 for no limit. It rounds like
// RollingSync's maxUpdate, except that it never returns 0: the CRD rejects 0, and a percentage rounds up to 1.
func groupLimit(maxParallel *intstr.IntOrString, total int) int {
	if maxParallel == nil {
		return -1
	}
	limit, err := intstr.GetScaledValueFromIntOrPercent(maxParallel, total, false)
	if err != nil {
		// groupOrder rejects these; fail closed if one gets through.
		return 1
	}
	return max(limit, 1)
}

func (r *ApplicationSyncReconciler) reconcileApp(ctx context.Context, s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncApp, st *syncv1alpha1.ApplicationSyncAppStatus) {
	wait := s.Spec.OnFailure == syncv1alpha1.ApplicationSyncOnFailureWait
	// fail ends the app in Fail mode. In Wait mode the app keeps waiting for a retry or a manual fix.
	fail := func(msg string) {
		if wait {
			st.Message = msg + waitingToRecoverSuffix
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
		st.Message = msg
	}
	argo := r.argoCD()

	switch st.Phase {
	case syncv1alpha1.ApplicationSyncPhasePending:
		app, err := argo.getApp(ctx, s.Namespace, st.Name)
		if apierrors.IsNotFound(err) {
			st.Message = waitingForAppMessage
			return
		} else if err != nil {
			st.Message = err.Error()
			return
		}
		if len(spec.Revisions) > 0 && st.PinnedRevisions == nil {
			// pinRevisions failed and left the reason in st.Message.
			return
		}
		// A previous pass may have set the operation but failed to persist the Running phase.
		if isOperationFor(s, app.Operation) || operationStateFor(s, app) != nil {
			st.Phase = syncv1alpha1.ApplicationSyncPhaseRunning
			st.Message = ""
			st.UID = app.UID
			return
		}
		// Argo CD's auto-sync and self-heal would otherwise sync the app too, ahead of its group or away from a pin.
		if autoSyncEnabled(app) {
			fail(errAutoSyncEnabled.Error())
			return
		}
		if err := checkSources(app, spec, st.PinnedRevisions); err != nil {
			fail(err.Error())
			return
		}
		if msg, err := r.windowBlocks(ctx, s, app); err != nil {
			fail(err.Error())
			return
		} else if msg != "" {
			// Wait without writing an operation, so a closed window doesn't hold a slot.
			st.Message = msg
			return
		}
		if s.Spec.SkipIfSynced {
			if !r.isFresh(ctx, app, s.Status.StartedAt) {
				st.Message = waitingForRefreshMessage
				return
			}
			if app.Status.Sync.Status == argov1alpha1.SyncStatusCodeSynced && revisionsMatch(st.PinnedRevisions, app.Status.GetRevisions()) {
				st.Phase = syncv1alpha1.ApplicationSyncPhaseProgressing
				st.Revisions = app.Status.GetRevisions()
				st.Message = "already synced"
				st.UID = app.UID
				return
			}
		}
		if checksEmpty(s, spec) {
			if !r.isFresh(ctx, app, s.Status.StartedAt) {
				st.Message = waitingForRefreshMessage
				return
			}
			if prunesEverything(app) {
				fail("sync would prune every resource of the application; set allowEmpty to allow it")
				return
			}
		}
		err = r.startOperation(ctx, s, spec, st, app)
		if errors.Is(err, errApplicationSyncGone) {
			return
		}
		if apierrors.IsNotFound(err) {
			// The cache hasn't caught up with a deleted app; a missing app waits before its sync starts.
			st.Message = waitingForAppMessage
			return
		}
		if errors.Is(err, errApplicationReplaced) {
			// The cache hasn't caught up with a recreated app; check the new one on a later pass.
			st.Message = "waiting for the cache to catch up with a recreated application"
			return
		}
		if errors.Is(err, errAnotherOperationInProgress) {
			// As the Sync API does, a busy app is a failed start, so onFailure decides whether to wait. A policy
			// doesn't retry a failed change, so its runs always wait.
			if isAutomated(s) {
				st.Message = "waiting for another operation to finish"
				return
			}
			fail(err.Error())
			return
		}
		if err != nil {
			fail(err.Error())
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseRunning
		st.Message = ""
		st.UID = app.UID

	case syncv1alpha1.ApplicationSyncPhaseRunning:
		app, err := argo.getApp(ctx, s.Namespace, st.Name)
		// An app deleted, or replaced by a same-named one, after its sync started never recovers, so it fails even
		// with onFailure: Wait.
		if err != nil || st.UID != "" && app.UID != st.UID {
			st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
			st.Message = "application was deleted or recreated"
			return
		}
		state := operationStateFor(s, app)
		if state == nil && !isOperationFor(s, app.Operation) {
			// The cache may not have caught up with the operation this controller just set.
			live, err := argo.getLiveApp(ctx, app.Namespace, app.Name)
			if apierrors.IsNotFound(err) || err == nil && st.UID != "" && live.UID != st.UID {
				st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
				st.Message = "application was deleted or recreated"
				return
			}
			if err != nil {
				st.Message = err.Error()
				return
			}
			state = operationStateFor(s, live)
			if state == nil && !isOperationFor(s, live.Operation) {
				fail("operation state was replaced before its result was observed")
				if wait {
					st.Phase = syncv1alpha1.ApplicationSyncPhaseProgressing
					st.Revisions = nil
				}
				return
			}
		}
		if state == nil {
			return
		}
		st.StartedAt = state.StartedAt.DeepCopy()
		st.Message = state.Message
		if !state.Phase.Completed() {
			return
		}
		st.FinishedAt = state.FinishedAt.DeepCopy()
		st.Revisions = nil
		if state.SyncResult != nil {
			st.Revisions = state.SyncResult.Revisions
			if len(st.Revisions) == 0 && state.SyncResult.Revision != "" {
				st.Revisions = []string{state.SyncResult.Revision}
			}
		}
		if !state.Phase.Successful() {
			fail(fmt.Sprintf("sync %s: %s", strings.ToLower(string(state.Phase)), state.Message))
			if wait {
				// Only Synced and Healthy ends the wait; the failed sync's revisions don't count.
				st.Phase = syncv1alpha1.ApplicationSyncPhaseProgressing
				st.Revisions = nil
			}
			return
		}
		if s.Spec.DryRun {
			st.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseProgressing
		fallthrough

	case syncv1alpha1.ApplicationSyncPhaseProgressing:
		app, err := argo.getApp(ctx, s.Namespace, st.Name)
		if err != nil || st.UID != "" && app.UID != st.UID {
			st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
			st.Message = "application was deleted or recreated"
			return
		}
		since := st.FinishedAt
		if since == nil {
			since = s.Status.StartedAt
		}
		if !r.isFresh(ctx, app, since) {
			st.Message = waitingForRefreshMessage
			return
		}
		switch app.Status.Health.Status {
		case health.HealthStatusHealthy:
		case health.HealthStatusDegraded:
			fail("application is Degraded")
			return
		default:
			st.Message = fmt.Sprintf("waiting for application to become Healthy (currently %s)", app.Status.Health.Status)
			return
		}
		if s.Spec.DoneWhen != syncv1alpha1.ApplicationSyncDoneWhenHealthy && app.Status.Sync.Status != argov1alpha1.SyncStatusCodeSynced &&
			(len(st.Revisions) == 0 || revisionsMatch(st.Revisions, app.Status.GetRevisions())) && !sourcesChangedSince(s, app) {
			// OutOfSync only blocks while the comparison is still against what was synced. If targetRevision
			// or the sources moved on, whoever created the ApplicationSync decides whether to start another run.
			st.Message = fmt.Sprintf("waiting for application to become Synced (currently %s)", app.Status.Sync.Status)
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
		st.SyncStatus = app.Status.Sync.Status
		st.Message = ""
	}
}

// isFresh reports whether app's status was computed after since and against its current spec, and asks for a
// refresh if not. A refresh that read an older copy of the spec records that spec in comparedTo, so it fails
// the check whatever its timing.
func (r *ApplicationSyncReconciler) isFresh(ctx context.Context, app *argov1alpha1.Application, since *metav1.Time) bool {
	fresh := app.Status.ReconciledAt != nil && (since == nil || !app.Status.ReconciledAt.Before(since)) && comparedToCurrentSpec(app)
	if !fresh {
		r.argoCD().requestRefresh(ctx, app)
	}
	return fresh
}

// sourcesChangedSince reports whether app's sources differ from the ones its sync for s used, the comparison
// Argo CD's auto-sync makes before retrying a change. It is false when the sync's result is no longer on the app.
func sourcesChangedSince(s *syncv1alpha1.ApplicationSync, app *argov1alpha1.Application) bool {
	state := operationStateFor(s, app)
	if state == nil || state.SyncResult == nil {
		return false
	}
	if app.Spec.HasMultipleSources() {
		return !reflect.DeepEqual(app.Spec.Sources, state.SyncResult.Sources)
	}
	return !reflect.DeepEqual(app.Spec.GetSource(), state.SyncResult.Source)
}

// revisionsMatch reports whether actual agrees with want on every source want sets.
func revisionsMatch(want, actual []string) bool {
	for i, w := range want {
		if w != "" && (i >= len(actual) || actual[i] != w) {
			return false
		}
	}
	return true
}

// checksEmpty reports whether an app's sync must not prune every resource, as auto-sync checks unless
// automated.allowEmpty is set. Manual syncs aren't checked, and a dry run deletes nothing.
func checksEmpty(s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncApp) bool {
	prune := s.Spec.Prune
	if spec.Prune != nil {
		prune = *spec.Prune
	}
	return isAutomated(s) && prune && !s.Spec.AllowEmpty && !s.Spec.DryRun
}

// prunesEverything reports whether every resource of app would be pruned. Unlike auto-sync, an app with no
// resources passes, since there is nothing to prune (argoproj/argo-cd#15371).
func prunesEverything(app *argov1alpha1.Application) bool {
	if len(app.Status.Resources) == 0 {
		return false
	}
	for _, r := range app.Status.Resources {
		if !r.RequiresPruning {
			return false
		}
	}
	return true
}

// windowBlocks returns a message if a sync window stops app from syncing right now.
func (r *ApplicationSyncReconciler) windowBlocks(ctx context.Context, s *syncv1alpha1.ApplicationSync, app *argov1alpha1.Application) (string, error) {
	proj, err := r.argoCD().getProject(ctx, app)
	if err != nil {
		return "", err
	}
	canSync, err := proj.Spec.SyncWindows.Matches(app).CanSync(!isAutomated(s), nil)
	if err != nil {
		return "", fmt.Errorf("invalid sync window: %w", err)
	}
	if !canSync {
		return "waiting for a sync window to allow the sync", nil
	}
	return "", nil
}

func (r *ApplicationSyncReconciler) startOperation(ctx context.Context, s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncApp, st *syncv1alpha1.ApplicationSyncAppStatus, app *argov1alpha1.Application) error {
	if app.DeletionTimestamp != nil {
		return errors.New("application is being deleted")
	}
	argo := r.argoCD()
	var retry *argov1alpha1.RetryStrategy
	var syncOptions argov1alpha1.SyncOptions
	if app.Spec.SyncPolicy != nil {
		syncOptions = app.Spec.SyncPolicy.SyncOptions
		retry = app.Spec.SyncPolicy.Retry
	}
	if s.Spec.Retry != nil {
		retry = s.Spec.Retry
	}
	if spec.Retry != nil {
		retry = spec.Retry
	}
	if s.Spec.SyncOptions != nil {
		syncOptions = s.Spec.SyncOptions
	}
	if syncOptions.HasOption(synccommon.SyncOptionReplace) {
		allowed, err := argo.replaceAllowed(ctx)
		if err != nil {
			return err
		}
		if !allowed {
			return errors.New("sync with replace was disabled via server.sync.replace.allowed")
		}
	}
	prune := s.Spec.Prune
	if spec.Prune != nil {
		prune = *spec.Prune
	}

	var source *argov1alpha1.ApplicationSource
	if !app.Spec.HasMultipleSources() {
		src := app.Spec.GetSource()
		source = &src
	}
	op := argov1alpha1.Operation{
		Sync: &argov1alpha1.SyncOperation{
			Source:       source,
			Sources:      app.Spec.Sources,
			Prune:        prune,
			DryRun:       s.Spec.DryRun,
			SyncOptions:  syncOptions,
			SyncStrategy: s.Spec.SyncStrategy,
			Resources:    spec.Resources,
		},
		InitiatedBy: argov1alpha1.OperationInitiator{Username: s.Spec.InitiatedBy.Username, Automated: isAutomated(s)},
		Info: append([]*argov1alpha1.Info{
			{Name: syncv1alpha1.ApplicationSyncOperationInfoName, Value: s.Name},
			{Name: syncv1alpha1.ApplicationSyncOperationUIDInfoName, Value: string(s.UID)},
		}, s.Spec.Info...),
	}
	// An empty entry leaves the source on its targetRevision, resolved when the sync starts.
	if app.Spec.HasMultipleSources() {
		if len(st.PinnedRevisions) > 0 {
			op.Sync.Revisions = st.PinnedRevisions
		}
	} else if len(st.PinnedRevisions) == 1 {
		op.Sync.Revision = st.PinnedRevisions[0]
	}
	if retry != nil {
		op.Retry = *retry
	}
	// The cache can still hold an ApplicationSync that was deleted, for example by argocd-server after a handoff
	// timeout; never start a sync for it.
	var live syncv1alpha1.ApplicationSync
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(s), &live); err != nil {
		if apierrors.IsNotFound(err) {
			return errApplicationSyncGone
		}
		return err
	}
	if live.UID != s.UID || live.DeletionTimestamp != nil {
		return errApplicationSyncGone
	}
	if err := argo.setOperation(ctx, s, app.Namespace, app.Name, app.UID, &op); errors.Is(err, errOperationAlreadySet) {
		return nil
	} else if err != nil {
		return err
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(app, s, corev1.EventTypeNormal, operationStartedEvent, "Sync", "initiated sync from ApplicationSync %s", s.Name)
	}
	return nil
}

// operationStateFor returns app's operationState if s requested it.
func operationStateFor(s *syncv1alpha1.ApplicationSync, app *argov1alpha1.Application) *argov1alpha1.OperationState {
	state := app.Status.OperationState
	if state == nil || !isOperationFor(s, &state.Operation) {
		return nil
	}
	return state
}

// isAutomated reports whether s is an automatic run, which only a run owned by an ApplicationSyncPolicy is. Claiming
// that owner falsely only subjects the run to more restrictions, so the owner reference needs no further check.
func isAutomated(s *syncv1alpha1.ApplicationSync) bool {
	owner := metav1.GetControllerOf(s)
	return owner != nil && owner.APIVersion == syncv1alpha1.GroupVersion.String() && owner.Kind == "ApplicationSyncPolicy"
}

func isOperationFor(s *syncv1alpha1.ApplicationSync, op *argov1alpha1.Operation) bool {
	if op == nil {
		return false
	}
	for _, i := range op.Info {
		if i != nil && i.Name == syncv1alpha1.ApplicationSyncOperationUIDInfoName && i.Value == string(s.UID) {
			return true
		}
	}
	return false
}
