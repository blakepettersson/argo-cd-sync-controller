package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/pkg/health"
	synccommon "github.com/argoproj/argo-cd/gitops-engine/pkg/sync/common"
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	syncv1alpha1 "github.com/argoproj-labs/argocd-application-sync-crd/api/v1alpha1"
)

const (
	// appIndex indexes ApplicationSyncs by the name of each app they list.
	appIndex = "spec.groups.apps.name"
	// resyncPeriod is a safety net; app changes wake an ApplicationSync straight away.
	resyncPeriod = 30 * time.Second
)

// ApplicationSyncReconciler runs ApplicationSyncs: it sets each app's operation in group order and reads the
// result back from the app's status.
type ApplicationSyncReconciler struct {
	client.Client
	APIReader client.Reader
	Recorder  events.EventRecorder
	// ArgoCDNamespace holds AppProjects and Argo CD's ConfigMaps.
	ArgoCDNamespace string
	// HistoryLimit caps completed ApplicationSyncs kept per set of apps; 0 keeps them all.
	HistoryLimit int
}

// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncs,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=argoproj.io,resources=applicationsyncs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=argoproj.io,resources=applications,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=argoproj.io,resources=appprojects,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get
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
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: s.Namespace, Name: s.Name}})
		}
	}
	return reqs
}

func (r *ApplicationSyncReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var s syncv1alpha1.ApplicationSync
	if err := r.Get(ctx, req.NamespacedName, &s); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if s.Status.Phase.Completed() {
		return ctrl.Result{}, nil
	}
	orig := s.Status.DeepCopy()
	r.reconcileApplicationSync(ctx, &s)
	if !equality.Semantic.DeepEqual(*orig, s.Status) {
		if err := r.Status().Update(ctx, &s); err != nil {
			return ctrl.Result{}, err
		}
	}
	if s.Status.Phase.Completed() {
		r.gc(ctx, &s)
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: resyncPeriod}, nil
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
	if s.Spec.Paused {
		s.Status.Message = "paused"
	}

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
		r.reconcileGroup(ctx, s, s.Spec.Groups[i], statuses)
	}

	failed, total := 0, 0
	for _, gs := range s.Status.Groups {
		if !gs.Phase.Completed() {
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

func (r *ApplicationSyncReconciler) reconcileGroup(ctx context.Context, s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncGroup, statuses map[string]*syncv1alpha1.ApplicationSyncGroupStatus) {
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
	limit, paused := groupLimit(spec.MaxParallel, len(spec.Apps))
	gs.Message = ""
	for i := range gs.Apps {
		st := &gs.Apps[i]
		if st.Phase != syncv1alpha1.ApplicationSyncPhasePending {
			continue
		}
		switch {
		case s.Spec.Paused:
			st.Message = "paused"
			continue
		case paused:
			st.Message = "paused by maxParallel"
			gs.Message = st.Message
			continue
		case limit >= 0 && inFlight >= limit:
			st.Message = fmt.Sprintf("waiting for one of %d parallel slots", limit)
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

// groupLimit returns how many apps of a group may be in flight at once (-1 for no limit), and whether
// maxParallel pauses the group. It rounds like RollingSync's maxUpdate.
func groupLimit(maxParallel *intstr.IntOrString, total int) (int, bool) {
	if maxParallel == nil {
		return -1, false
	}
	limit, err := intstr.GetScaledValueFromIntOrPercent(maxParallel, total, false)
	if err != nil {
		return -1, false
	}
	if maxParallel.Type == intstr.String && maxParallel.StrVal != "0%" && limit < 1 {
		limit = 1
	}
	return limit, limit == 0
}

func (r *ApplicationSyncReconciler) reconcileApp(ctx context.Context, s *syncv1alpha1.ApplicationSync, spec syncv1alpha1.ApplicationSyncApp, st *syncv1alpha1.ApplicationSyncAppStatus) {
	wait := s.Spec.OnFailure == syncv1alpha1.ApplicationSyncOnFailureWait
	// fail ends the app in Fail mode. In Wait mode the app keeps waiting for a retry or a manual fix.
	fail := func(msg string) {
		if wait {
			st.Message = msg + "; waiting for the application to recover"
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
		st.Message = msg
	}
	argo := r.argoCD()

	switch st.Phase {
	case syncv1alpha1.ApplicationSyncPhasePending:
		app, err := argo.getApp(ctx, s.Namespace, st.Name)
		if err != nil {
			st.Message = "waiting for application to exist"
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
				st.Message = "waiting for application to be refreshed"
				return
			}
			if app.Status.Sync.Status == argov1alpha1.SyncStatusCodeSynced && revisionsMatch(st.PinnedRevisions, app.Status.GetRevisions()) {
				st.Phase = syncv1alpha1.ApplicationSyncPhaseProgressing
				st.Revisions = app.Status.GetRevisions()
				st.Message = "already synced"
				return
			}
		}
		err = r.startOperation(ctx, s, spec, st, app)
		if errors.Is(err, errAnotherOperationInProgress) {
			st.Message = "waiting for another operation to finish"
			return
		}
		if err != nil {
			fail(err.Error())
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseRunning
		st.Message = ""

	case syncv1alpha1.ApplicationSyncPhaseRunning:
		app, err := argo.getApp(ctx, s.Namespace, st.Name)
		if err != nil {
			st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
			st.Message = "application was deleted"
			return
		}
		state := operationStateFor(s, app)
		if state == nil && !isOperationFor(s, app.Operation) {
			// The cache may not have caught up with the operation this controller just set.
			live, err := argo.getLiveApp(ctx, app.Namespace, app.Name)
			if apierrors.IsNotFound(err) {
				st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
				st.Message = "application was deleted"
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
		if err != nil {
			st.Phase = syncv1alpha1.ApplicationSyncPhaseFailed
			st.Message = "application was deleted"
			return
		}
		since := st.FinishedAt
		if since == nil {
			since = s.Status.StartedAt
		}
		if !r.isFresh(ctx, app, since) {
			st.Message = "waiting for application to be refreshed"
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
			(len(st.Revisions) == 0 || revisionsMatch(st.Revisions, app.Status.GetRevisions())) {
			// OutOfSync only blocks while the comparison is still against what was synced. If targetRevision
			// moved on, whoever created the ApplicationSync decides whether to start another run.
			st.Message = fmt.Sprintf("waiting for application to become Synced (currently %s)", app.Status.Sync.Status)
			return
		}
		st.Phase = syncv1alpha1.ApplicationSyncPhaseSucceeded
		st.Message = ""
	}
}

// isFresh reports whether app's status was computed after since and against its current spec, and asks for a
// refresh if not. A refresh that read an older copy of the spec records that spec in comparedTo, so it fails
// the check whatever its timing.
func (r *ApplicationSyncReconciler) isFresh(ctx context.Context, app *argov1alpha1.Application, since *metav1.Time) bool {
	fresh := app.Status.ReconciledAt != nil && (since == nil || !app.Status.ReconciledAt.Before(since)) && comparedToCurrentSpec(app)
	if !fresh {
		if err := r.argoCD().requestRefresh(ctx, app); err != nil {
			log.FromContext(ctx).Error(err, "requesting refresh", "application", app.Name)
		}
	}
	return fresh
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

// windowBlocks returns a message if a sync window stops app from syncing right now.
func (r *ApplicationSyncReconciler) windowBlocks(ctx context.Context, s *syncv1alpha1.ApplicationSync, app *argov1alpha1.Application) (string, error) {
	proj, err := r.argoCD().getProject(ctx, app)
	if err != nil {
		return "", err
	}
	canSync, err := proj.Spec.SyncWindows.Matches(app).CanSync(!s.Spec.InitiatedBy.Automated, nil)
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
		InitiatedBy: s.Spec.InitiatedBy,
		Info:        append([]*argov1alpha1.Info{{Name: syncv1alpha1.ApplicationSyncOperationInfoName, Value: s.Name}}, s.Spec.Info...),
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
	if err := argo.setOperation(ctx, app.Namespace, app.Name, &op); err != nil {
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
	// StartedAt guards against a same-named ApplicationSync that was deleted and recreated.
	if state == nil || !isOperationFor(s, &state.Operation) || state.StartedAt.Before(&s.CreationTimestamp) {
		return nil
	}
	return state
}

func isOperationFor(s *syncv1alpha1.ApplicationSync, op *argov1alpha1.Operation) bool {
	if op == nil {
		return false
	}
	for _, i := range op.Info {
		if i != nil && i.Name == syncv1alpha1.ApplicationSyncOperationInfoName && i.Value == s.Name {
			return true
		}
	}
	return false
}
