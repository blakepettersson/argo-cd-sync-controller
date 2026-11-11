package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

// This file holds what the controller needs from Argo CD, reached only through the Kubernetes API.

const (
	argoCDConfigMap    = "argocd-cm"
	argoCDCmdParamsMap = "argocd-cmd-params-cm"
	requireOverrideKey = "application.sync.requireOverridePrivilegeForRevisionSync"
	replaceAllowedKey  = "server.sync.replace.allowed"
	// The application controller's self-heal settings, which policy self-heal follows too.
	selfHealTimeoutKey        = "controller.self.heal.timeout.seconds"
	selfHealBackoffTimeoutKey = "controller.self.heal.backoff.timeout.seconds"
	selfHealBackoffFactorKey  = "controller.self.heal.backoff.factor"
	selfHealBackoffCapKey     = "controller.self.heal.backoff.cap.seconds"
	// Defaults for those settings, matching argocd-application-controller's flag defaults.
	defaultSelfHealTimeoutSeconds        = 0
	defaultSelfHealBackoffTimeoutSeconds = 2
	defaultSelfHealBackoffFactor         = 3
	defaultSelfHealBackoffCapSeconds     = 300
	refreshAnnotation                    = "argocd.argoproj.io/refresh"
	operationStartedEvent                = "OperationStarted"
)

// errAutoSyncEnabled is why an app with Argo CD auto-sync on is never synced: both controllers would sync it.
var errAutoSyncEnabled = errors.New("auto-sync is enabled on the application; turn it off so only ApplicationSyncs sync it")

// autoSyncEnabled reports whether Argo CD's application controller auto-syncs (and self-heals) app.
func autoSyncEnabled(app *argov1alpha1.Application) bool {
	return app.Spec.SyncPolicy != nil && app.Spec.SyncPolicy.IsAutomatedSyncEnabled()
}

// errAnotherOperationInProgress mirrors the Sync API's error when an app already has an operation.
var errAnotherOperationInProgress = errors.New("another operation is already in progress")

// errApplicationReplaced means the live app is a newer one than the cached copy the checks ran against.
var errApplicationReplaced = errors.New("application was replaced")

// errOperationAlreadySet means the live app already has this run's operation, or its result, from an earlier pass.
var errOperationAlreadySet = errors.New("operation already set")

// errApplicationSyncGone is returned when the ApplicationSync was deleted, or replaced by a same-named one, since it
// was read from the cache.
var errApplicationSyncGone = errors.New("the ApplicationSync no longer exists")

// argoCD reads Argo CD's objects and settings. Settings and live reads go to the API server directly, so the
// controller doesn't cache every ConfigMap in the cluster.
type argoCD struct {
	client    client.Client
	apiReader client.Reader
	namespace string
}

func (a *argoCD) getApp(ctx context.Context, namespace, name string) (*argov1alpha1.Application, error) {
	var app argov1alpha1.Application
	if err := a.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &app); err != nil {
		return nil, err
	}
	return &app, nil
}

// getLiveApp bypasses the cache, for when it may not have caught up with a write.
func (a *argoCD) getLiveApp(ctx context.Context, namespace, name string) (*argov1alpha1.Application, error) {
	var app argov1alpha1.Application
	if err := a.apiReader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &app); err != nil {
		return nil, err
	}
	return &app, nil
}

// getProject returns app's AppProject, checking the project allows apps from app's namespace.
func (a *argoCD) getProject(ctx context.Context, app *argov1alpha1.Application) (*argov1alpha1.AppProject, error) {
	var proj argov1alpha1.AppProject
	if err := a.client.Get(ctx, types.NamespacedName{Namespace: a.namespace, Name: app.Spec.GetProject()}, &proj); err != nil {
		return nil, fmt.Errorf("getting project %q: %w", app.Spec.GetProject(), err)
	}
	if !proj.IsAppNamespacePermitted(app, a.namespace) {
		return nil, fmt.Errorf("application %q is not permitted to use project %q", app.Name, proj.Name)
	}
	return &proj, nil
}

func (a *argoCD) configMapData(ctx context.Context, configMap string) (map[string]string, error) {
	var cm corev1.ConfigMap
	err := a.apiReader.Get(ctx, types.NamespacedName{Namespace: a.namespace, Name: configMap}, &cm)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return cm.Data, err
}

func (a *argoCD) boolSetting(ctx context.Context, configMap, key string, def bool) (bool, error) {
	data, err := a.configMapData(ctx, configMap)
	if err != nil {
		return false, err
	}
	if data[key] == "" {
		return def, nil
	}
	return strconv.ParseBool(data[key])
}

// selfHealDelay returns how long to wait, after a run that finished at finishedAt, before self-heal attempt
// attempts+1. It uses the application controller's settings and defaults, so a policy self-heals at the same
// pace as auto-sync.
func (a *argoCD) selfHealDelay(ctx context.Context, finishedAt *metav1.Time, attempts int) (time.Duration, error) {
	data, err := a.configMapData(ctx, argoCDCmdParamsMap)
	if err != nil {
		return 0, err
	}
	seconds := func(key string, def int) (int, error) {
		if data[key] == "" {
			return def, nil
		}
		return strconv.Atoi(data[key])
	}
	var errs [4]error
	var timeout, backoffTimeout, factor, backoffCap int
	timeout, errs[0] = seconds(selfHealTimeoutKey, defaultSelfHealTimeoutSeconds)
	backoffTimeout, errs[1] = seconds(selfHealBackoffTimeoutKey, defaultSelfHealBackoffTimeoutSeconds)
	factor, errs[2] = seconds(selfHealBackoffFactorKey, defaultSelfHealBackoffFactor)
	backoffCap, errs[3] = seconds(selfHealBackoffCapKey, defaultSelfHealBackoffCapSeconds)
	if err := errors.Join(errs[:]...); err != nil {
		return 0, fmt.Errorf("reading self-heal settings from %s: %w", argoCDCmdParamsMap, err)
	}
	// As in the application controller, a backoff timeout of 0 falls back to the fixed timeout.
	delay := time.Duration(timeout) * time.Second
	if backoffTimeout != 0 {
		backoff := wait.Backoff{Duration: time.Duration(backoffTimeout) * time.Second, Factor: float64(factor), Cap: time.Duration(backoffCap) * time.Second, Steps: attempts}
		delay = 0
		for range attempts {
			delay = backoff.Step()
		}
	}
	if finishedAt != nil {
		delay -= time.Since(finishedAt.Time)
	}
	return delay, nil
}

// requireOverride reports application.sync.requireOverridePrivilegeForRevisionSync from argocd-cm.
func (a *argoCD) requireOverride(ctx context.Context) (bool, error) {
	return a.boolSetting(ctx, argoCDConfigMap, requireOverrideKey, false)
}

// replaceAllowed reports server.sync.replace.allowed from argocd-cmd-params-cm, which the Sync API honours.
func (a *argoCD) replaceAllowed(ctx context.Context) (bool, error) {
	return a.boolSetting(ctx, argoCDCmdParamsMap, replaceAllowedKey, true)
}

// setOperation writes s's op on the live app, as the Sync API does: it fails if an operation is already set, and
// clears the previous operationState in the same update. It also fails if the live app isn't the one with uid that
// the caller checked. If the live app already has s's operation, or its result, from a pass whose status update
// failed, it returns errOperationAlreadySet.
func (a *argoCD) setOperation(ctx context.Context, s *syncv1alpha1.ApplicationSync, namespace, name string, uid types.UID, op *argov1alpha1.Operation) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		app, err := a.getLiveApp(ctx, namespace, name)
		if err != nil {
			return err
		}
		if app.UID != uid {
			return errApplicationReplaced
		}
		if isOperationFor(s, app.Operation) || operationStateFor(s, app) != nil {
			return errOperationAlreadySet
		}
		if app.Operation != nil {
			return errAnotherOperationInProgress
		}
		app.Operation = op
		app.Status.OperationState = nil
		return a.client.Update(ctx, app)
	})
}

// requestRefresh asks the application controller to refresh app, the way the CLI and ApplicationSets do.
func (a *argoCD) requestRefresh(ctx context.Context, app *argov1alpha1.Application) error {
	if app.Annotations[refreshAnnotation] != "" {
		return nil
	}
	patch := client.MergeFrom(app.DeepCopy())
	updated := app.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = map[string]string{}
	}
	updated.Annotations[refreshAnnotation] = string(argov1alpha1.RefreshTypeNormal)
	return a.client.Patch(ctx, updated, patch)
}

// comparedToCurrentSpec reports whether the app's last comparison, recorded in status.sync.comparedTo, was
// made against its current sources, destination, ignoreDifferences and managed namespace metadata.
func comparedToCurrentSpec(app *argov1alpha1.Application) bool {
	var sourcesEqual bool
	if app.Spec.HasMultipleSources() {
		sourcesEqual = app.Spec.Sources.Equals(app.Status.Sync.ComparedTo.Sources)
	} else {
		source := app.Spec.GetSource()
		sourcesEqual = source.Equals(&app.Status.Sync.ComparedTo.Source)
	}
	return sourcesEqual &&
		app.Spec.Destination == app.Status.Sync.ComparedTo.Destination &&
		app.Spec.IgnoreDifferences.Equals(app.Status.Sync.ComparedTo.IgnoreDifferences) &&
		!app.HasChangedManagedNamespaceMetadata()
}
