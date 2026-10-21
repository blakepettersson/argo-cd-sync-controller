package v1alpha1

import (
	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// ApplicationSyncList is a list of ApplicationSync resources
// +kubebuilder:object:root=true
type ApplicationSyncList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ApplicationSync `json:"items"`
}

// ApplicationSync requests a one-off sync of groups of Applications in its namespace, ordered by group dependsOn.
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=applicationsyncs,shortName=appsync;appsyncs
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Message",type=string,JSONPath=`.status.message`,priority=10
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ApplicationSync struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	// Every field except paused is immutable. CEL can't compare a struct minus one field, so each is listed.
	// +kubebuilder:validation:XValidation:rule="self.groups == oldSelf.groups && has(self.prune) == has(oldSelf.prune) && (!has(self.prune) || self.prune == oldSelf.prune) && has(self.dryRun) == has(oldSelf.dryRun) && (!has(self.dryRun) || self.dryRun == oldSelf.dryRun) && has(self.syncOptions) == has(oldSelf.syncOptions) && (!has(self.syncOptions) || self.syncOptions == oldSelf.syncOptions) && has(self.syncStrategy) == has(oldSelf.syncStrategy) && (!has(self.syncStrategy) || self.syncStrategy == oldSelf.syncStrategy) && has(self.retry) == has(oldSelf.retry) && (!has(self.retry) || self.retry == oldSelf.retry) && has(self.initiatedBy) == has(oldSelf.initiatedBy) && (!has(self.initiatedBy) || self.initiatedBy == oldSelf.initiatedBy) && has(self.info) == has(oldSelf.info) && (!has(self.info) || self.info == oldSelf.info) && has(self.onFailure) == has(oldSelf.onFailure) && (!has(self.onFailure) || self.onFailure == oldSelf.onFailure) && has(self.doneWhen) == has(oldSelf.doneWhen) && (!has(self.doneWhen) || self.doneWhen == oldSelf.doneWhen) && has(self.skipIfSynced) == has(oldSelf.skipIfSynced) && (!has(self.skipIfSynced) || self.skipIfSynced == oldSelf.skipIfSynced)",message="spec is immutable, except spec.paused"
	Spec   ApplicationSyncSpec   `json:"spec"`
	Status ApplicationSyncStatus `json:"status,omitempty"`
}

// ApplicationSyncSpec lists the Applications to sync, in groups, and the options applied to each sync operation.
type ApplicationSyncSpec struct {
	// Groups of Applications to sync. A group starts once the groups it depends on have succeeded, and its
	// apps sync in parallel. The apps must be in the same namespace as the ApplicationSync.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MinItems=1
	Groups []ApplicationSyncGroup `json:"groups"`
	// Prune specifies to delete resources from the cluster that are no longer tracked in git
	Prune bool `json:"prune,omitempty"`
	// DryRun specifies to perform a `kubectl apply --dry-run` without actually performing the sync
	DryRun bool `json:"dryRun,omitempty"`
	// SyncOptions override each Application's spec.syncPolicy.syncOptions when set
	SyncOptions argov1alpha1.SyncOptions `json:"syncOptions,omitempty"`
	// SyncStrategy describes how to perform the sync
	SyncStrategy *argov1alpha1.SyncStrategy `json:"syncStrategy,omitempty"`
	// Retry overrides each Application's spec.syncPolicy.retry when set
	Retry *argov1alpha1.RetryStrategy `json:"retry,omitempty"`
	// InitiatedBy is copied into each app's operation. It is informational only and never used for authorization.
	// When Automated is true, sync windows are checked with automatic semantics.
	InitiatedBy argov1alpha1.OperationInitiator `json:"initiatedBy,omitempty"`
	// Info is added to each app's operation
	Info []*argov1alpha1.Info `json:"info,omitempty"`
	// OnFailure decides what happens when an app fails: Fail (default) fails it and skips its dependents, Wait keeps
	// it Progressing until it is done
	// +kubebuilder:validation:Enum=Fail;Wait
	OnFailure ApplicationSyncOnFailure `json:"onFailure,omitempty"`
	// DoneWhen decides when an app counts as done: SyncedAndHealthy (default) or Healthy
	// +kubebuilder:validation:Enum=SyncedAndHealthy;Healthy
	DoneWhen ApplicationSyncDoneWhen `json:"doneWhen,omitempty"`
	// SkipIfSynced counts an app that is already Synced at its revisions as synced, without starting a sync
	SkipIfSynced bool `json:"skipIfSynced,omitempty"`
	// Paused stops new app syncs from starting; syncs already running finish. It is the only field that can
	// change after creation.
	Paused bool `json:"paused,omitempty"`
}

// ApplicationSyncOnFailure is what an ApplicationSync does when an app fails
type ApplicationSyncOnFailure string

const (
	ApplicationSyncOnFailureFail ApplicationSyncOnFailure = "Fail"
	ApplicationSyncOnFailureWait ApplicationSyncOnFailure = "Wait"
)

// ApplicationSyncDoneWhen is the condition under which an app in an ApplicationSync counts as done
type ApplicationSyncDoneWhen string

const (
	ApplicationSyncDoneWhenSyncedAndHealthy ApplicationSyncDoneWhen = "SyncedAndHealthy"
	ApplicationSyncDoneWhenHealthy          ApplicationSyncDoneWhen = "Healthy"
)

// ApplicationSyncGroup is a set of Applications synced in parallel, after the groups it depends on.
type ApplicationSyncGroup struct {
	// Name of the group, unique within the ApplicationSync. Defaults to "default", so it can be left out when there
	// is only one group; two unnamed groups are rejected as duplicates.
	// +kubebuilder:default=default
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name,omitempty"`
	// DependsOn names other groups whose apps must all be synced and Healthy before this group starts
	// +listType=set
	DependsOn []string `json:"dependsOn,omitempty"`
	// Apps to sync in this group. A group with no apps counts as complete.
	// +listType=map
	// +listMapKey=name
	Apps []ApplicationSyncApp `json:"apps,omitempty"`
	// MaxParallel caps how many apps in the group sync at once, as a number or a percentage of the group.
	// A percentage that rounds down to 0 becomes 1, except 0%. 0 or 0% pauses the group.
	MaxParallel *intstr.IntOrString `json:"maxParallel,omitempty"`
}

// ApplicationSyncApp is an Application to sync.
type ApplicationSyncApp struct {
	// Name of the Application
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// Revisions pins sources of the app. A source that isn't listed syncs to its targetRevision as resolved when
	// the app's sync starts.
	Revisions []ApplicationSyncRevision `json:"revisions,omitempty"`
	// Prune overrides spec.prune for this app
	Prune *bool `json:"prune,omitempty"`
	// Resources limits the sync to these resources, as in the Sync API
	Resources []argov1alpha1.SyncOperationResource `json:"resources,omitempty"`
	// Retry overrides spec.retry and the app's own syncPolicy.retry for this app
	Retry *argov1alpha1.RetryStrategy `json:"retry,omitempty"`
}

// ApplicationSyncRevision pins one source of an app. Single-source apps set neither Source nor Position.
// +kubebuilder:validation:XValidation:rule="!(has(self.source) && has(self.position))",message="set source or position, not both"
type ApplicationSyncRevision struct {
	// Source is the name of the source in spec.sources
	Source string `json:"source,omitempty"`
	// Position is the 1-based index of the source in spec.sources
	// +kubebuilder:validation:Minimum=1
	Position int32 `json:"position,omitempty"`
	// Revision to sync the source to. It is passed to the sync as given, so use a commit SHA, an exact chart
	// version or a digest to be sure every app in the run gets the same one.
	// +kubebuilder:validation:MinLength=1
	Revision string `json:"revision"`
}

// ApplicationSyncOperationInfoName tags each Operation an ApplicationSync starts with an Info entry whose value
// is the ApplicationSync's name, so the result can be matched back from status.operationState.
const ApplicationSyncOperationInfoName = "ApplicationSync"

// ApplicationSyncPhase is the phase of an ApplicationSync, or of one of its groups or Applications
type ApplicationSyncPhase string

const (
	ApplicationSyncPhasePending ApplicationSyncPhase = "Pending"
	ApplicationSyncPhaseRunning ApplicationSyncPhase = "Running"
	// ApplicationSyncPhaseProgressing is only used per app: the sync succeeded and the app is not yet Healthy.
	ApplicationSyncPhaseProgressing ApplicationSyncPhase = "Progressing"
	ApplicationSyncPhaseSucceeded   ApplicationSyncPhase = "Succeeded"
	ApplicationSyncPhaseFailed      ApplicationSyncPhase = "Failed"
	// ApplicationSyncPhaseSkipped is only used per group and per app: a group it depends on did not succeed.
	ApplicationSyncPhaseSkipped ApplicationSyncPhase = "Skipped"
)

// Completed returns true if the phase is terminal
func (p ApplicationSyncPhase) Completed() bool {
	return p == ApplicationSyncPhaseSucceeded || p == ApplicationSyncPhaseFailed || p == ApplicationSyncPhaseSkipped
}

// ApplicationSyncStatus is the observed progress of an ApplicationSync
type ApplicationSyncStatus struct {
	Phase   ApplicationSyncPhase `json:"phase,omitempty"`
	Message string               `json:"message,omitempty"`
	// StartedAt is when the controller accepted the ApplicationSync
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// FinishedAt is when every group reached a terminal phase
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
	// Groups holds per-group and per-app progress, in spec.groups order
	Groups []ApplicationSyncGroupStatus `json:"groups,omitempty"`
}

// ApplicationSyncGroupStatus is the progress of one group within an ApplicationSync
type ApplicationSyncGroupStatus struct {
	Name    string               `json:"name"`
	Phase   ApplicationSyncPhase `json:"phase"`
	Message string               `json:"message,omitempty"`
	// Apps holds per-app progress, in the group's apps order
	Apps []ApplicationSyncAppStatus `json:"apps,omitempty"`
}

// ApplicationSyncAppStatus is the progress of one Application within an ApplicationSync
type ApplicationSyncAppStatus struct {
	Name    string               `json:"name"`
	Phase   ApplicationSyncPhase `json:"phase"`
	Message string               `json:"message,omitempty"`
	// Revisions the sync operation resolved to, one per source
	Revisions []string `json:"revisions,omitempty"`
	// PinnedRevisions are the spec revisions matched to the app's sources, one per source in spec.sources order,
	// empty for sources that aren't pinned
	PinnedRevisions []string `json:"pinnedRevisions,omitempty"`
	// StartedAt and FinishedAt mirror the Application's operationState for this sync
	StartedAt  *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
}

// ApplicationSyncPolicyList is a list of ApplicationSyncPolicy resources
// +kubebuilder:object:root=true
type ApplicationSyncPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []ApplicationSyncPolicy `json:"items"`
}

// ApplicationSyncPolicy creates an ApplicationSync from its template whenever one of its apps is OutOfSync.
// In effect it is auto-sync for a group of apps.
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=applicationsyncpolicies,shortName=appsyncpolicy;appsyncpolicies
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Current",type=string,JSONPath=`.status.currentRun`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ApplicationSyncPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              ApplicationSyncPolicySpec   `json:"spec"`
	Status            ApplicationSyncPolicyStatus `json:"status,omitempty"`
}

// ApplicationSyncPolicySpec is the template for the ApplicationSyncs a policy creates
type ApplicationSyncPolicySpec struct {
	// ConcurrencyPolicy decides what happens when apps drift while a run is unfinished: Forbid (default) waits for
	// it to finish, Replace deletes it and starts a new one
	// +kubebuilder:validation:Enum=Forbid;Replace
	ConcurrencyPolicy ApplicationSyncConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`
	// Template is the spec of each ApplicationSync the policy creates
	Template ApplicationSyncSpec `json:"template"`
}

// ApplicationSyncConcurrencyPolicy is what a policy does when its apps drift during a run
type ApplicationSyncConcurrencyPolicy string

const (
	ApplicationSyncConcurrencyForbid  ApplicationSyncConcurrencyPolicy = "Forbid"
	ApplicationSyncConcurrencyReplace ApplicationSyncConcurrencyPolicy = "Replace"
)

// ApplicationSyncPolicyStatus is the observed state of an ApplicationSyncPolicy
type ApplicationSyncPolicyStatus struct {
	// CurrentRun is the name of the ApplicationSync the policy created last
	CurrentRun string `json:"currentRun,omitempty"`
	Message    string `json:"message,omitempty"`
}
