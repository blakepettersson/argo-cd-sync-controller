// Package v1alpha1 contains the ApplicationSync and ApplicationSyncPolicy API.
// +kubebuilder:object:generate=true
// +groupName=argoproj.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is the group and version of these objects. They share argoproj.io/v1alpha1 with Argo CD's
	// own kinds, so moving them into Argo CD later needs no CRD rename.
	GroupVersion = schema.GroupVersion{Group: "argoproj.io", Version: "v1alpha1"}

	// SchemeBuilder registers the types with a scheme.
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(GroupVersion, &ApplicationSync{}, &ApplicationSyncList{}, &ApplicationSyncPolicy{}, &ApplicationSyncPolicyList{})
	metav1.AddToGroupVersion(scheme, GroupVersion)
	return nil
}
