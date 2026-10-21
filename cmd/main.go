// Command argocd-application-sync-controller runs ApplicationSyncs and ApplicationSyncPolicies against an
// existing Argo CD installation.
package main

import (
	"flag"
	"os"
	"strings"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	syncv1alpha1 "github.com/argoproj-labs/argocd-application-sync-crd/api/v1alpha1"
	"github.com/argoproj-labs/argocd-application-sync-crd/internal/controller"
)

func main() {
	var (
		argoCDNamespace string
		namespaces      string
		historyLimit    int
		metricsAddr     string
		probeAddr       string
		leaderElect     bool
	)
	flag.StringVar(&argoCDNamespace, "argocd-namespace", "", "Namespace of the Argo CD installation, which holds AppProjects and argocd-cm. Defaults to the controller's own namespace, or the kubeconfig's when run outside a cluster.")
	flag.StringVar(&namespaces, "namespaces", "", "Comma-separated namespaces to watch for Applications and ApplicationSyncs. Empty watches all namespaces.")
	flag.IntVar(&historyLimit, "history-limit", 20, "Completed ApplicationSyncs kept per set of apps. 0 keeps them all.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the health probes bind to.")
	flag.BoolVar(&leaderElect, "leader-elect", true, "Use leader election, so only one replica reconciles at a time.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	if argoCDNamespace == "" {
		// Resolves to the pod's namespace in-cluster, and to the current context's namespace otherwise.
		ns, _, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).Namespace()
		if err != nil {
			setupLog.Error(err, "determining the controller's namespace")
			os.Exit(1)
		}
		argoCDNamespace = ns
	}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, argov1alpha1.AddToScheme, syncv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			setupLog.Error(err, "building scheme")
			os.Exit(1)
		}
	}

	// Only Argo CD's namespace is readable for AppProjects (a namespaced Role), so don't watch them cluster-wide.
	cacheOpts := cache.Options{ByObject: map[client.Object]cache.ByObject{
		&argov1alpha1.AppProject{}: {Namespaces: map[string]cache.Config{argoCDNamespace: {}}},
	}}
	if namespaces != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{argoCDNamespace: {}}
		for _, ns := range strings.Split(namespaces, ",") {
			cacheOpts.DefaultNamespaces[strings.TrimSpace(ns)] = cache.Config{}
		}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Cache:                  cacheOpts,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "argocd-application-sync-controller",
	})
	if err != nil {
		setupLog.Error(err, "creating manager")
		os.Exit(1)
	}

	if err := (&controller.ApplicationSyncReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		Recorder:        mgr.GetEventRecorder("argocd-application-sync-controller"),
		ArgoCDNamespace: argoCDNamespace,
		HistoryLimit:    historyLimit,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up ApplicationSync controller")
		os.Exit(1)
	}
	if err := (&controller.ApplicationSyncPolicyReconciler{
		Client:          mgr.GetClient(),
		APIReader:       mgr.GetAPIReader(),
		ArgoCDNamespace: argoCDNamespace,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "setting up ApplicationSyncPolicy controller")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "adding health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "adding ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "running manager")
		os.Exit(1)
	}
}
