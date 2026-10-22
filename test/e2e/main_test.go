// Package e2e runs the controller binary against the current kubeconfig context, in a namespace of its own. Argo CD's
// application controller doesn't watch that namespace, so tests play its part by writing Application status.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

const namespace = "argo-cd-sync-controller-e2e"

var k8s client.Client

func TestMain(m *testing.M) {
	// It deletes and recreates a namespace in the current kube context, so it only runs when asked to.
	if os.Getenv("E2E") != "true" {
		fmt.Println("skipping e2e tests; run make test-e2e-local, or set E2E=true")
		os.Exit(0)
	}
	if err := setUp(); err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		os.Exit(1)
	}
	controller, err := startController()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting controller:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = controller.Process.Kill()
	_ = controller.Wait()
	_ = k8s.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}})
	os.Exit(code)
}

// setUp recreates the test namespace with a default AppProject, since the controller reads sync windows from it.
func setUp() error {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, argov1alpha1.AddToScheme, syncv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			return err
		}
	}
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		return err
	}
	ctx := context.Background()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := client.IgnoreNotFound(k8s.Delete(ctx, ns)); err != nil {
		return err
	}
	err = wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		err := k8s.Get(ctx, client.ObjectKeyFromObject(ns), &corev1.Namespace{})
		return apierrors.IsNotFound(err), client.IgnoreNotFound(err)
	})
	if err != nil {
		return fmt.Errorf("waiting for namespace %s to be deleted: %w", namespace, err)
	}
	if err := k8s.Create(ctx, ns); err != nil {
		return err
	}
	return k8s.Create(ctx, &argov1alpha1.AppProject{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace},
		Spec: argov1alpha1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argov1alpha1.ApplicationDestination{{Server: "*", Namespace: "*"}},
		},
	})
}

// startController runs the binary make build-local produces, or E2E_CONTROLLER, logging to dist/e2e-controller.log.
func startController() (*exec.Cmd, error) {
	bin := os.Getenv("E2E_CONTROLLER")
	if bin == "" {
		bin = filepath.Join("..", "..", "dist", "argo-cd-sync-controller")
	}
	logFile, err := os.Create(filepath.Join("..", "..", "dist", "e2e-controller.log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(context.Background(), bin, "--leader-elect=false", "--argocd-namespace="+namespace, "--namespaces="+namespace,
		"--metrics-bind-address=0", "--health-probe-bind-address=0", "--zap-devel")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}
