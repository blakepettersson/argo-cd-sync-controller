// Package e2e runs the controller binary against a throwaway k3s cluster in a container, with Argo CD's CRDs and ours
// installed. Argo CD itself doesn't run there, so tests play its application controller by writing Application status.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

const (
	namespace = "argo-cd-sync-controller-e2e"
	// k3sImage matches the Kubernetes minor version of the client libraries Argo CD pins.
	k3sImage = "rancher/k3s:v1.37.1-k3s1"
)

var k8s client.Client

func TestMain(m *testing.M) {
	// It needs Docker and takes a while to start a cluster, so it only runs when asked to.
	if os.Getenv("E2E") != "true" {
		fmt.Println("skipping e2e tests; run make test-e2e-local, or set E2E=true")
		os.Exit(0)
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// The images are public; without this, testcontainers asks credential helpers for every registry in
	// ~/.docker/config.json, which can block on a keychain prompt.
	if _, ok := os.LookupEnv("DOCKER_AUTH_CONFIG"); !ok {
		_ = os.Setenv("DOCKER_AUTH_CONFIG", "{}")
	}
	ctx := context.Background()
	cluster, err := k3s.Run(ctx, k3sImage)
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			fmt.Fprintln(os.Stderr, "terminating k3s:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting k3s:", err)
		return 1
	}
	kubeconfig, err := setUp(ctx, cluster)
	defer os.Remove(kubeconfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		return 1
	}
	controller, err := startController(kubeconfig)
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting controller:", err)
		return 1
	}
	defer func() {
		_ = controller.Process.Kill()
		_ = controller.Wait()
	}()
	return m.Run()
}

// setUp installs the CRDs, and creates the test namespace with a default AppProject, since the controller reads sync
// windows from it. It returns the path of the cluster's kubeconfig, for the controller.
func setUp(ctx context.Context, cluster *k3s.K3sContainer) (string, error) {
	raw, err := cluster.GetKubeConfig(ctx)
	if err != nil {
		return "", err
	}
	kubeconfig := filepath.Join(os.TempDir(), fmt.Sprintf("argo-cd-sync-controller-e2e-%d.kubeconfig", os.Getpid()))
	if err := os.WriteFile(kubeconfig, raw, 0o600); err != nil {
		return "", err
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return "", err
	}
	argoCD, err := argoCDDir()
	if err != nil {
		return "", err
	}
	_, err = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths:              []string{filepath.Join(argoCD, "manifests", "crds"), filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfPathMissing: true,
		MaxTime:            time.Minute,
	})
	if err != nil {
		return "", fmt.Errorf("installing CRDs: %w", err)
	}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, argov1alpha1.AddToScheme, syncv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			return "", err
		}
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		return "", err
	}
	if err := k8s.Create(ctx, &corev1.Namespace{Name: namespace}); err != nil {
		return "", err
	}
	return kubeconfig, k8s.Create(ctx, &argov1alpha1.AppProject{
		Name: "default", Namespace: namespace,
		Spec: argov1alpha1.AppProjectSpec{
			SourceRepos:  []string{"*"},
			Destinations: []argov1alpha1.ApplicationDestination{{Server: "*", Namespace: "*"}},
		},
	})
}

// argoCDDir is the source directory of the Argo CD module in go.mod, whose manifests/crds has its CRDs.
func argoCDDir() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}", "github.com/argoproj/argo-cd/v3").Output()
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return "", fmt.Errorf("locating Argo CD module: %w: %s", err, exitErr.Stderr)
	}
	return strings.TrimSpace(string(out)), err
}

// startController builds and runs the controller, or runs E2E_CONTROLLER, logging to dist/e2e-controller.log.
func startController(kubeconfig string) (*exec.Cmd, error) {
	dist := filepath.Join("..", "..", "dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		return nil, err
	}
	bin := os.Getenv("E2E_CONTROLLER")
	if bin == "" {
		bin = filepath.Join(dist, "argo-cd-sync-controller-e2e")
		build := exec.CommandContext(context.Background(), "go", "build", "-race", "-o", bin, "../../cmd")
		if out, err := build.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("building controller: %w: %s", err, out)
		}
	}
	logFile, err := os.Create(filepath.Join(dist, "e2e-controller.log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(context.Background(), bin, "--leader-elect=false", "--argocd-namespace="+namespace, "--namespaces="+namespace,
		"--metrics-bind-address=0", "--health-probe-bind-address=0", "--zap-devel")
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}
