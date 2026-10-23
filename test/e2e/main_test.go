// Package e2e runs the controller binary against a throwaway k3s cluster in a container, with our CRDs and the Argo CD
// release in go.mod installed. Argo CD's application controller and repo server run in argoCDNamespace and sync apps
// from a git repository this process serves. Tests in namespace, which Argo CD doesn't watch, play its application
// controller instead, by writing Application status, to reach states a real sync can't produce on demand.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	syncv1alpha1 "github.com/argoproj-labs/argo-cd-sync-controller/api/v1alpha1"
)

const (
	namespace = "argo-cd-sync-controller-e2e"
	// argoCDNamespace runs a real Argo CD, and its own instance of the controller.
	argoCDNamespace = "argocd"
	// targetNamespace is where Argo CD deploys the resources of the apps it syncs.
	targetNamespace = "argo-cd-sync-controller-e2e-target"
	// k3sImage matches the Kubernetes minor version of the client libraries Argo CD pins.
	k3sImage = "rancher/k3s:v1.37.1-k3s1"
)

var (
	k8s  client.Client
	repo *gitRepo
)

// TestMain starts the cluster, Argo CD and the controllers once for the package. It needs Docker and takes a few
// minutes, so make test-local leaves this package out, as argo-cd's does; make test-e2e-local runs it.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// The images are public; without this, testcontainers asks credential helpers for every registry in
	// ~/.docker/config.json, which can block on a keychain prompt.
	if _, ok := os.LookupEnv("DOCKER_AUTH_CONFIG"); !ok {
		_ = os.Setenv("DOCKER_AUTH_CONFIG", "{}")
	}
	ctx := context.Background()
	var err error
	if repo, err = startGitServer(); err != nil {
		fmt.Fprintln(os.Stderr, "starting git server:", err)
		return 1
	}
	defer repo.close()
	cluster, err := k3s.Run(ctx, k3sImage, testcontainers.WithHostPortAccess(repo.port))
	defer func() {
		if err := testcontainers.TerminateContainer(cluster); err != nil {
			fmt.Fprintln(os.Stderr, "terminating k3s:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting k3s:", err)
		return 1
	}
	kubeconfig := filepath.Join(os.TempDir(), fmt.Sprintf("argo-cd-sync-controller-e2e-%d.kubeconfig", os.Getpid()))
	defer os.Remove(kubeconfig)
	if err := setUp(ctx, cluster, kubeconfig); err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		return 1
	}
	bin, err := buildController()
	if err != nil {
		fmt.Fprintln(os.Stderr, "building controller:", err)
		return 1
	}
	for _, ns := range []string{namespace, argoCDNamespace} {
		controller, err := startController(bin, kubeconfig, ns)
		if err != nil {
			fmt.Fprintln(os.Stderr, "starting controller:", err)
			return 1
		}
		defer func() {
			_ = controller.Process.Kill()
			_ = controller.Wait()
		}()
	}
	return m.Run()
}

// setUp writes the cluster's kubeconfig to kubeconfig, for the controllers, installs Argo CD and our CRDs, and creates
// the namespaces with a default AppProject, since the controller reads sync windows from it.
func setUp(ctx context.Context, cluster *k3s.K3sContainer, kubeconfig string) error {
	raw, err := cluster.GetKubeConfig(ctx)
	if err != nil {
		return err
	}
	if err := os.WriteFile(kubeconfig, raw, 0o600); err != nil {
		return err
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return err
	}
	if err := installArgoCD(ctx, cluster); err != nil {
		return fmt.Errorf("installing Argo CD: %w", err)
	}
	if err := repo.resolveURL(ctx, cluster); err != nil {
		return err
	}
	_, err = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths:              []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfPathMissing: true,
		MaxTime:            time.Minute,
	})
	if err != nil {
		return fmt.Errorf("installing CRDs: %w", err)
	}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, argov1alpha1.AddToScheme, syncv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			return err
		}
	}
	if k8s, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		return err
	}
	for _, ns := range []string{namespace, targetNamespace} {
		if err := k8s.Create(ctx, &corev1.Namespace{Name: ns}); err != nil {
			return err
		}
	}
	for _, ns := range []string{namespace, argoCDNamespace} {
		err := k8s.Create(ctx, &argov1alpha1.AppProject{
			Name: "default", Namespace: ns,
			Spec: argov1alpha1.AppProjectSpec{
				SourceRepos:  []string{"*"},
				Destinations: []argov1alpha1.ApplicationDestination{{Server: "*", Namespace: "*"}},
			},
		})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

// installArgoCD runs the application controller and repo server of the Argo CD release in go.mod, from its
// core-install.yaml. The images are pulled on the host and loaded into k3s, so a rerun doesn't download them again.
func installArgoCD(ctx context.Context, cluster *k3s.K3sContainer) error {
	dir, err := argoCDDir()
	if err != nil {
		return err
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "manifests", "core-install.yaml"))
	if err != nil {
		return err
	}
	// ECR Public mirrors Docker Hub's official images, but limits anonymous downloads per IP, which shared CI runners
	// exceed. Docker Hub has the same images, by digest.
	manifest = bytes.ReplaceAll(manifest, []byte("public.ecr.aws/docker/library/"), []byte("docker.io/library/"))
	var images []string
	for _, m := range regexp.MustCompile(`(?m)^\s+image: (\S+)$`).FindAllStringSubmatch(string(manifest), -1) {
		if !slices.Contains(images, m[1]) {
			images = append(images, m[1])
		}
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return err
	}
	defer provider.Close()
	for _, image := range images {
		if err := provider.PullImage(ctx, image); err != nil {
			return err
		}
		// PullImage returns nil when the pull fails partway through, reporting the error in its stream:
		// https://github.com/testcontainers/testcontainers-go/issues/3945
		if _, err := provider.Client().ImageInspect(ctx, image); err != nil {
			return fmt.Errorf("pulling %s: %w", image, err)
		}
	}
	if err := cluster.LoadImages(ctx, images...); err != nil {
		return fmt.Errorf("loading images: %w", err)
	}
	if err := cluster.CopyToContainer(ctx, manifest, "/tmp/core-install.yaml", 0o644); err != nil {
		return err
	}
	for _, args := range [][]string{
		{"create", "namespace", argoCDNamespace},
		// Applying the install keeps this data, which it doesn't set. A short reconciliation timeout without
		// jitter makes Argo CD notice a new commit within seconds.
		{"create", "configmap", "argocd-cm", "-n", argoCDNamespace, "--from-literal=timeout.reconciliation=5s", "--from-literal=timeout.reconciliation.jitter=0s"},
		{"apply", "--server-side", "-n", argoCDNamespace, "-f", "/tmp/core-install.yaml"},
		{"rollout", "status", "-n", argoCDNamespace, "deployment/argocd-repo-server", "--timeout=5m"},
		{"rollout", "status", "-n", argoCDNamespace, "statefulset/argocd-application-controller", "--timeout=5m"},
	} {
		if err := kubectl(ctx, cluster, args...); err != nil {
			return err
		}
	}
	return nil
}

func kubectl(ctx context.Context, cluster *k3s.K3sContainer, args ...string) error {
	code, out, err := cluster.Exec(ctx, append([]string{"kubectl"}, args...), tcexec.Multiplexed())
	if err != nil {
		return err
	}
	if code != 0 {
		msg, _ := io.ReadAll(out)
		return fmt.Errorf("kubectl %s: %s", strings.Join(args, " "), msg)
	}
	return nil
}

// argoCDDir is the source directory of the Argo CD module in go.mod.
func argoCDDir() (string, error) {
	out, err := exec.CommandContext(context.Background(), "go", "list", "-m", "-f", "{{.Dir}}", "github.com/argoproj/argo-cd/v3").Output()
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return "", fmt.Errorf("locating Argo CD module: %w: %s", err, exitErr.Stderr)
	}
	return strings.TrimSpace(string(out)), err
}

// buildController builds the controller into dist, which also holds the controllers' logs, or returns E2E_CONTROLLER.
func buildController() (string, error) {
	dist := filepath.Join("..", "..", "dist")
	if err := os.MkdirAll(dist, 0o755); err != nil {
		return "", err
	}
	if bin := os.Getenv("E2E_CONTROLLER"); bin != "" {
		return bin, nil
	}
	bin := filepath.Join(dist, "argo-cd-sync-controller-e2e")
	out, err := exec.CommandContext(context.Background(), "go", "build", "-race", "-o", bin, "../../cmd").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, out)
	}
	return bin, nil
}

// startController runs bin for the apps and Argo CD settings in ns, logging to dist/e2e-controller-<ns>.log.
func startController(bin, kubeconfig, ns string) (*exec.Cmd, error) {
	logFile, err := os.Create(filepath.Join("..", "..", "dist", "e2e-controller-"+ns+".log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(context.Background(), bin, "--leader-elect=false", "--argocd-namespace="+ns, "--namespaces="+ns,
		"--metrics-bind-address=0", "--health-probe-bind-address=0", "--zap-devel")
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}
