package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/cgi"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"
)

// gitRepo is a git repository this process serves over smart HTTP with git http-backend, as Argo CD's own e2e tests
// do, for the Argo CD in the cluster to sync from.
type gitRepo struct {
	root   string
	server *http.Server
	port   int
	// url reaches the repository from pods, through the port testcontainers forwards from the cluster to this host.
	url string
}

func startGitServer() (*gitRepo, error) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "argo-cd-sync-controller-e2e-git-")
	if err != nil {
		return nil, err
	}
	r := &gitRepo{root: root}
	if err := r.git("init", "--initial-branch=main"); err != nil {
		return nil, errors.Join(err, os.RemoveAll(root))
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(root))
	}
	r.port = ln.Addr().(*net.TCPAddr).Port
	r.server = &http.Server{
		Handler: &cgi.Handler{
			Path: gitPath,
			Args: []string{"http-backend"},
			Env:  []string{"GIT_PROJECT_ROOT=" + root, "GIT_HTTP_EXPORT_ALL=1"},
		},
		ReadHeaderTimeout: time.Minute,
	}
	go func() { _ = r.server.Serve(ln) }()
	return r, nil
}

// resolveURL sets url to the address of the sshd container testcontainers forwards the port through, which pods can
// reach where they can't resolve its host alias.
func (r *gitRepo) resolveURL(ctx context.Context, cluster *k3s.K3sContainer) error {
	info, err := cluster.Inspect(ctx)
	if err != nil {
		return err
	}
	for _, h := range info.HostConfig.ExtraHosts {
		if ip, ok := strings.CutPrefix(h, testcontainers.HostInternal+":"); ok {
			r.url = fmt.Sprintf("http://%s:%d/.git", ip, r.port)
			return nil
		}
	}
	return fmt.Errorf("no %s host in the k3s container", testcontainers.HostInternal)
}

func (r *gitRepo) close() {
	_ = r.server.Close()
	_ = os.RemoveAll(r.root)
}

func (r *gitRepo) git(args ...string) error {
	cmd := exec.CommandContext(context.Background(), "git", append([]string{"-c", "user.name=e2e", "-c", "user.email=e2e@example.com"}, args...)...)
	cmd.Dir = r.root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return nil
}

// commit writes files, by path in the repository, commits them to main, and returns the commit SHA.
func (r *gitRepo) commit(t *testing.T, files map[string]string) string {
	t.Helper()
	for path, content := range files {
		path = filepath.Join(r.root, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	require.NoError(t, r.git("add", "--all"))
	require.NoError(t, r.git("commit", "--message", t.Name()))
	out, err := exec.CommandContext(t.Context(), "git", "-C", r.root, "rev-parse", "HEAD").Output()
	require.NoError(t, err)
	return strings.TrimSpace(string(out))
}

// configMap is a manifest for a ConfigMap whose data.value is value, which Argo CD counts as Healthy once applied.
func configMap(name, value string) string {
	return fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: %s\ndata:\n  value: %q\n", name, value)
}
