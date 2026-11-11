package controller

import (
	"context"
	"errors"
	"testing"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// recordingSink records the messages logged at info and error level.
type recordingSink struct {
	infos, errors []string
}

func (s *recordingSink) Init(logr.RuntimeInfo)               {}
func (s *recordingSink) Enabled(int) bool                    { return true }
func (s *recordingSink) Info(_ int, msg string, _ ...any)    { s.infos = append(s.infos, msg) }
func (s *recordingSink) Error(_ error, msg string, _ ...any) { s.errors = append(s.errors, msg) }
func (s *recordingSink) WithValues(...any) logr.LogSink      { return s }
func (s *recordingSink) WithName(string) logr.LogSink        { return s }

func TestRequestRefresh(t *testing.T) {
	errPatch := errors.New("apiserver unavailable")
	tests := []struct {
		name         string
		annotation   string
		patchErr     error
		wantPatches  int
		wantRefresh  string
		wantInfoLogs int
	}{
		{name: "sets the refresh annotation", wantPatches: 1, wantRefresh: string(argov1alpha1.RefreshTypeNormal)},
		{name: "leaves a pending refresh alone", annotation: string(argov1alpha1.RefreshTypeHard), wantRefresh: string(argov1alpha1.RefreshTypeHard)},
		{name: "logs a failed patch at info level", patchErr: errPatch, wantPatches: 1, wantInfoLogs: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := testApp("db")
			if tt.annotation != "" {
				app.Annotations = map[string]string{refreshAnnotation: tt.annotation}
			}
			patches := 0
			c := fake.NewClientBuilder().
				WithScheme(testScheme(t)).
				WithObjects(app).
				WithInterceptorFuncs(interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches++
					if tt.patchErr != nil {
						return tt.patchErr
					}
					return c.Patch(ctx, obj, patch, opts...)
				}}).
				Build()
			sink := &recordingSink{}
			ctx := log.IntoContext(t.Context(), logr.New(sink))

			a := &argoCD{client: c, apiReader: c, namespace: testNamespace}
			a.requestRefresh(ctx, getTestApp(t, c, "db"))

			assert.Equal(t, tt.wantPatches, patches)
			assert.Empty(t, sink.errors, "a refresh is retried on the next reconcile, so failures aren't errors")
			assert.Len(t, sink.infos, tt.wantInfoLogs)
			got := getTestApp(t, c, "db")
			assert.Equal(t, tt.wantRefresh, got.Annotations[refreshAnnotation])
		})
	}
}
