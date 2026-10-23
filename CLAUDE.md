# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Target names match argo-cd's Makefile; the non `-local` names (`build`, `lint`, `test`, ...) are aliases.

```bash
make codegen-local        # deepcopy, CRDs (config/crd/bases) and ClusterRole (config/rbac/role.yaml)
make build-local          # dist/argo-cd-sync-controller
make lint-local           # golangci-lint v2.13.2 with argo-cd's config, --fix
make test-local           # go test -race -cover, except test/e2e; TEST_MODULE=./internal/controller for one package
make test-e2e-local       # test/e2e in a throwaway k3s container with Argo CD (go.mod's release) installed; needs Docker and git
make pre-commit-local     # codegen, build, lint, test: must pass before committing

go test -race ./internal/controller -run TestDependencyOrder   # single test

make start-test-k8s       # k3d cluster with the Argo CD release from go.mod, plus the CRDs
make start-local          # goreman runs Procfile: controller + argocd-server port-forward (:8443)
make start-local START=controller   # only the controller, e.g. next to argo-cd's own `make start-local`
```

Run `make codegen-local` after changing anything in `api/v1alpha1` or any `+kubebuilder:rbac` marker; the
generated files are committed, and CI fails if they are out of date.

## Standards

Until the code moves into argo-cd, it follows argo-cd's rules so the move needs no rework:

- `.golangci.yaml` is argo-cd's, with only goimports' `local-prefixes` changed. Keep it in step with
  `../argo-cd/.golangci.yaml`, and don't relax it here. The workflows in `.github/workflows` mirror argo-cd's
  Go jobs and pin the same action SHAs and Go version.
- Use semantic commit and PR titles: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`, `ci:`, `revert:`.
- Ship every behavior change or fix with unit tests. Update `README.md` for user-visible behavior, and the
  proposal (`../argo-cd/docs/proposals/applicationsync.md`) when the design changes.

## What this is

A controller-runtime controller that syncs Argo CD Applications in dependency order, running next to an existing
Argo CD. It **never calls Argo CD's API server or repo server**: it writes `Application.operation` the way the Sync
API does, and reads results back from `status.operationState`. Argo CD's application controller does the actual
syncing. The CRDs use `argoproj.io/v1alpha1` on purpose, because the plan is to move this into Argo CD itself.

- `ApplicationSync` — a one-off, immutable (except `spec.ttlSecondsAfterFinished`, enforced by a CEL rule on the
  spec type) request to sync groups of apps; groups have `dependsOn`, and `activeDeadlineSeconds`
  fails a run that doesn't finish in time.
- `ApplicationSyncPolicy` — auto-sync for a group: creates an owned `ApplicationSync` from `spec.template` when an
  app is OutOfSync.

Managed Applications must not have Argo CD auto-sync enabled, and the controller refuses them. The policy's
`selfHeal` and the ApplicationSync's `allowEmpty` replace `automated.selfHeal`/`allowEmpty`; nothing is read from
the app's own `syncPolicy`.

### Future direction

The design goal is one sync engine inside Argo CD, so keep `ApplicationSyncPolicy` generic enough to express
these (see "Future work: auto-sync and RollingSync as policies" in `../argo-cd/docs/proposals/applicationsync.md`):

- **App auto-sync as a generated policy.** With `spec.syncPolicy.automated` enabled, the application controller
  would generate a single-app policy owned by the Application (prune from `automated.prune`, retry from
  `syncPolicy.retry`). The policy's "same change already attempted" check already matches auto-sync, and
  `automated.selfHeal`/`allowEmpty` would map to the policy's `selfHeal` and the template's `allowEmpty`. The "no auto-sync" rule would
  become "one policy per app", with the generated policy counting as that one.
- **RollingSync as a generated policy.** An ApplicationSet would generate one policy: `steps` become groups,
  `maxUpdate` becomes `maxParallel`, and `concurrencyPolicy: Replace` restarts the rollout on a new change. Pausing
  with `maxUpdate: 0` means generating a template that stops before that step, or suspending the policy at the first
  step; `maxParallel` can't be 0.

## Architecture (`internal/controller`)

- `applicationsync_controller.go` — `ApplicationSyncReconciler`. Each reconcile walks groups in `groupOrder`,
  advancing per-group/per-app phases in `status.groups`, and requeues every 30s until completed. Completed syncs are
  only revisited to delete them: `gc` keeps each app's newest `--history-limit` runs, and `expire` applies
  `ttlSecondsAfterFinished`, sparing a policy's latest run. It is woken by Application changes through a field index
  (`appIndex`) of app names.
- `applicationsyncpolicy_controller.go` — `ApplicationSyncPolicyReconciler`. Only acts on app status that is fresh
  (`comparedToCurrentSpec`), otherwise requests a refresh. Pins every source to the currently compared revision
  (`pinFromStatus`) and records the change in its status so the same change isn't retried (mirrors auto-sync).
  `concurrencyPolicy: Replace` deletes the in-flight run.
- `argocd.go` — everything the controller needs from Argo CD, only via the Kubernetes API: reading apps/AppProjects,
  `argocd-cm`/`argocd-cmd-params-cm` settings (read uncached via `APIReader`), `setOperation` (fails if an
  operation is already set, like the Sync API), and refresh requests.
- `spec.go` — spec validation (dependency cycles, duplicate apps), revision pinning onto sources by name or
  1-based position, `requireOverridePrivilegeForRevisionSync` checks, and history GC.

Ownership of an app's operation is tracked by an `Info` entry named `ApplicationSync` (value = the sync's name)
on the operation, plus an `operationState.startedAt` vs `creationTimestamp` check to ignore a deleted-and-recreated
same-named sync (`operationStateFor`).

An app only counts as done after its operation succeeded **and** its status is fresh — `status.sync.comparedTo`
matches its current spec and `reconciledAt` is after the operation (`isFresh`); otherwise a refresh is requested.

Tests use controller-runtime's fake client. `finishOperation` in `applicationsync_controller_test.go` simulates
Argo CD's application controller completing an operation; `testApp` builds an app with fresh, OutOfSync status.

`test/e2e` runs the controller against a k3s testcontainer with Argo CD's `core-install.yaml` (from the module in
`go.mod`). `argocd_test.go` syncs real apps from a git repo the test process serves (`gitserver_test.go`);
`applicationsync_test.go` uses a namespace Argo CD doesn't watch and plays its application controller, for states a
real sync can't produce on demand. Each namespace has its own controller process, logging to `dist/e2e-controller-*.log`.

## Dependencies

- Pinned to Argo CD v3.6.0-rc3 for its API types. `go.mod` must repeat Argo CD's `replace` directives (they don't
  propagate to importers) — keep them in step when bumping Argo CD, except Argo CD's local `gitops-engine/v3`
  replace: require the `gitops-engine/vX.Y.Z` tag matching the Argo CD release instead.
- Because there is no repo-server access, revisions are passed to syncs as given — branches are not resolved to
  commits.
