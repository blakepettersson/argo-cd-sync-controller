# argo-cd-sync-controller

A controller that syncs Argo CD Applications in dependency order, driven by two resources:

- **`ApplicationSync`**: a one-off, immutable request to sync groups of Applications. Groups can depend on other
  groups, and a group starts once the groups it depends on are Synced and Healthy. Create one with
  `kubectl apply`.
- **`ApplicationSyncPolicy`**: auto-sync for a group. It creates an ApplicationSync from its template whenever
  one of its Applications is OutOfSync.

It runs next to an existing Argo CD installation. It never talks to Argo CD's API server or repo server: it sets
each Application's `operation` the way the Sync API does, and reads the result back from
`status.operationState`. Argo CD's application controller runs the syncs as usual.

The plan is to move this into Argo CD itself later, so the resources use Argo CD's API group,
`argoproj.io/v1alpha1`. Then no CRD rename is needed when that happens.

## Example

```yaml
apiVersion: argoproj.io/v1alpha1
kind: ApplicationSync
metadata:
  name: release-42
  namespace: argocd
spec:
  prune: true
  groups:
  - name: data
    apps:
    - name: db
  - name: backend
    dependsOn: [data]
    apps:
    - name: api
    - name: worker
  - name: frontend
    dependsOn: [backend]
    apps:
    - name: web
```

```bash
kubectl apply -f release-42.yaml
kubectl get appsync release-42 -o wide
```

A single app is a single group with one entry. A group's `name` defaults to `default`, so it can be left out
when there is only one group (`groups: [{apps: [{name: guestbook}]}]`). Status is reported per group and per app
under `status.groups`.

## Behavior

- **No auto-sync.** Applications in an ApplicationSync or an ApplicationSyncPolicy must not have auto-sync
  enabled. Auto-sync would sync an app ahead of the groups it depends on, or undo a pinned revision. The
  controller enforces this: an app with auto-sync on fails (or waits, with `onFailure: Wait`) without being
  synced, and a policy starts no run while any of its apps has it on. To keep a group of apps in sync
  automatically, use an ApplicationSyncPolicy instead, with `selfHeal` if drift should be reverted. Nothing is
  read from an app's own `syncPolicy.automated`.
- **Waiting.** An app that doesn't exist yet waits; the run's message and an `ApplicationWaiting` event say why. An
  app that already has an operation running fails, as the Sync API rejects it, unless `onFailure: Wait` is set;
  policy runs always wait for it. An app deleted, or recreated under the same name, after its sync started
  fails, even with `onFailure: Wait`.
- **Deadline.** `spec.activeDeadlineSeconds` fails a run that hasn't finished that long after it started, as for a
  Job: unfinished apps fail, running operations finish, and nothing new starts.
- **Health gate.** An app is done once its sync succeeded and it is Synced and Healthy. That status is only
  trusted once `status.sync.comparedTo` shows the app's current spec, and the controller requests a refresh
  when it doesn't.
- **Failures.** When an app fails, the rest of its group finishes, the group fails, and the groups that depend
  on it are Skipped. With `onFailure: Wait`, a failed or Degraded app keeps its dependents waiting until it
  recovers.
- **Sync windows** are enforced. A closed window keeps the app Pending without starting a sync.
- **Automatic runs.** Only runs owned by an ApplicationSyncPolicy are automatic: sync windows apply to them as to
  auto-sync, and with prune on, an app whose resources would all be pruned fails without syncing. Every other run
  is manual.
- **Immutability.** `spec` is immutable, except `spec.ttlSecondsAfterFinished`. To stop a run, delete it: operations
  already running finish and nothing new starts.
- **Suspend.** A policy with `suspend: true` starts no runs, self-heal included, as a suspended CronJob starts no
  Jobs. A run already going continues; delete it to stop the rollout.
- **Self-heal.** A policy with `selfHeal: true` starts another run for the same change when an app drifts after a
  successful run, syncing only the resources that aren't Synced. Runs are spaced by the same
  `controller.self.heal.*` settings in `argocd-cmd-params-cm` that Argo CD's auto-sync uses. An app the run left
  OutOfSync, as `doneWhen: Healthy` allows, isn't treated as drift and is left out of self-heal runs.
- **Observed generation.** Both resources set `status.observedGeneration`, so you can tell when a TTL change or a
  policy edit has been picked up.
- **History.** Each app keeps its 20 most recent completed ApplicationSyncs (`--history-limit`); a run is deleted
  once it is among the newest 20 for none of its apps. `spec.ttlSecondsAfterFinished` also deletes a run that many
  seconds after it finishes, as for a Job; a policy's latest run is kept until a newer one replaces it.

| Field | Effect |
|---|---|
| `groups[].maxParallel` | How many apps of the group sync at once, as a number or a percentage; at least 1 or 1%. |
| `groups[].apps[].revisions` | Pins sources, by `source` name or 1-based `position`. Revisions are passed to the sync as given, so use commit SHAs to be sure every app gets the same one. Sources that aren't listed sync to their `targetRevision`. |
| `groups[].apps[].prune`, `.retry`, `.resources` | Per-app prune, retry and partial sync. |
| `doneWhen: Healthy` | Healthy is enough; OutOfSync doesn't block. |
| `skipIfSynced: true` | Apps that are already Synced aren't synced again. |
| `allowEmpty: true` | Lets an automatic run with prune delete every resource of an app, like `automated.allowEmpty`. An app with no resources never needs it. |

## Authorization

Creating an `ApplicationSync` needs Kubernetes RBAC `create` on `applicationsyncs` in the apps' namespace. That
is equivalent to granting sync on every Application in that namespace. Argo CD RBAC is not consulted, the same as
for writing `operation` with kubectl today. With `application.sync.requireOverridePrivilegeForRevisionSync` set in
`argocd-cm`, a pin must name `targetRevision`, or the revision the app last resolved it to.

## Differences from running inside Argo CD

A prototype of this ran inside Argo CD's application controller. Running outside it means:

- **Revisions are passed as given.** There is no repo-server access to resolve a branch to a commit.
- **Syncs from Argo CD's API and UI aren't recorded** as ApplicationSyncs, and ApplicationSet RollingSync is not
  replaced.

## Installing

```bash
kubectl apply --server-side -k config/default   # into the argocd namespace
```

The image in `config/manager` is a placeholder until the project publishes images.

## Development

The project is meant to move into Argo CD, so it follows Argo CD's conventions: the same `.golangci.yaml`,
the same `make` target names, the same CI checks, and semantic PR titles (`feat:`, `fix:`, `docs:`, ...).

```bash
make codegen-local        # deepcopy, CRDs and ClusterRole from the kubebuilder markers
make build-local
make lint-local           # golangci-lint with Argo CD's config, applying fixes
make test-local           # TEST_MODULE=./internal/controller for one package
make test-e2e-local       # e2e suite against a throwaway k3s container; needs Docker
make pre-commit-local     # all of the above; run it before committing
```

### Running everything locally

Like Argo CD's `make start-local`, this runs the processes in `Procfile` with
[goreman](https://github.com/mattn/goreman):

```bash
make start-test-k8s       # k3d cluster with the Argo CD release from go.mod, and the CRDs
make start-local          # the controller, and the Argo CD UI on https://localhost:8443
kubectl apply -k config/samples                          # example Applications, auto-sync off
kubectl create -n argocd -f config/samples/applicationsync.yaml
```

To run against Argo CD built from source instead, start it with `make start-local` in an argo-cd checkout, then
run only the controller here with `make start-local START=controller`. The controller's metrics and probes
listen on `:18080` and `:18081`, so they don't clash with Argo CD's ports. `ARGOCD_APPLICATION_NAMESPACES` is
passed to the controller's `--namespaces`, as for Argo CD.

It depends on Argo CD v3.6.0-rc3 for its API types, and on the matching `gitops-engine/v3` release. `go.mod` repeats
Argo CD's replace directives, which don't carry over to modules that import it.
