# argocd-application-sync-crd

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
  enabled. Auto-sync would sync an app ahead of the groups it depends on, or undo a pinned revision. To keep a
  group of apps in sync automatically, use an ApplicationSyncPolicy instead.
- **Waiting.** An app that doesn't exist yet, or already has an operation running, waits.
- **Health gate.** An app is done once its sync succeeded and it is Synced and Healthy. That status is only
  trusted once `status.sync.comparedTo` shows the app's current spec, and the controller requests a refresh
  when it doesn't.
- **Failures.** When an app fails, the rest of its group finishes, the group fails, and the groups that depend
  on it are Skipped. With `onFailure: Wait`, a failed or Degraded app keeps its dependents waiting until it
  recovers.
- **Sync windows** are enforced. A closed window keeps the app Pending without starting a sync.
- **Immutability.** `spec` is immutable, except `spec.paused`.
- **History.** The 20 most recent completed ApplicationSyncs are kept per set of apps (`--history-limit`).

| Field | Effect |
|---|---|
| `groups[].maxParallel` | How many apps of the group sync at once, as a number or a percentage. `0` pauses the group. |
| `groups[].apps[].revisions` | Pins sources, by `source` name or 1-based `position`. Revisions are passed to the sync as given, so use commit SHAs to be sure every app gets the same one. Sources that aren't listed sync to their `targetRevision`. |
| `groups[].apps[].prune`, `.retry`, `.resources` | Per-app prune, retry and partial sync. |
| `doneWhen: Healthy` | Healthy is enough; OutOfSync doesn't block. |
| `skipIfSynced: true` | Apps that are already Synced aren't synced again. |
| `initiatedBy.automated: true` | Sync windows apply as they do to auto-sync. |

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

```bash
make generate manifests   # deepcopy, CRDs and ClusterRole from the kubebuilder markers
make test
make run                  # run against the current kubeconfig context
```

It depends on Argo CD v3.5.3 for its API types. Two things come with that:

- **Replace directives.** `go.mod` repeats Argo CD's replace directives, which don't carry over to modules that
  import it.
- **Vendored gitops-engine.** `third_party/gitops-engine` is the `gitops-engine` directory of that release,
  because its required version can't be resolved from outside Argo CD. See `third_party/README.md`.
