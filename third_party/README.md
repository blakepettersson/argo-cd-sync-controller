`gitops-engine/` is the `gitops-engine` directory of Argo CD v3.5.3, without tests (`git archive v3.5.3 gitops-engine`).

Argo CD v3.5.3 requires `github.com/argoproj/argo-cd/gitops-engine` at a pseudo-version whose commit has no
`gitops-engine/` directory, and resolves it with a local replace that doesn't reach modules importing Argo CD.
Replace this copy with a proper module version once Argo CD tags `gitops-engine` releases.
