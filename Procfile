controller: go run ./cmd --leader-elect=false --zap-devel --zap-log-level=debug --metrics-bind-address=:18080 --health-probe-bind-address=:18081 --argocd-namespace=${ARGOCD_NAMESPACE:-argocd} --namespaces=${ARGOCD_APPLICATION_NAMESPACES:-}
argocd-server: sh -c 'while true; do kubectl port-forward -n ${ARGOCD_NAMESPACE:-argocd} svc/argocd-server 8443:443; sleep 2; done'
