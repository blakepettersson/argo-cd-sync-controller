CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.18.0

.PHONY: all
all: generate manifests build test

.PHONY: generate
generate: ## Generate DeepCopy methods
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

.PHONY: manifests
manifests: ## Generate CRDs and the ClusterRole
	$(CONTROLLER_GEN) rbac:roleName=argocd-application-sync-controller crd paths=./... \
		output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

.PHONY: build
build:
	go build -o bin/argocd-application-sync-controller ./cmd

.PHONY: test
test:
	go test -race ./...

.PHONY: lint
lint:
	go vet ./...

.PHONY: install
install: manifests ## Install the CRDs into the current cluster
	kubectl apply --server-side -k config/crd

.PHONY: run
run: ## Run the controller locally against the current cluster
	go run ./cmd --leader-elect=false
