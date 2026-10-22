# Target names follow argo-cd's Makefile, so the same commands work in both repos. There is no containerized
# toolchain here, so build, lint, test, codegen and pre-commit are aliases of their -local targets.

CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.18.0
# The golangci-lint version argo-cd's CI uses.
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2

# The Argo CD release this controller builds against, which start-test-k8s installs.
ARGOCD_VERSION ?= $(shell go list -m -f '{{.Version}}' github.com/argoproj/argo-cd/v3)
TEST_MODULE ?= ./...
# Procfile processes for start-local; empty starts them all.
START ?=

.PHONY: all
all: pre-commit-local

.PHONY: generate
generate: ## Generate DeepCopy methods
	$(CONTROLLER_GEN) object:headerFile=hack/boilerplate.go.txt paths=./api/...

.PHONY: manifests
manifests: ## Generate CRDs and the ClusterRole
	$(CONTROLLER_GEN) rbac:roleName=argo-cd-sync-controller crd paths=./... \
		output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

.PHONY: codegen-local
codegen-local: generate manifests

.PHONY: build-local
build-local:
	go build -o dist/argo-cd-sync-controller ./cmd

.PHONY: lint-local
lint-local:
	$(GOLANGCI_LINT) --version
	$(GOLANGCI_LINT) run --fix --verbose

# Set TEST_MODULE to test a single package, e.g. TEST_MODULE=./internal/controller.
.PHONY: test-local
test-local:
	go test -race -cover $(TEST_MODULE)

.PHONY: test-e2e-local
test-e2e-local: build-local install ## Run the e2e suite against the current cluster
	E2E=true go test -race -count=1 -v ./test/e2e/...

.PHONY: pre-commit-local
pre-commit-local: codegen-local build-local lint-local test-local

.PHONY: build lint test codegen pre-commit
build: build-local
lint: lint-local
test: test-local
codegen: codegen-local
pre-commit: pre-commit-local

.PHONY: install
install: manifests ## Install the CRDs into the current cluster
	kubectl apply --server-side -k config/crd

# Creates a k3d cluster running Argo CD $(ARGOCD_VERSION), for start-local.
.PHONY: start-test-k8s
start-test-k8s:
	ARGOCD_VERSION=$(ARGOCD_VERSION) ./hack/start-test-k8s.sh

# Runs the Procfile with goreman against the current cluster, as argo-cd's start-local does.
.PHONY: start-local
start-local: install
	goreman -f Procfile start $(START)

.PHONY: run
run: ## Run only the controller against the current cluster
	go run ./cmd --leader-elect=false
