PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
ARTIFACTS ?= $(PROJECT_DIR)/bin

ifeq ($(shell uname),Darwin)
    GOFLAGS ?= -ldflags=-linkmode=internal
endif

ifeq (,$(shell go env GOBIN))
	GOBIN=$(shell go env GOPATH)/bin
else
	GOBIN=$(shell go env GOBIN)
endif

GO_CMD ?= go
GO_TEST_FLAGS ?= -race
version_pkg = sigs.k8s.io/kueue/pkg/version
LD_FLAGS += -X '$(version_pkg).GitVersion=$(GIT_TAG)'
LD_FLAGS += -X '$(version_pkg).GitCommit=$(shell git rev-parse HEAD)'

# Use go.mod go version as source.
GINKGO_VERSION ?= $(shell $(GO_CMD) list -m -f '{{.Version}}' github.com/onsi/ginkgo/v2)
KUSTOMIZE_VERSION ?= $(shell $(GO_CMD) list -m -f '{{.Version}}' sigs.k8s.io/kustomize/kustomize/v5)
YQ_VERSION ?= $(shell $(GO_CMD) list -m -f '{{.Version}}' github.com/mikefarah/yq/v4)

KUSTOMIZE = $(PROJECT_DIR)/bin/kustomize
.PHONY: kustomize
kustomize: ## Download kustomize locally if necessary.
	@GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install sigs.k8s.io/kustomize/kustomize/v5@$(KUSTOMIZE_VERSION)

GINKGO = $(PROJECT_DIR)/bin/ginkgo
.PHONY: ginkgo
ginkgo: ## Download ginkgo locally if necessary.
	@GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install github.com/onsi/ginkgo/v2/ginkgo@$(GINKGO_VERSION)

YQ = $(PROJECT_DIR)/bin/yq
.PHONY: yq
yq: ## Download yq locally if necessary.
	@GOBIN=$(PROJECT_DIR)/bin GO111MODULE=on $(GO_CMD) install github.com/mikefarah/yq/v4@$(YQ_VERSION)

.PHONY: test-e2e-ocp
test-e2e-ocp: kustomize ginkgo yq dep-crds kueuectl ginkgo-top-ocp run-test-e2e-ocp-singlecluster
run-test-e2e-ocp-singlecluster:
	@echo "Running e2e tests on OpenShift cluster ($(shell oc whoami --show-server))"
		ARTIFACTS="$(ARTIFACTS)/$@" IMAGE_TAG=$(IMAGE_TAG) GINKGO_ARGS="$(GINKGO_ARGS)" \
		E2E_TARGET_FOLDER="singlecluster" \
		./hack/e2e-test-ocp.sh
	$(PROJECT_DIR)/bin/ginkgo-top -i $(ARTIFACTS)/$@/e2e.json > $(ARTIFACTS)/$@/e2e-top.yaml

.PHONY: ginkgo-top-ocp
ginkgo-top-ocp:
	$(GO_BUILD_ENV) $(GO_CMD) build -ldflags="$(LD_FLAGS)" -o $(PROJECT_DIR)/bin/ginkgo-top ./ginkgo-top

.PHONY: kueuectl
kueuectl:
	CGO_ENABLED=$(CGO_ENABLED) $(GO_BUILD_ENV) $(GO_CMD) build -ldflags="$(LD_FLAGS)" -o $(PROJECT_DIR)/bin/kubectl-kueue cmd/kueuectl/main.go
