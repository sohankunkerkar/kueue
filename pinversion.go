//go:build tools
// +build tools

package tools

// Keep a reference to the code generators so they are not removed by go mod tidy
import (
	_ "k8s.io/code-generator"
	_ "sigs.k8s.io/mdtoc"

	// since verify will error when referencing a cmd package
	// we need to reference individual dependencies used by it
	_ "github.com/gohugoio/hugo/common"
	_ "github.com/gohugoio/hugo/docshelper"
	_ "github.com/golangci/golangci-lint/pkg/exitcodes"
	_ "github.com/mikefarah/yq/v4/cmd"
	_ "github.com/onsi/ginkgo/v2/ginkgo/command"
	_ "github.com/onsi/ginkgo/v2/ginkgo/run"
	_ "gotest.tools/gotestsum/cmd"
	_ "helm.sh/helm/v3/pkg/cli"
	_ "helm.sh/helm/v3/pkg/lint"
	_ "sigs.k8s.io/controller-runtime/tools/setup-envtest/env"
	_ "sigs.k8s.io/controller-tools/pkg/crd"
	_ "sigs.k8s.io/controller-tools/pkg/genall/help/pretty"
	_ "sigs.k8s.io/kind/pkg/cmd"
	_ "sigs.k8s.io/kustomize/kustomize/v5/commands/edit/listbuiltin"
)
