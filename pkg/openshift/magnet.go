//go:build tools
// +build tools

package tools

// Keep a reference to the code generators so they are not removed by go mod tidy
import (
	_ "github.com/mikefarah/yq/v4/cmd"
	_ "github.com/onsi/ginkgo/v2/ginkgo/command"
	_ "github.com/onsi/ginkgo/v2/ginkgo/run"
	_ "sigs.k8s.io/kustomize/kustomize/v5/commands/edit/listbuiltin"
)
