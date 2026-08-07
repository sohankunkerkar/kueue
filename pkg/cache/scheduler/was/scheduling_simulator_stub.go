//go:build exclude_scheduler_library

package was

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/cache/scheduler/simulator"
)

type wasSimulator struct{}

// WASOption configures the WAS simulator.
type WASOption func(*wasSimulator)

// WithDRA enables DRA device feasibility checking (stub, compiled out).
func WithDRA(cl client.Client) WASOption {
	return func(s *wasSimulator) {}
}

func NewWASSimulator(ctx context.Context, restConfig *rest.Config, opts ...WASOption) (simulator.SchedulingSimulator, error) {
	return nil, errors.New("scheduler-library integration is compiled out of this binary. Disable the SchedulerLibraryIntegration feature gate.")
}

func NewWASSimulatorForTest(ctx context.Context) (simulator.SchedulingSimulator, error) {
	return nil, errors.New("scheduler-library integration is compiled out of this binary")
}

func (s *wasSimulator) NewFeasibilityChecker(_ context.Context, _ []*corev1.Node) (simulator.NodeFeasibilityChecker, error) {
	return nil, errors.New("scheduler-library integration is compiled out")
}

func (s *wasSimulator) TrackPod(_ *corev1.Pod)            {}
func (s *wasSimulator) UntrackPod(_ types.NamespacedName)  {}
