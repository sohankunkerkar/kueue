package simulator

import (
	"context"
	"iter"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
)

type testCandidate struct {
	node          *corev1.Node
	id            utiltas.TopologyDomainID
	affinityScore int64
}

func (c *testCandidate) GetNode() *corev1.Node            { return c.node }
func (c *testCandidate) GetID() utiltas.TopologyDomainID   { return c.id }
func (c *testCandidate) GetAffinityScore() int64           { return c.affinityScore }
func (c *testCandidate) SetAffinityScore(score int64)      { c.affinityScore = score }

type passthroughChecker struct{}

func (p *passthroughChecker) FindFeasibleNodes(_ context.Context, candidates iter.Seq[Candidate], _ *PodRequirements, stats *NodeExclusionStats) ([]MatchedCandidate, error) {
	var result []MatchedCandidate
	for c := range candidates {
		mc := c.(MatchedCandidate)
		stats.TotalNodes++
		result = append(result, mc)
	}
	return result, nil
}

func TestDRACheckerFindFeasibleNodes(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = resourceapi.AddToScheme(scheme)

	gpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-node"},
	}
	cpuNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "cpu-node"},
	}

	gpuDeviceClass := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu.example.com"},
	}
	gpuClaimTemplate := &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-template", Namespace: "default"},
		Spec: resourceapi.ResourceClaimTemplateSpec{
			Spec: resourceapi.ResourceClaimSpec{
				Devices: resourceapi.DeviceClaim{
					Requests: []resourceapi.DeviceRequest{
						{
							Name: "gpu",
							Exactly: &resourceapi.ExactDeviceRequest{
								DeviceClassName:  "gpu.example.com",
								AllocationMode:   resourceapi.DeviceAllocationModeExactCount,
								Count:            1,
							},
						},
					},
				},
			},
		},
	}
	gpuSlice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-node-slice"},
		Spec: resourceapi.ResourceSliceSpec{
			Driver:   "gpu.example.com",
			NodeName: ptr.To("gpu-node"),
			Pool: resourceapi.ResourcePool{
				Name:               "gpu-pool",
				Generation:         1,
				ResourceSliceCount: 1,
			},
			Devices: []resourceapi.Device{
				{Name: "gpu-0"},
				{Name: "gpu-1"},
			},
		},
	}

	gpuClaim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "existing-gpu-claim", Namespace: "default"},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "gpu",
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: "gpu.example.com",
							AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
							Count:           1,
						},
					},
				},
			},
		},
	}

	tests := map[string]struct {
		objects      []runtime.Object
		podTemplate  *corev1.PodTemplateSpec
		candidates   []*testCandidate
		wantFeasible []string
		wantErr      bool
	}{
		"non-DRA pod passes through all nodes": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass},
			podTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
				{node: cpuNode, id: "cpu-node"},
			},
			wantFeasible: []string{"gpu-node", "cpu-node"},
		},
		"nil PodTemplate passes through": {
			objects:     []runtime.Object{gpuSlice, gpuDeviceClass},
			podTemplate: nil,
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
			},
			wantFeasible: []string{"gpu-node"},
		},
		"DRA pod filters out nodes without matching devices": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass, gpuClaimTemplate},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:                      "gpu",
							ResourceClaimTemplateName: ptr.To("gpu-template"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
				{node: cpuNode, id: "cpu-node"},
			},
			wantFeasible: []string{"gpu-node"},
		},
		"ResourceClaimName resolves existing claim": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass, gpuClaim},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:              "gpu",
							ResourceClaimName: ptr.To("existing-gpu-claim"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
				{node: cpuNode, id: "cpu-node"},
			},
			wantFeasible: []string{"gpu-node"},
		},
		"missing ResourceClaimTemplate returns error": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:                      "gpu",
							ResourceClaimTemplateName: ptr.To("nonexistent-template"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
			},
			wantErr: true,
		},
		"missing ResourceClaimName returns error": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:              "gpu",
							ResourceClaimName: ptr.To("nonexistent-claim"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
			},
			wantErr: true,
		},
		"candidate with nil node passes through": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass, gpuClaimTemplate},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:                      "gpu",
							ResourceClaimTemplateName: ptr.To("gpu-template"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: nil, id: "rack-level"},
				{node: gpuNode, id: "gpu-node"},
				{node: cpuNode, id: "cpu-node"},
			},
			wantFeasible: []string{"rack-level", "gpu-node"},
		},
		"multiple claims on one pod": {
			objects: []runtime.Object{
				gpuSlice, gpuDeviceClass, gpuClaimTemplate,
				&resourceapi.ResourceClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "gpu-template-2", Namespace: "default"},
					Spec: resourceapi.ResourceClaimTemplateSpec{
						Spec: resourceapi.ResourceClaimSpec{
							Devices: resourceapi.DeviceClaim{
								Requests: []resourceapi.DeviceRequest{
									{
										Name: "gpu2",
										Exactly: &resourceapi.ExactDeviceRequest{
											DeviceClassName: "gpu.example.com",
											AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
											Count:           1,
										},
									},
								},
							},
						},
					},
				},
			},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:                      "gpu",
							ResourceClaimTemplateName: ptr.To("gpu-template"),
						},
						{
							Name:                      "gpu2",
							ResourceClaimTemplateName: ptr.To("gpu-template-2"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
			},
			wantFeasible: []string{"gpu-node"},
		},
		"DRA pod with all devices allocated filters out all nodes": {
			objects: []runtime.Object{
				gpuSlice, gpuDeviceClass, gpuClaimTemplate,
				&resourceapi.ResourceClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "existing-claim", Namespace: "other"},
					Spec: resourceapi.ResourceClaimSpec{
						Devices: resourceapi.DeviceClaim{
							Requests: []resourceapi.DeviceRequest{
								{
									Name: "gpu",
									Exactly: &resourceapi.ExactDeviceRequest{
										DeviceClassName: "gpu.example.com",
										Count:           1,
									},
								},
							},
						},
					},
					Status: resourceapi.ResourceClaimStatus{
						Allocation: &resourceapi.AllocationResult{
							Devices: resourceapi.DeviceAllocationResult{
								Results: []resourceapi.DeviceRequestAllocationResult{
									{Request: "gpu", Driver: "gpu.example.com", Pool: "gpu-pool", Device: "gpu-0"},
									{Request: "gpu", Driver: "gpu.example.com", Pool: "gpu-pool", Device: "gpu-1"},
								},
							},
						},
					},
				},
			},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{
							Name:                      "gpu",
							ResourceClaimTemplateName: ptr.To("gpu-template"),
						},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
			},
			wantFeasible: nil,
		},
		"PodResourceClaim with neither name nor template is skipped": {
			objects: []runtime.Object{gpuSlice, gpuDeviceClass},
			podTemplate: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
					ResourceClaims: []corev1.PodResourceClaim{
						{Name: "empty"},
					},
				},
			},
			candidates: []*testCandidate{
				{node: gpuNode, id: "gpu-node"},
				{node: cpuNode, id: "cpu-node"},
			},
			wantFeasible: []string{"gpu-node", "cpu-node"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cl := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.objects...).Build()
			inner := &passthroughChecker{}
			checker := NewDRAChecker(inner, cl)

			candidateSeq := func(yield func(Candidate) bool) {
				for _, c := range tc.candidates {
					if !yield(c) {
						return
					}
				}
			}

			stats := &NodeExclusionStats{}
			feasible, err := checker.FindFeasibleNodes(t.Context(), candidateSeq, &PodRequirements{
				PodTemplate: tc.podTemplate,
			}, stats)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error but got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("FindFeasibleNodes returned error: %v", err)
			}

			var gotNames []string
			for _, mc := range feasible {
				if mc.GetNode() != nil {
					gotNames = append(gotNames, mc.GetNode().Name)
				} else {
					gotNames = append(gotNames, string(mc.GetID()))
				}
			}

			if diff := cmp.Diff(tc.wantFeasible, gotNames); diff != "" {
				t.Errorf("feasible nodes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
