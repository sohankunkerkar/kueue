package simulator

import (
	"context"
	"errors"
	"fmt"
	"iter"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	dracel "k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DRAChecker wraps any NodeFeasibilityChecker and adds per-node DRA device
// availability filtering. It uses the structured.Allocator, the same
// allocation engine as kube-scheduler, to verify that candidate nodes
// have matching devices before admitting workloads.
type DRAChecker struct {
	inner    NodeFeasibilityChecker
	cl       client.Client
	celCache *dracel.Cache
}

func NewDRAChecker(inner NodeFeasibilityChecker, cl client.Client) *DRAChecker {
	return &DRAChecker{
		inner: inner,
		cl:    cl,
		celCache: dracel.NewCache(10, dracel.Features{
			EnableConsumableCapacity:  utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity),
			EnableListTypeAttributes: utilfeature.DefaultFeatureGate.Enabled(features.DRAListTypeAttributes),
		}),
	}
}

func (c *DRAChecker) FindFeasibleNodes(
	ctx context.Context,
	candidates iter.Seq[Candidate],
	requirements *PodRequirements,
	stats *NodeExclusionStats,
) ([]MatchedCandidate, error) {
	feasible, err := c.inner.FindFeasibleNodes(ctx, candidates, requirements, stats)
	if err != nil {
		return nil, err
	}

	if requirements.PodTemplate == nil || !hasDRAClaims(requirements.PodTemplate) {
		return feasible, nil
	}

	allocator, claims, err := c.buildAllocator(ctx, requirements)
	if err != nil {
		return nil, err
	}
	if len(claims) == 0 {
		return feasible, nil
	}

	return c.filterByDevices(ctx, feasible, allocator, claims, stats)
}

func (c *DRAChecker) buildAllocator(ctx context.Context, requirements *PodRequirements) (structured.Allocator, []*resourceapi.ResourceClaim, error) {
	// In production, Namespace is set from the Workload's namespace via
	// PodRequirements. The PodTemplate fallback covers unit tests where
	// PodTemplate.ObjectMeta.Namespace is set directly.
	ns := requirements.Namespace
	if ns == "" {
		ns = requirements.PodTemplate.Namespace
	}
	claims, err := buildSyntheticClaims(ctx, c.cl, ns, requirements.PodTemplate)
	if err != nil {
		return nil, nil, fmt.Errorf("building synthetic DRA claims: %w", err)
	}
	if len(claims) == 0 {
		return nil, nil, nil
	}

	var sliceList resourceapi.ResourceSliceList
	if err := c.cl.List(ctx, &sliceList); err != nil {
		return nil, nil, fmt.Errorf("listing ResourceSlices: %w", err)
	}
	slices := make([]*resourceapi.ResourceSlice, len(sliceList.Items))
	for i := range sliceList.Items {
		slices[i] = &sliceList.Items[i]
	}

	allocatedState, err := buildAllocatedState(ctx, c.cl)
	if err != nil {
		return nil, nil, fmt.Errorf("building allocated device state: %w", err)
	}

	classLister := &clientDeviceClassLister{cl: c.cl, ctx: ctx}
	allocator, err := structured.NewAllocator(ctx, draFeatures(), allocatedState, classLister, slices, c.celCache)
	if err != nil {
		return nil, nil, fmt.Errorf("creating DRA allocator: %w", err)
	}
	return allocator, claims, nil
}

func (c *DRAChecker) filterByDevices(
	ctx context.Context,
	feasible []MatchedCandidate,
	allocator structured.Allocator,
	claims []*resourceapi.ResourceClaim,
	stats *NodeExclusionStats,
) ([]MatchedCandidate, error) {
	logger := log.FromContext(ctx)
	var draFeasible []MatchedCandidate
	for _, candidate := range feasible {
		node := candidate.GetNode()
		if node == nil {
			draFeasible = append(draFeasible, candidate)
			continue
		}

		results, err := allocator.Allocate(ctx, node, claims)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			logger.V(3).Info("DRA allocation error for node, skipping", "node", node.Name, "error", err)
			stats.SchedulerLibraryNoFit++
			continue
		}
		if results == nil {
			logger.V(5).Info("Node lacks matching DRA devices", "node", node.Name)
			stats.SchedulerLibraryNoFit++
			continue
		}

		draFeasible = append(draFeasible, candidate)
	}
	return draFeasible, nil
}

func draFeatures() structured.Features {
	return structured.Features{
		AdminAccess:          utilfeature.DefaultFeatureGate.Enabled(features.DRAAdminAccess),
		ConsumableCapacity:   utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity),
		DeviceTaints:         utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaints),
		ListTypeAttributes:   utilfeature.DefaultFeatureGate.Enabled(features.DRAListTypeAttributes),
		PartitionableDevices: utilfeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevices),
		PrioritizedList:      utilfeature.DefaultFeatureGate.Enabled(features.DRAPrioritizedList),
	}
}

func hasDRAClaims(podTemplate *corev1.PodTemplateSpec) bool {
	return len(podTemplate.Spec.ResourceClaims) > 0
}

func buildSyntheticClaims(ctx context.Context, cl client.Client, namespace string, podTemplate *corev1.PodTemplateSpec) ([]*resourceapi.ResourceClaim, error) {
	var claims []*resourceapi.ResourceClaim
	for _, prc := range podTemplate.Spec.ResourceClaims {
		spec, err := resolveClaimSpec(ctx, cl, namespace, prc)
		if err != nil {
			return nil, fmt.Errorf("resolving claim %q: %w", prc.Name, err)
		}
		if spec == nil {
			continue
		}
		claims = append(claims, &resourceapi.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("kueue-sim-%s", prc.Name),
				Namespace: namespace,
			},
			Spec: *spec,
		})
	}
	return claims, nil
}

func resolveClaimSpec(ctx context.Context, cl client.Client, namespace string, prc corev1.PodResourceClaim) (*resourceapi.ResourceClaimSpec, error) {
	switch {
	case prc.ResourceClaimTemplateName != nil:
		var tmpl resourceapi.ResourceClaimTemplate
		if err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: *prc.ResourceClaimTemplateName}, &tmpl); err != nil {
			return nil, err
		}
		return &tmpl.Spec.Spec, nil
	case prc.ResourceClaimName != nil:
		var claim resourceapi.ResourceClaim
		if err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: *prc.ResourceClaimName}, &claim); err != nil {
			return nil, err
		}
		return &claim.Spec, nil
	default:
		return nil, nil
	}
}

func buildAllocatedState(ctx context.Context, cl client.Client) (structured.AllocatedState, error) {
	allocatedDevices := sets.New[structured.DeviceID]()
	allocatedSharedDeviceIDs := sets.New[structured.SharedDeviceID]()
	aggregatedCapacity := structured.NewConsumedCapacityCollection()
	enabledCC := utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity)

	var claimList resourceapi.ResourceClaimList
	if err := cl.List(ctx, &claimList); err != nil {
		return structured.AllocatedState{}, fmt.Errorf("listing ResourceClaims: %w", err)
	}

	for i := range claimList.Items {
		claim := &claimList.Items[i]
		if claim.Status.Allocation == nil {
			continue
		}
		for _, result := range claim.Status.Allocation.Devices.Results {
			if ptr.Deref(result.AdminAccess, false) {
				continue
			}
			deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)
			if enabledCC && result.ShareID != nil {
				sharedID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
				allocatedSharedDeviceIDs.Insert(sharedID)
				if result.ConsumedCapacity != nil {
					cc := structured.NewDeviceConsumedCapacity(deviceID, result.ConsumedCapacity)
					aggregatedCapacity.Insert(cc)
				}
				continue
			}
			allocatedDevices.Insert(deviceID)
		}
	}

	return structured.AllocatedState{
		AllocatedDevices:         allocatedDevices,
		AllocatedSharedDeviceIDs: allocatedSharedDeviceIDs,
		AggregatedCapacity:       aggregatedCapacity,
	}, nil
}

type clientDeviceClassLister struct {
	cl  client.Client
	ctx context.Context
}

func (l *clientDeviceClassLister) List() ([]*resourceapi.DeviceClass, error) {
	var list resourceapi.DeviceClassList
	if err := l.cl.List(l.ctx, &list); err != nil {
		return nil, err
	}
	result := make([]*resourceapi.DeviceClass, len(list.Items))
	for i := range list.Items {
		result[i] = &list.Items[i]
	}
	return result, nil
}

func (l *clientDeviceClassLister) Get(className string) (*resourceapi.DeviceClass, error) {
	var dc resourceapi.DeviceClass
	if err := l.cl.Get(l.ctx, client.ObjectKey{Name: className}, &dc); err != nil {
		return nil, err
	}
	return &dc, nil
}
