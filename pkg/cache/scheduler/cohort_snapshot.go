/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	"sigs.k8s.io/kueue/pkg/resources"
)

type CohortSnapshot struct {
	Name kueue.CohortReference

	ResourceNode resourceNode
	hierarchy.Cohort[*ClusterQueueSnapshot, *CohortSnapshot]

	FairWeight float64

	// cachedLendable stores precomputed lendable resources for the root cohort.
	// This is computed once during snapshot creation and used to avoid
	// repeated tree traversals in calculateLendable.
	cachedLendable map[corev1.ResourceName]int64
}

func (c *CohortSnapshot) GetName() kueue.CohortReference {
	return c.Name
}

// Root returns the root of the Cohort Tree. It expects that no cycles
// exist in the Cohort graph.
func (c *CohortSnapshot) Root() *CohortSnapshot {
	if !c.HasParent() {
		return c
	}
	return c.Parent().Root()
}

// SubtreeClusterQueues returns all of the ClusterQueues in the
// subtree starting at the given Cohort. It expects that no cycles
// exist in the Cohort graph.
func (c *CohortSnapshot) SubtreeClusterQueues() []*ClusterQueueSnapshot {
	return c.subtreeClusterQueuesHelper(make([]*ClusterQueueSnapshot, 0, c.subtreeClusterQueueCount()))
}

func (c *CohortSnapshot) subtreeClusterQueuesHelper(cqs []*ClusterQueueSnapshot) []*ClusterQueueSnapshot {
	cqs = append(cqs, c.ChildCQs()...)
	for _, cohort := range c.ChildCohorts() {
		cqs = cohort.subtreeClusterQueuesHelper(cqs)
	}
	return cqs
}

func (c *CohortSnapshot) subtreeClusterQueueCount() int {
	count := len(c.ChildCQs())
	for _, cohort := range c.ChildCohorts() {
		count += cohort.subtreeClusterQueueCount()
	}
	return count
}

func (c *CohortSnapshot) DominantResourceShare() DRS {
	return dominantResourceShare(c, nil)
}

// implement flatResourceNode/hierarchicalResourceNode interfaces

func (c *CohortSnapshot) getResourceNode() resourceNode {
	return c.ResourceNode
}

func (c *CohortSnapshot) parentHRN() hierarchicalResourceNode {
	return c.Parent()
}

// Implements dominantResourceShareNode interface.

func (c *CohortSnapshot) fairWeight() float64 {
	return c.FairWeight
}

func (c *CohortSnapshot) BorrowingWith(fr resources.FlavorResource, val int64) bool {
	return c.ResourceNode.Usage[fr]+val > c.ResourceNode.SubtreeQuota[fr]
}

// PrecomputeLendable caches lendable resources for root cohorts without borrowing/lending limits.
func (c *CohortSnapshot) PrecomputeLendable() {
	if c.HasParent() {
		// Only root cohorts should cache lendable
		return
	}
	if c.hasBorrowingOrLendingLimitsInSubtree() {
		return
	}
	c.cachedLendable = make(map[corev1.ResourceName]int64, len(c.ResourceNode.SubtreeQuota))
	for fr, quota := range c.ResourceNode.SubtreeQuota {
		c.cachedLendable[fr.Resource] += quota
	}
}

// hasBorrowingOrLendingLimitsInSubtree checks if any node in the subtree has borrowing or lending limits configured.
func (c *CohortSnapshot) hasBorrowingOrLendingLimitsInSubtree() bool {
	// Check this cohort's quotas
	for _, quota := range c.ResourceNode.Quotas {
		if quota.BorrowingLimit != nil || quota.LendingLimit != nil {
			return true
		}
	}
	// Check child cohorts
	for _, child := range c.ChildCohorts() {
		if child.hasBorrowingOrLendingLimitsInSubtree() {
			return true
		}
	}
	// Check child ClusterQueues
	for _, cq := range c.ChildCQs() {
		for _, quota := range cq.ResourceNode.Quotas {
			if quota.BorrowingLimit != nil || quota.LendingLimit != nil {
				return true
			}
		}
	}
	return false
}

// GetCachedLendable returns cached lendable resources, or nil if unavailable.
func (c *CohortSnapshot) GetCachedLendable() map[corev1.ResourceName]int64 {
	return c.cachedLendable
}
