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

package queue

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// inadmissibleWorkloads is a thin wrapper around a map to encapsulate
// operations on inadmissible workloads and prevent direct map access.
type inadmissibleWorkloads map[workload.Reference]*inadmissibleEntry

// InadmissibleCategory identifies why a workload could not be admitted.
// Used for selective requeue when cluster conditions change.
type InadmissibleCategory string

const (
	InadmissibleUnknown              InadmissibleCategory = "Unknown"
	InadmissibleQuotaInsufficient    InadmissibleCategory = "QuotaInsufficient"
	InadmissibleAdmissionCheck       InadmissibleCategory = "AdmissionCheckPending"
	InadmissibleTASConstraints       InadmissibleCategory = "TASConstraints"
	InadmissibleNamespaceMismatch    InadmissibleCategory = "NamespaceMismatch"
	InadmissibleClusterQueueInactive InadmissibleCategory = "ClusterQueueInactive"
	InadmissibleClusterQueueNotFound InadmissibleCategory = "ClusterQueueNotFound"
	InadmissibleInvalidResources     InadmissibleCategory = "InvalidResources"
	InadmissibleLimitRangeViolation  InadmissibleCategory = "LimitRangeViolation"
	InadmissiblePreemptionPending InadmissibleCategory = "PreemptionPending"
	InadmissibleFlavorUnavailable InadmissibleCategory = "FlavorUnavailable"
	InadmissibleFlavorUnsuitable  InadmissibleCategory = "FlavorUnsuitable"
)

// InadmissibleDetails contains the specific resources, flavors, or checks
// that caused a workload to be inadmissible. Used to match workloads
// for selective requeue when those specific conditions change.
type InadmissibleDetails struct {
	Resources       sets.Set[corev1.ResourceName]
	Flavors         sets.Set[kueue.ResourceFlavorReference]
	AdmissionChecks sets.Set[kueue.AdmissionCheckReference]
	TopologyName    *kueue.TopologyReference
}

type inadmissibleEntry struct {
	info           *workload.Info
	category       InadmissibleCategory
	details        InadmissibleDetails
	inadmissibleAt time.Time
}

// get retrieves a workload from the inadmissible workloads map.
// Returns the workload if it exists, otherwise returns nil.
func (iw inadmissibleWorkloads) get(key workload.Reference) *inadmissibleEntry {
	return iw[key]
}

// delete removes a workload from the inadmissible workloads map.
func (iw inadmissibleWorkloads) delete(key workload.Reference) {
	delete(iw, key)
}

// insert adds a workload to the inadmissible workloads map.
func (iw inadmissibleWorkloads) insert(key workload.Reference, entry *inadmissibleEntry) {
	iw[key] = entry
}

// len returns the number of inadmissible workloads.
func (iw inadmissibleWorkloads) len() int {
	return len(iw)
}

// empty returns true if there are no inadmissible workloads.
func (iw inadmissibleWorkloads) empty() bool {
	return len(iw) == 0
}

// forEach iterates over all inadmissible workloads and calls the provided function.
// The iteration can be stopped early by returning false from the function.
func (iw inadmissibleWorkloads) forEach(f func(key workload.Reference, entry *inadmissibleEntry) bool) {
	for key, entry := range iw {
		if !f(key, entry) {
			return
		}
	}
}

// replaceAll replaces all inadmissible workloads with the provided map.
func (iw *inadmissibleWorkloads) replaceAll(newMap map[workload.Reference]*inadmissibleEntry) {
	*iw = newMap
}
