# KEP-8095: Stateful Inadmissibility for Reduced API Churn

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1: Mixed GPU and CPU Workloads](#story-1-mixed-gpu-and-cpu-workloads)
    - [Story 2: Large-Scale Batch Processing](#story-2-large-scale-batch-processing)
    - [Story 3: WAS Gang Scheduling Integration](#story-3-was-gang-scheduling-integration)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Current Architecture and Actual Root Cause](#current-architecture-and-actual-root-cause)
  - [Phase 1: Status and Event Throttling](#phase-1-status-and-event-throttling)
  - [Phase 2: Category-Aware Inadmissible Tracking](#phase-2-category-aware-inadmissible-tracking)
  - [Phase 3: Selective Requeue by Category](#phase-3-selective-requeue-by-category)
  - [Complete Trigger List](#complete-trigger-list)
  - [Interaction with Existing Mechanisms](#interaction-with-existing-mechanisms)
  - [Restart and HA Recovery](#restart-and-ha-recovery)
  - [API Changes](#api-changes)
  - [Changes to Metrics](#changes-to-metrics)
  - [Upgrade, Downgrade, and Backward Compatibility](#upgrade-downgrade-and-backward-compatibility)
  - [Test Plan](#test-plan)
    - [Prerequisite testing updates](#prerequisite-testing-updates)
    - [Unit Tests](#unit-tests)
    - [Integration tests](#integration-tests)
    - [Performance tests](#performance-tests)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
  - [Alternative 1: Scheduler-Level State Tracking](#alternative-1-scheduler-level-state-tracking)
  - [Alternative 2: New API Fields for Inadmissibility](#alternative-2-new-api-fields-for-inadmissibility)
  - [Alternative 3: Exponential Backoff for All Inadmissible Workloads](#alternative-3-exponential-backoff-for-all-inadmissible-workloads)
<!-- /toc -->

## Summary

Introduce throttling and selective requeue mechanisms to reduce API server load when handling inadmissible workloads.

The key insight is that Kueue's queue system is **already event-driven** via controller event handlers. The problem is that each event triggers a **blanket requeue** of ALL inadmissible workloads, even those unaffected by the change. This KEP makes requeue **selective** based on why each workload is inadmissible.

## Motivation

Performance testing with 100 GPU workloads (quota: 1) and 1000 CPU workloads revealed significant inefficiencies:

| Metric | Observed Value | Expected |
|--------|----------------|----------|
| Scheduling cycles | 5,137 in 7 minutes | N/A |
| GPU scheduling attempts | 4,137 | 100 (one per workload) |
| Status API writes | ~4,000 | ~100 |
| Events generated | 4,037 | ~100 |
| cpu+gpu runtime | 427s | ~189s (cpu-only baseline) |

Each GPU workload was processed ~40 times on average, with each processing triggering an API write and event, despite the cluster state not changing between most cycles.

### Goals

* Reduce API server writes for inadmissible workloads by 90%+ through throttling and selective requeue.
* Reduce event spam by implementing event throttling for pending workloads.
* Improve scheduling throughput for mixed workload scenarios (e.g., GPU + CPU).
* Provide a foundation for clean WAS integration (gang scheduling, DRA, TAS).
* **Enhance existing Kueue patterns** rather than introduce new abstractions.
* **Avoid new Workload API surface** - use existing conditions and mechanisms.

### Non-Goals

* Changing the fundamental queueing strategies (StrictFIFO, BestEffortFIFO).
* Implementing resource-aware queue partitioning (separate GPU/CPU queues).
* Changing preemption behavior.
* Adding new fields to WorkloadStatus (use existing conditions).
* Adding scheduler-level state that duplicates queue-level state.

## Proposal

Enhance Kueue's existing inadmissibility handling in three phases:

1. **Phase 1 (Minimal)**: Add throttling for status updates and events—skip API calls entirely when throttled
2. **Phase 2 (Category Tracking)**: Track WHY each workload is inadmissible by category, not just resources
3. **Phase 3 (Selective Requeue)**: Make `QueueInadmissibleWorkloads` selective, only requeuing workloads whose category matches the trigger

### User Stories

#### Story 1: Mixed GPU and CPU Workloads

As a platform administrator running mixed GPU/CPU workloads:
- I have 100 GPU workloads competing for 1 GPU slot
- I have 1000 CPU workloads with ample quota
- I expect CPU workloads to complete quickly without being blocked by GPU workload churn
- I expect the API server to not be overwhelmed by status updates for GPU workloads that will obviously fail

**Current behavior**: 427s to complete, 4,000+ API writes
**Expected behavior**: ~200s to complete, ~100 API writes

#### Story 2: Large-Scale Batch Processing

As a data platform team running thousands of batch jobs:
- We submit 10,000 workloads to a ClusterQueue with quota for 100 concurrent workloads
- We expect the 9,900 pending workloads to not generate continuous API churn
- We expect workloads to be re-evaluated only when quota becomes available (workloads finish)

#### Story 3: WAS Gang Scheduling Integration

As a platform administrator using gang scheduling:
- I have 50 gangs, each requiring 4 GPUs
- GPU quota allows 1 gang at a time
- I expect failed gangs to not be continuously re-evaluated
- I expect gangs to be re-evaluated only when a running gang completes

### Notes/Constraints/Caveats

**Alignment with existing Kueue patterns:**
- This enhancement builds on the existing `inadmissibleWorkloads` mechanism in `ClusterQueue`
- No new scheduler-level state that duplicates queue-level state
- No new Workload API fields - visibility through existing conditions and metrics
- Selective requeue aligns with Kubernetes controller patterns (react to what changed)

**Delayed status updates:**
- Status messages may be up to 5 seconds stale (configurable)
- Status/Reason fields remain accurate; only Message field may lag
- Events are throttled but still generated periodically

**Interaction with existing `PushOrUpdate` behavior:**
- The existing code in `ClusterQueue.PushOrUpdate()` already moves workloads from inadmissible to active when their spec changes
- This KEP complements that behavior by making cluster-event-triggered requeues selective

### Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Missed category mapping | Workload stuck inadmissible | Safety valve: periodic requeue (configurable, default 60 seconds) |
| Stale status information | User confusion | Clear documentation; accurate Status/Reason; only Message delayed |
| Incomplete trigger coverage | Some state changes not detected | Conservative category assignment; safety valve timer |
| Restart loses throttle state | Burst of updates after restart | Acceptable - throttle state rebuilds quickly; no correctness impact |

## Design Details

### Current Architecture and Actual Root Cause

The Kueue queue system is **already event-driven**. `QueueInadmissibleWorkloads` is called from event handlers in various controllers:

| Controller | Event |
|------------|-------|
| `resourceflavor_controller` | Create/Update/Delete |
| `clusterqueue_controller` | Namespace label changes |
| `admissioncheck_controller` | Create/Update/Delete |
| `tas/resource_flavor` | Topology changes |

The problem is NOT polling—it's that each of these calls triggers a **blanket requeue** of all inadmissible workloads in the affected ClusterQueues, regardless of whether the specific change affects each workload.

**Example of the problem:**

1. ClusterQueue has 100 GPU workloads inadmissible (waiting for GPU quota)
2. Operator updates the `cpu-flavor` ResourceFlavor (irrelevant to GPU workloads)
3. `resourceflavor_controller` calls `QueueInadmissibleWorkloads(ctx, cqNames)`
4. All 100 GPU workloads are moved back to the active heap
5. Scheduler pops each, fails it, patches status, records event
6. 100 unnecessary API writes

The current `QueueInadmissibleWorkloads` implementation moves ALL workloads without filtering—a CPU flavor change requeues GPU workloads, an AdmissionCheck update requeues workloads without that check, etc.

### Phase 1: Status and Event Throttling

The scheduler will track the last status update and event time per workload. Before making API calls, it checks if enough time has passed since the last update and skips the `PatchAdmissionStatus` call entirely when throttled.

**Key design points:**
- Throttle check happens BEFORE the API call, not inside callbacks
- Only skip updates when the Reason is unchanged (only Message differs)
- Default throttle duration: 5 seconds (configurable)
- Throttle state is ephemeral (in-memory only, not persisted)

The scheduler maintains two maps:
- `lastStatusUpdate`: tracks last update time and reason per workload
- `lastEventTime`: tracks last event time per workload

When a workload fails admission:
1. Check if within throttle window AND same reason → skip status update
2. Check if within event throttle window → skip event recording
3. Increment `kueue_status_updates_skipped_total` metric when skipped

### Phase 2: Category-Aware Inadmissible Tracking

The existing `inadmissibleWorkloads` map in `ClusterQueue` will be enhanced to track WHY each workload is inadmissible.

**Inadmissibility Categories:**

| Category | Description | Trigger Events |
|----------|-------------|----------------|
| `QuotaInsufficient` | Resource quota exhausted | Workload finish, CQ quota update, RF update |
| `AdmissionCheckPending` | Waiting for admission check | AdmissionCheck update |
| `TASConstraints` | Topology constraints not met | TAS topology changes |
| `NamespaceMismatch` | Namespace selector doesn't match | Namespace label changes |
| `PreemptionPending` | Waiting for preemption to complete | Preemption completion |
| `ProvisioningPending` | Waiting for provisioning request | ProvisioningRequest ready |
| `FlavorUnavailable` | Required flavor doesn't exist | ResourceFlavor create |
| `BorrowingExceeded` | Would exceed borrowing limit | Cohort changes |
| `Unknown` | Category not determined | Any event (safety fallback) |

**Category Details:**

For categories that need specificity (e.g., which flavor, which admission check), additional details are stored:
- Resources: which resources caused quota failure
- Flavors: which flavors were tried
- AdmissionChecks: which checks are pending
- TopologyDomain: which topology domain failed

The scheduler determines the category when requeuing a workload based on **structured sources first**, with message parsing used only as a fallback:
1. The requeue reason enum (e.g., `RequeueReasonPendingPreemption`) and admission-check state
2. The last assignment state (e.g., PendingFlavors, borrowing, preemption)
3. The inadmissible message content (fallback only, not primary)

### Phase 3: Selective Requeue by Category

Replace the blanket `QueueInadmissibleWorkloads` with category-aware selective requeue.

**New queue manager methods:**

- `QueueInadmissibleByCategory(category, details, cqNames)`: Only requeues workloads matching the specified category and details
- `QueueInadmissibleExpired(maxDuration)`: Requeues workloads past the safety timeout

**Selective requeue logic:**

1. `Unknown` category workloads always get requeued (safety behavior)
2. For other categories, category must match the trigger
3. For categories with details, check overlap (e.g., same flavor, same admission check)

**Event handler updates:**

Each controller event handler will specify the category when triggering requeue:
- `resourceflavor_controller`: `QuotaInsufficient` with flavor name
- `admissioncheck_controller`: `AdmissionCheckPending` with check name
- `clusterqueue_controller` (namespace changes): `NamespaceMismatch`
- etc.

### Complete Trigger List

The following events trigger selective re-evaluation of inadmissible workloads:

| Trigger | Category | Details |
|---------|----------|---------|
| **Workload finishes/evicted** | QuotaInsufficient | Resources+Flavors used by finished workload |
| **Workload spec changes** | N/A | Handled by existing `PushOrUpdate` |
| **Workload priority changes** | N/A | Handled by existing `PushOrUpdate` |
| **ClusterQueue quota updated** | QuotaInsufficient | Changed resources |
| **ClusterQueue spec changes** | Multiple | Mapped by field changed (see below) |
| **ResourceFlavor updated** | QuotaInsufficient | That flavor |
| **ResourceFlavor created** | FlavorUnavailable | That flavor |
| **LocalQueue changes** | NamespaceMismatch | - |
| **Cohort borrowing changes** | BorrowingExceeded | - |
| **Admission check becomes Ready** | AdmissionCheckPending | That check |
| **TAS topology changes** | TASConstraints | That topology domain |
| **Preemption completes** | PreemptionPending | - |
| **Provisioning request ready** | ProvisioningPending | - |
| **Node capacity changes** | QuotaInsufficient | Affected resources (via node/cluster inventory updates) |
| **Namespace LimitRange/ResourceQuota** | QuotaInsufficient | Affected resources (via namespace-level controllers) |
| **Safety timer expires** | All (per workload) | Periodic check (default 60s) |

**Trigger sources/ownership (implementation detail):**
- Node capacity changes: emitted by scheduler cache/resource inventory updates.
- Namespace LimitRange/ResourceQuota: emitted by namespace-level controllers that already watch these resources.
- If a trigger source is not available yet, default to `Unknown` for that path (correctness first).

**Note:** Node capacity and namespace LimitRange/ResourceQuota triggers are follow-up work required for Phase 3 completeness. They are not blockers for alpha/beta graduation, as `Unknown` fallback ensures correctness. These triggers should be implemented before marking the feature stable.

### Interaction with Existing Mechanisms

**Existing `PushOrUpdate` behavior is preserved:**

The existing code in `ClusterQueue.PushOrUpdate()` already handles workload spec changes by moving workloads from inadmissible to active when their spec, ReclaimablePods, or Evicted/Requeued conditions change. This KEP **complements** this behavior:
- `PushOrUpdate` handles workload-initiated changes (user updates spec)
- Selective requeue handles cluster-initiated changes (quota freed, flavor updated)

**Existing `backoffWaitingTimeExpired` is preserved:**

The existing backoff mechanism for PodsReady evictions (from KEP-1282) continues to work. Workloads with `RequeueState.RequeueAt` in the future stay inadmissible. Category-based requeue respects this by checking `backoffWaitingTimeExpired` before moving to heap.

### Restart and HA Recovery

**Throttle state is scheduler-local and ephemeral:**
- `lastStatusUpdate` and `lastEventTime` maps are in-memory only
- On restart, these maps are empty, causing a brief burst of updates
- Maps rebuild naturally as workloads are processed
- This is acceptable—correctness is preserved, only efficiency is temporarily reduced

**Queue-level state (inadmissibleWorkloads) is rebuilt on startup:**

On startup, the queue manager rebuilds inadmissible state from workloads by examining the `QuotaReserved` condition. Workloads with `QuotaReserved=False` and `Reason=Pending` are placed in the inadmissible set.

The category is inferred from structured data where possible (requeue reason, admission check state, last assignment). When the category cannot be determined, `Unknown` is used, which causes the workload to be requeued on ANY cluster event (conservative but correct).

**Key semantics for Unknown category:**
- `Unknown` workloads are requeued on ANY cluster event
- This ensures correctness at the cost of some efficiency
- Over time, as workloads cycle through scheduler, they get proper categories
- The safety timer (60s default) also ensures Unknown workloads don't wait forever
- If `Unknown` requeues prove noisy after restart (e.g., large clusters with frequent events), a follow-up optimization could rate-limit `Unknown` requeues to at most once per safety-valve interval

**ClusterQueue spec change mapping (non-exhaustive):**
- ResourceGroup/Flavor quota change → `QuotaInsufficient` (resources in diff)
- Cohort/borrowing policy change → `BorrowingExceeded`
- NamespaceSelector change → `NamespaceMismatch`
- AdmissionChecks change → `AdmissionCheckPending`
- Queueing strategy change → conservative `Unknown` (re-evaluate once)

### API Changes

**No API changes in alpha.**

This KEP deliberately avoids adding new fields to KueueConfig or Workload API:

- **KueueConfig**: The Configuration struct is already in beta. Adding new fields for tuning parameters is disruptive and may not be necessary. We use hardcoded defaults for alpha and evaluate whether configuration is needed based on operational experience.

- **Workload API**: Visibility is provided through existing conditions and new metrics, not new status fields.

**Hardcoded defaults for alpha:**

| Parameter | Default | Rationale |
|-----------|---------|-----------|
| Status update throttle | 5s | Balances freshness vs API load |
| Event throttle | 5s | Same as status throttle for consistency |
| Safety valve (max inadmissible duration) | 60s | Short enough to catch missed triggers, long enough to reduce churn |

**Future configuration (beta, if needed):**

If operational experience shows that different clusters need different tuning, configuration options can be added in beta. Candidates include:
- Command-line flags on controller-manager (e.g., `--status-update-throttle=5s`)
- Extension of existing KueueConfig sections (if a natural fit exists)
- Per-ClusterQueue annotations (if per-queue tuning is needed)

### Changes to Metrics

New metrics to monitor the effectiveness of throttling and selective requeue:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `kueue_inadmissible_workloads` | Gauge | `cluster_queue`, `category` | Workloads in inadmissible set per CQ and category |
| `kueue_status_updates_total` | Counter | `cluster_queue` | Total status updates |
| `kueue_status_updates_skipped_total` | Counter | `cluster_queue` | Throttled/skipped updates |
| `kueue_selective_requeue_total` | Counter | `cluster_queue`, `category` | Requeues by trigger category |
| `kueue_blanket_requeue_total` | Counter | `cluster_queue` | Unknown category requeues (should decrease over time) |

### Upgrade, Downgrade, and Backward Compatibility

**Upgrade path:**

1. **Feature gate disabled (default in alpha):** No behavior change. Existing blanket requeue behavior continues.

2. **Feature gate enabled:** New category tracking activates. Existing inadmissible workloads get `Unknown` category initially. `Unknown` workloads requeue on any event (same as before). Over time, as workloads cycle through scheduler, they get proper categories.

**Downgrade path:**

When feature gate is disabled after being enabled:
- Category information is lost (in-memory only)
- Falls back to blanket requeue behavior
- No correctness issues—only efficiency regression
- Workload conditions are unchanged, so no API migration needed

**In-flight workloads:**

- Workloads currently in `inadmissibleWorkloads` map get `Unknown` on feature enable
- They'll be requeued on next cluster event
- After re-evaluation, they get proper categories
- No workloads get stuck

### Test Plan

[X] I/we understand the owners of the involved components may require updates to
existing tests to make this code solid enough prior to committing the changes necessary
to implement this enhancement.

##### Prerequisite testing updates

- Add benchmarks for current scheduler behavior under high contention
- Add metrics collection for API write rates

#### Unit Tests

- `pkg/scheduler`: Test throttle check happens BEFORE API call
- `pkg/scheduler`: Test category determination logic
- `pkg/cache/queue`: Test category-based selective requeue
- `pkg/cache/queue`: Test Unknown category always requeues
- `pkg/cache/queue`: Test safety valve timeout
- `pkg/cache/queue`: Test interaction with existing `PushOrUpdate`

Coverage targets:
- `pkg/scheduler`: Current ~45% → Target 55%
- `pkg/cache/queue`: Current ~34% → Target 45%

#### Integration tests

- Test mixed GPU/CPU workload scenario (100 GPU + 1000 CPU)
- Verify API write reduction (measure resourceVersion changes)
- Verify event throttling (count events generated)
- Test ResourceFlavor update only requeues affected workloads
- Test AdmissionCheck update only requeues workloads with that check
- Test Unknown category workloads requeue on any event
- Test safety valve (max inadmissible duration)
- Test restart recovery (rebuild from workload conditions)
- Test upgrade path (existing inadmissible → Unknown → proper category)

#### Performance tests

Add performance test case based on issue #8095 reproducer:
- 100 GPU workloads (quota: 1)
- 1000 CPU workloads (quota: 600)
- Measure: total runtime, API writes, events, scheduling cycles

Success criteria:
- Runtime reduction: 427s → <250s
- API writes reduction: 4000 → <500
- Events reduction: 4000 → <500

### Graduation Criteria

**Alpha (feature gate: StatefulInadmissibility)**
- Phase 1 (throttling) implemented with BEFORE-API-call check
- Phase 2 (category tracking) implemented
- Unit and integration tests passing
- Performance tests showing expected improvement

**Beta**
- Phase 3 (selective requeue) implemented
- Feature gate enabled by default
- All category handlers implemented
- Metrics available for monitoring
- Upgrade/downgrade tested

**Stable**
- Feature gate removed
- Stable for 2+ releases
- Adopted by multiple production users
- Documentation complete

## Implementation History

- 2025-01: Issue #8095 opened identifying API churn problem
- 2025-01: Initial analysis and PR #8139 for throttling
- 2025-01: This KEP proposed

## Drawbacks

* **Additional complexity in queue manager**: Category tracking adds complexity
* **Incomplete category coverage**: If category can't be determined, falls back to Unknown (blanket behavior)
* **Delayed status updates**: Users may see slightly stale status messages (5s default)
* **Category inference on restart**: May not perfectly reconstruct categories from condition messages

## Alternatives

### Alternative 1: Scheduler-Level State Tracking

**Description**: Add scheduler-level state to track cluster state hashes and inadmissible workloads.

**Pros**:
- Centralized state management
- Clear separation from queue logic

**Cons**:
- Duplicates existing `inadmissibleWorkloads` in queue
- New state to manage, clean up, and recover on restart
- Adds complexity to scheduler hot path
- Not aligned with existing Kueue patterns

**Why not chosen**: Violates principle of enhancing existing patterns; duplicates state.

### Alternative 2: New API Fields for Inadmissibility

**Description**: Add `WorkloadStatus.InadmissibleCategory` to track inadmissibility category.

**Pros**:
- Explicit visibility into inadmissibility category
- Survives restarts perfectly
- Rich information available to users

**Cons**:
- Expands API surface
- More API writes (to update category field)
- May expose internal implementation details

**Why not chosen**: Existing conditions already provide sufficient visibility; adding API fields creates more churn, not less.

### Alternative 3: Exponential Backoff for All Inadmissible Workloads

**Description**: Apply exponential backoff to all inadmissible workloads, not just PodsReady evictions.

**Pros**:
- Uses existing mechanism
- Simple to implement

**Cons**:
- Adds latency even when resources become available
- Not responsive to cluster state changes
- May delay legitimate admissions significantly

**Why not chosen**: Category-based selective requeue is more responsive and precise.
