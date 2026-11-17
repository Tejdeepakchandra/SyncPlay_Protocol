# Design Documents Index

| Document | Use it for |
|---|---|
| `MASTER_HLD.md` | Overall architecture, component responsibilities, data/control flow. |
| `BOOTSTRAP_SERVER_LLD.md` | Room lifecycle, membership, signaling, validation, rejoin. |
| `PEER_MESH_RTT_ENGINE_LLD.md` | WebRTC peer connections, DataChannels, connection lifecycle, RTT measurement. |
| `CONTROL_EVENTS_LAYER_LLD.md` | Typed playback events, validation, deduplication, dispatch. |
| `FAULT_TOLERANCE_LLD.md` | Failure suspicion, retries, reconnection, recovery orchestration. |
| `SYNCENGINE_INTERFACE_RESEARCH_SPEC.md` | Replaceable SyncEngine interface, state semantics and research questions. |
| `MASTER_INTERFACE_CONSISTENCY_REVIEW.md` | Cross-module contract review and integration sequence. |
| `ARCHITECTURE_STRATEGY.md` | Architecture principles before implementation. |
| `INITIAL_BOOTSTRAP_SERVER_SYSTEM_DESIGN.md` | Early bootstrap design background. |
| `PATENT_AND_PRIOR_ART_RESEARCH_GUIDE.md` | Research approach and prior-art/patent review. |
| `WEEKLY_DISTRIBUTED_MEDIA_SYNC_READING_GUIDE.md` | Reading plan and technical learning. |

## Phase 7 — Leaderless Distributed Synchronization Engine
The master planning suite for Phase 7 is located at the root of the `docs/` directory:
- `docs/PHASE7_MASTER_ROADMAP.md`: Master milestone structure (P7.1–P7.7), critical paths, and execution strategy.
- `docs/PHASE7_ARCHITECTURAL_PRINCIPLES.md`: Non-negotiable architectural principles, server-minimized invariants, and system invariants.
- `docs/PHASE7_MILESTONE_ACCEPTANCE_CRITERIA.md`: Granular, testable pass/fail acceptance criteria for every Phase 7 milestone.
- `docs/PHASE7_RESEARCH_QUESTIONS.md`: Comprehensive theoretical analysis of distributed ordering, convergence, candidate algorithms, and academic literature.
- `docs/PHASE7_RISK_REGISTER.md`: Architectural and technical risk register, failure modes, triggers, and contingency plans.
- `docs/PHASE7_DEPENDENCY_AND_DELIVERY_PLAN.md`: Dependency graph, upstream inputs, governance review gates, and transition plan to Phase 8.

### Milestone P7.1 Research & Formal Problem Specifications
- `docs/PHASE7_1_DISTRIBUTED_SYNC_PROBLEM.md`: Formal problem specification across 12 concurrent, delayed, and failure scenarios.
- `docs/PHASE7_1_CONSISTENCY_MODEL.md`: Formal consistency model (SEC under quiescence), network model, and CAP theorem analysis.
- `docs/PHASE7_1_ALGORITHM_EVALUATION.md`: Multi-dimensional evaluation of 8 candidate distributed synchronization paradigms.
- `docs/PHASE7_1_EVENT_ORDERING_RESEARCH.md`: Deep investigation of Lamport logical clocks, strict total order proofs, and integer safety.
- `docs/PHASE7_1_ANTI_ENTROPY_RESEARCH.md`: Multi-origin reconciliation, version vectors, bounded history horizon ($K=256$), and stable frontier compaction.
- `docs/PHASE7_1_CONFLICT_SEMANTICS.md`: Operation-specific conflict semantics for `PLAY`, `PAUSE`, `SEEK`, and `RATE_CHANGE`.
- `docs/PHASE7_1_ARCHITECTURE_RECOMMENDATION.md`: Proposed modular architecture (`LeaderlessSyncEngine`) and component responsibility matrix.
- `docs/PHASE7_1_FINAL_RECOMMENDATION.md`: Answers to 10 core architectural decisions, alternative rejections, and P7.2 scope.

### Milestone P7.2 Architecture & Interface Specifications
- `docs/PHASE7_2_R9_FINAL_CORRECTIONS.md`: Authoritative architectural correction register and definitive specification freeze (P7.2-R9) correcting multi-event Lamport validation ($A_{11} \to 14, C_1 \to 15$), formalizing component-wise causal frontier merging, establishing receiver-only concurrent observation isolation, detailing missing-dependency buffering and repair handshakes, and defining Suite 13 simulation tests.
- `docs/PHASE7_2_R8_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R8) correcting receive-side clock semantics to $L_{\text{local}} = \max(L_{\text{local}}, L_{\text{received}})$ (WITHOUT $+1$), reconciling local event creation ($L(e) = 1 + L_{\text{local}} \equiv 1 + \max_{(p, s) \in e.\text{causalContext}} L(p, s)$), and defining Suite 12 simulation tests (refined by R9).
- `docs/PHASE7_2_R7_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R7) reconciling snapshot import with the exact Lamport creation invariant ($L(e) \equiv 1 + \max L(p, s)$), eliminating the off-by-one double increment defect via exact reseeding ($L_{\text{local}} = \text{maxLogicalTimestamp}$ without $+1$), proving `maxLogicalTimestamp` equivalence to $\max_p \text{originFrontierTimestamps}[p]$, and defining Suite 11 simulation tests (refined by R8).
- `docs/PHASE7_2_R6_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R6) enforcing exact Lamport timestamp creation invariants ($L(e) \equiv \text{expectedTimestamp}(e)$), bounded sequence-indexed checkpoints ($K=256$) for arbitrary compacted dependencies, non-substitution invariants for `maxLogicalTimestamp`, and 9 simulation specification test cases (refined by R7).
- `docs/PHASE7_2_R5_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R5) resolving exact logical timestamp validation across 3 storage tiers, anomalous timestamp jump handling, strict `LOCAL_REPLAY` contracts, and collision vs duplicate detection invariants (superseded where amended by R6).
- `docs/PHASE7_2_R4_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R4) resolving historical event trust (Cooperative Non-Byzantine Model Option C), snapshot semantic boundaries (canonical digest preimage & non-proof of semantic truth), tightened 5-category event validation rules (`ValidationContext`), and revised convergence preconditions.
- `docs/PHASE7_2_R3_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R3) resolving snapshot safety (elimination of arbitrary tie-breaking), transport vs origin identity (Strict No-Forwarding policy), implementable token-bucket Lamport clock validation, and conditional eventual convergence theorem.
- `docs/PHASE7_2_R2_FINAL_CORRECTIONS.md`: Architectural correction register (P7.2-R2) resolving causal-context vs replication vectors, strict epoch fencing, RFC 8785 snapshot integrity, and exact Lamport rules (superseded where amended).
- `docs/PHASE7_2_ARCHITECTURAL_CORRECTIONS.md`: Initial architectural correction register (P7.2-R1).
- `docs/PHASE7_2_HLD.md`: High-Level Design for `LeaderlessSyncEngine`, trust boundaries, data flows, and module responsibilities.
- `docs/PHASE7_2_LLD.md`: Low-Level Design covering ordering keys, conflict policies, bounded ring buffer ($K=256$), and anti-entropy version vectors.
- `docs/PHASE7_2_PROTOCOL_EVOLUTION_RFC.md`: Wire protocol schemas for `logicalTimestamp`, `STATE_DIGEST`, `REPAIR_REQUEST`, and `OPERATION_BATCH`.
- `docs/design/PHASE7_INTERFACE_CONTRACTS.md`: Formal TypeScript interface contracts for engine components and public compatibility.
- `docs/PHASE7_2_SIMULATION_TEST_STRATEGY.md`: Headless deterministic simulation harness with virtual clock, seedable PRNG, and network impairment injection.
- `docs/PHASE7_2_COMPATIBILITY_AND_MIGRATION.md`: Package compatibility, wire version migration, snapshot migration, and zero-risk rollback plan.
- `docs/PHASE7_2_FINAL_DESIGN_REVIEW.md`: Final design review, decisions summary, AC-7.2.1–7.2.5 assessment, and implementation sequence for P7.3+.

### Milestone P7.3 Implementation Reports & Closure
- `docs/reports/PHASE7_3_COMPLETION_REPORT.md`: Baseline implementation of the Event Ordering & Conflict Engine (P7.3).
- `docs/reports/PHASE7_3_R1_ARCHITECTURAL_AUDIT_REPORT.md`: P7.3-R1 audit findings and corrections.
- `docs/reports/PHASE7_3_R2_FINAL_BOUNDARY_VERIFICATION.md`: Resource bounds and epoch fencing closure.
- `docs/reports/PHASE7_3_R3_FINAL_IDENTITY_AUTHORIZATION_AUDIT.md`: Immutable operation fingerprinting and epoch transition semantics.
- `docs/reports/PHASE7_3_R4_FINAL_TRUST_BOUNDARY_VERIFICATION.md`: Transport supplier authorization and RFC 8785 canonicalization.
- `docs/reports/PHASE7_3_R5_FINAL_AUTHORIZATION_CANONICALIZATION_CLOSURE.md`: Capability-based epoch transition authorization.
- `docs/reports/PHASE7_3_R5F_FINAL_CAPABILITY_AND_TRANSPORT_CLOSURE.md`: Exact referential capability identity and transport context enforcement.
- `docs/reports/PHASE7_3_R5G_FINAL_SUPPLIER_AUTHORIZATION_CLOSURE.md`: Elimination of implicit supplier generation defaulting (`?? 1`).
- `docs/reports/PHASE7_3_R5H_FINAL_TRANSPORT_CONTEXT_ISSUER_CLOSURE.md`: Transport registration capability gating and formal Phase 7.3 freeze.

### Milestone P7.4 State Replication & Anti-Entropy (Planning & Specification)
- `docs/PHASE7_4_ARCHITECTURE.md`: High-level and low-level architecture for decentralized anti-entropy, replication knowledge model, vector comparison, missing range detection, supplier selection, and multi-peer convergence dynamics.
- `docs/PHASE7_4_INTERFACE_SPECIFICATION.md`: Wire protocol contracts (`STATE_DIGEST`, `REPAIR_REQUEST`, `OPERATION_BATCH`, `REPAIR_REJECTED`), SyncEngine subsystem interfaces (`IAntiEntropyManager`), and transport adapter binding.
- `docs/PHASE7_4_IMPLEMENTATION_ROADMAP.md`: Five-stage staged delivery plan (M1–M5), file-by-file changes, verification gates, and non-regression safeguards.
- `docs/PHASE7_4_ACCEPTANCE_CRITERIA.md`: Seven levels of state agreement, 10 mandatory acceptance criteria (AC-1–AC-10), performance thresholds, and test verification matrix.
- `docs/PHASE7_4_RISK_REGISTER.md`: Risk inventory (amplification storms, silent forks, stale replay, memory bloat, churn, buffer starvation), trigger conditions, automated mitigations, and telemetry counters.

Use the consistency review and interface contracts as cross-module references; module LLDs provide task-specific detail.
