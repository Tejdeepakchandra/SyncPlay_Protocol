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
- `docs/PHASE7_2_R5_FINAL_CORRECTIONS.md`: Definitive authoritative architectural correction register (P7.2-R5) resolving exact logical timestamp validation across 3 storage tiers, anomalous timestamp jump handling, strict `LOCAL_REPLAY` contracts, collision vs duplicate detection invariants, and final specification freeze.
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

Use the consistency review and interface contracts as cross-module references; module LLDs provide task-specific detail.
