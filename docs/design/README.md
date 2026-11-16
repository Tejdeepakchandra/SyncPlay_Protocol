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

Use the consistency review and interface contracts as cross-module references; module LLDs provide task-specific detail.
