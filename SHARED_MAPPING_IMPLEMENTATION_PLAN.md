# Shared fixture mapping implementation plan

Extract the reusable engine from the current football working tree. Football is the first caller and the behavior reference. Basketball will adopt that workflow and supply its own US alias identities, timestamp precision, scoring periods, and provider adapters. Follow the repository boundaries in `COMMON_EXTRACTION_PLAN.md`.

Companion plans are in `../poisson_football_rust/mapping/shared_mapping_implementation_plan.md` and `../poisson_basketball_rust/mapping/shared_mapping_implementation_plan.md` relative to the utilities repo root.

## 1. Define the caller boundary

- Add a focused fixture mapping module. Design the interface with both callers, using a small borrowed fixture view and focused policy inputs where practical. Share the outer execution loop. Keep provider source enums, queries, alias tables, and sport-specific period types in the callers.
- Provide the engine with raw fixture identity, team and context IDs, normalized seed identities, kickoff data and precision, period and scores, and caller-supplied fingerprints or serializable fingerprint inputs. Support provider-local IDs and optional canonical seed/context identities separately. Keep canonical matching identities independent of published source keys.
- Allow callers to supply the rules for candidate windows, score and period compatibility, reversed home/away evidence, and timestamp precision. Include every policy input that can affect a cached decision in the caller's fingerprint or cache version. Keep the API as small as the two callers allow.

## 2. Extract the football matcher and assessment cache

- Use football's `mapping/src/matching.rs` and `incremental.rs` as the source. Move duplicate identity handling, candidate indexing, bounded time lookup, relationship learning and conflict handling, tier ranking and mutual uniqueness, selective reassessment, and the in-memory assessment cache.
- Preserve deterministic ordering, the inclusive search window, tie handling, and the distinction between provisional learning evidence and published matches. Re-evaluate global learning and uniqueness even when individual assessments are reused. Ensure changes to candidate eligibility or quarantine schedule the affected fixtures for reassessment.
- Preserve the SHA-256 representation and the existing ordered edge checksum. Support football's current assessment-key format without forcing a cache rebuild. Keep typed ClickHouse assessment queries in the caller for the first extraction. Expose the cache data needed to save changed assessments and validate them on reload.
- Move the existing name normalization and season suffix trimming helpers with their tests. Keep provider-specific name defaults and alias policies with their callers.

## 3. Share lifecycle code after both callers are wired

- Start with pure publication change detection and the orientation-aware registry merge logic once both integrations expose the same contracts. Preserve raw identity on withdrawal and reactivation. Make the component merge consume caller-supplied score and timestamp rules.
- Examine generation-scoped evidence validation and run completion after basketball has adopted the football workflow. Move code where both callers can use the same contract. Keep provider queries, table schemas, central ID policy, and CLI orchestration in the caller until that boundary is demonstrated.
- Reuse the existing `io::click_house` helpers for `FINAL`, table exchange, and batched inserts where applicable. Build new SQL with SeaQuery where it supports the required operation.

## Acceptance checks

- Use synthetic input to test duplicate IDs, mutual best-tier ties, conflict quarantine, reversed home/away, strict anchored learning, and exact versus date-only timestamps. Verify that selective reassessment and cache reuse agree with full replay after relevant changes.
- Use football regression data and compatibility checks owned by the football repo to verify equivalent results and cache bytes through the new API. Use the basketball integration to verify canonical seed handling and alias-driven invalidation. Keep sports-specific fixtures in their respective repos.
- Run focused utility tests and lints, then check both callers against the same sibling utilities checkout. Report which APIs both callers use and which lifecycle pieces still live in the callers.
