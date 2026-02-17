# Expert Store Spec (Repo-Wide)

## Purpose
Define a stable, repo-wide contract for expert hat memory so all hats remain consistent, foundational, and non-breaking. This spec ensures:
- Foundational memory behavior is uniform across hats.
- Expert-specific data can be layered safely.
- Context and chat memory are not broken by hat enhancements.

## Scope
- Applies to all hats and all projects.
- Covers global expert data only (stable, serious facts).
- Project-specific data remains in project state and artifacts.

## Foundational Principles
- Foundational memory is always available and consistent across hats.
- Expert memory is layered on top of foundational memory, never replaces it.
- Expert memory is injected ONLY when its hat is active.
- All canonical writes must go through canonical write paths.
- No hat can bypass or override foundational memory rules.

## Storage Locations (Global Expert Store)
Global expert memory lives under:
- `projects/<user>/_user/experts/`

Files:
- `projects/<user>/_user/experts/health_profile.json` (active now)
- `projects/<user>/_user/experts/therapist_profile.json` (scaffold only)
- `projects/<user>/_user/experts/coding_profile.json` (scaffold only)
- `projects/<user>/_user/experts/analysis_profile.json` (scaffold only)
- `projects/<user>/_user/experts/general_profile.json` (scaffold only)

Only Health is active for now. Others are reserved scaffolds and remain empty until enabled.

## Schemas
Each expert profile uses a versioned schema and must be a JSON object.

Example base shape:
```
{
  "schema": "expert_profile_v1",
  "expert": "health",
  "updated_at": "<iso8601>",
  "data": {}
}
```

Health can keep its specialized schema for now, but should still include:
- `schema`
- `expert`
- `updated_at`

## Injection Rules
Foundational injection (always, every hat):
- Tier-2G global profile (identity kernel)
- Tier-2M global facts subset
- Project canonical facts (facts_map, project_state, etc.)

Expert injection (only when hat active):
- If active hat == `health`, inject global `health_profile.json`.
- Other expert profiles are not injected until explicitly enabled.

## Write Rules
- Expert memory writes must be canonical and deterministic.
- No hat writes directly to expert files; hats request promotion via a standard pipeline.
- Expert memory must never be overwritten by project-scoped data without explicit promotion rules.

## Promotion Rules (Health)
Health profile data may be promoted from:
- Global Tier-2M facts (meds, allergies, weight)
- Explicit user statements (deterministic extraction)
- Lab artifacts or uploads (deterministic parsing)

Health profile data must be normalized before write.

## Non-Breaking Guarantees
- Adding or improving a hat must not modify foundational memory behavior.
- Context retrieval should always include foundational memory regardless of hat.
- Expert overlays must remain bounded and deterministic.

## Future Enablement
To enable another hat’s expert memory:
1. Define its schema and stable fields.
2. Add deterministic promotion rules.
3. Add injection to the expert-memory adapter for that hat.
4. Add test coverage for:
   - Foundational memory unchanged.
   - Hat memory injected only when active.

## Notes
- This spec is repo-wide and must be followed by all future hat implementations.
- If a conflict arises between hat logic and foundational memory, foundational rules win.
