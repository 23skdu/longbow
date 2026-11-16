# Longbow Roadmap (Remaining Work)

All P0 performance optimizations (`P0-1` through `P0-7`), OIDC Federation & Cloud IAM authentication (`OIDC-0` through `OIDC-8`), and DevSecOps / observability upgrades (`DS-1` through `DS-11`) have been completed and verified.

See:
- `docs/performance.md` for benchmark results, P0 root-cause fixes, and baselines.
- `docs/security-compliance.md` for the OIDC Federation security architecture, IAM policies, GA checklist, and SOC2/GDPR control mappings.
- `docs/runbooks.md` for operational and OIDC incident-response runbooks.

---

## Remaining Optional Items

- [ ] P0-2.5 (Optional) Zero-copy Float32→complex128 extraction (requires API change; currently uses zero-allocation worker-local scratch widening in `extractVectorComplex128Into`).
