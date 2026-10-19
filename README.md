# PBM Split-Accumulator Reconciliation

Simulation of a Pharmacy Benefit Manager (PBM) split-accumulator reconciliation pipeline, built on Databricks with PySpark and Delta Lake. The project models the problem of tracking member cost accumulators across multiple Third-Party Administrators (TPAs), detecting discrepancies in deductible and out-of-pocket maximum tracking, and surfacing findings through a SQL-native analytics dashboard.

In split-accumulator PBM arrangements, a single member's deductible and out-of-pocket maximum (OOP max) are tracked by both the PBM and each TPA processing claims. These accumulators drift apart because an eligibility file arrives late, a retroactive adjustment doesn't propagate, or a TPA applies different accumulation logic. Members overpay copays, plans absorb costs they shouldn't, and nobody catches it until a complaint or an audit.

```
             PLAN SPONSOR
            834 enrollment
                  |
    +-------------+-------------+
PBM (pharmacy)              TPA (medical)
pbm_eligibility             tpa_eligibility_feed
pbm_accumulators            tpa_accumulator_feed
pharmacy_claims                 |
    |                           |
    |   nightly SFTP file       |
    |<--------------------------+
    |   tpa_accumulator_file_raw
    v
combined_balance vs plan_design limits
```

Some of the causes for accumulator discrepancies are:

a) Asynchrony between PBM and TPA balances (nightly batch, not real-time)

b) File transfer failures - late, missing, or malformed TPA files

c) Inadequate plan rule implementation across program types

d) Benefit year rollover - accumulators that don't reset cleanly on Jan 1


![Split_accumulator_ecosystem](pbm-split-accumulator-recon/diagrams/spli_accumulator_diagram.png)


## Pipeline Architecture

Ten PySpark notebooks execute through a central orchestrator. NB_00 provides shared utilities (structured logging, error handling, table validation) and is imported by every downstream notebook via `%run`. The orchestrator enforces a gate on NB_01 (the pipeline stops if inbound file quality fails), runs NB_02 through NB_05 as independent reconciliation stages, treats NB_06 as non-blocking, and chains NB_07 and NB_08 as dependent on prior results.

| Notebook | Role | What it does |
|----------|------|-------------|
| `NB_00` | Pipeline Utilities | Structured logging (`pipeline_audit_log`), run ID generation, error handling decorators, table validation helpers. Imported by all downstream notebooks. |
| `NB_01` | Inbound File Validation | Gate notebook. Validates the TPA accumulator file (`tpa_accumulator_file_raw`) against the file control log. Checks schema conformance, NULL rates, record counts, duplicate detection, and cross-references member IDs against `pbm_eligibility`. Pipeline stops here if quality checks fail. |
| `NB_02` | Eligibility Reconciliation | Compares `pbm_eligibility` against `tpa_eligibility_feed` to find members the PBM knows about but the TPA doesn't (and vice versa). Joins against `pharmacy_claims` to quantify the dollar exposure of eligibility gaps. |
| `NB_03` | Accumulator Reconciliation | Core matching - lines up `pbm_accumulators` against `tpa_accumulator_feed` by member and benefit year, flags deductible and OOP max variances, and applies tolerance thresholds from `plan_design`. |
| `NB_04` | Copay Accumulator Audit | Audits how copay accumulator programs (TRADITIONAL vs ACCUMULATOR vs MAXIMIZER) apply manufacturer copay card payments toward member cost-sharing. Includes state-level ban tracking (`accumulator_ban_states`). |
| `NB_05` | Benefit Year Rollover | Investigates cross-year edge cases - accumulators that should have reset on Jan 1 but didn't, claims straddling benefit years, carryover amounts that don't match plan design rules. |
| `NB_06` | Trend Analysis + Time Travel | Simulates 90 days of daily reconciliation results (`daily_recon_log`) with drift, spikes, and remediation events. Demonstrates Delta Lake time travel by versioning `tpa_accum_versioned` and querying historical snapshots. |
| `NB_07` | Convergence Query | Pulls findings from NB_01 through NB_05, deduplicates, scores severity, and writes the unified output: `findings_registry`, `findings_member_rollup`, `findings_plan_rollup`. |
| `NB_08` | Executive Dashboard | Notebook-based dashboard summarizing total findings, exposure by severity tier, top affected plans, and compliance status. Reads from the convergence tables and `daily_recon_log`. |
| `NB_ORCHESTRATOR` | Pipeline Orchestrator | Runs the full pipeline end-to-end. Manages execution order, gate logic, error handling, and timing. Logs run metadata to `pipeline_audit_log`. |


## Dashboard

A Lakeview SQL dashboard (`dashboard/PBM_Split-Accumulator_Reconciliation.lvdash.json`) provides daily monitoring on top of the Delta Lake tables.

- Overcharge rate trend (from `daily_recon_log`) with current, peak, and 30-day average counters
- Exposure distribution by plan, finding category, and severity tier
- Compliance status by plan against configurable thresholds
- Plan-level detail table with drill-through to member-level findings

The dashboard is independent of NB_08 - both read from the same underlying tables but serve different audiences. NB_08 runs inside the pipeline for notebook-level visibility. The Lakeview dashboard is the operational monitoring layer for daily use.
