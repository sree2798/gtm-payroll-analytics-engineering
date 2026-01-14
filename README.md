# GTM Payroll Analytics Engineering (BigQuery + dbt + CI/CD)

This project is an end-to-end **Analytics Engineering** build for a Payroll SaaS Go-To-Market (GTM) analytics layer. It transforms raw CRM, Marketing, Billing, Product Usage, and Support data into **governed, tested, production-grade marts** that answer 15 stakeholder GTM questions by client segment (employee count bands)

---
## Architecture
<img width="500" height="500" alt="image" src="https://github.com/user-attachments/assets/27a9e701-5d9c-494b-b872-785f25172cac" />


## Why this project

Payroll SaaS GTM teams (Sales, Marketing, CS, Product, Leadership) need consistent definitions for:
- Pipeline, win rate, sales cycle, discounting impact
- Funnel conversion + CAC proxy
- Onboarding + retention (GRR/NRR) + churn drivers
- Product adoption → expansion signals
- ARR mix & growth by client segment

This repo demonstrates:
- Dimensional modeling & analytics engineering best practices
- dbt staging → marts patterns (facts, dims, gold tables)
- Testing, documentation, and CI/CD in dbt Cloud
- BigQuery as the warehouse and consumption layer in `analytics`

---

## Tech stack

- **BigQuery** (warehouse)
- **dbt Cloud** (transformations + orchestration)
- **GitHub** (version control)
- **CI/CD** in dbt Cloud (PR checks + prod deploy)
- **SQL** (modeling + metrics)

---

## Stakeholder GTM Questions (15)

### Sales / RevOps
1. Pipeline coverage: Do we have enough pipeline to hit next quarter’s ARR target by segment?
2. Win rate: What’s win rate by segment and by primary competitor?
3. Sales cycle: How long is the sales cycle by segment and channel (inbound vs outbound vs partner)?
4. Discounting: Are we discounting more in certain segments, and does it improve close rate or just lower ARR?
5. Rep performance: Which reps perform best by segment (win rate + cycle time + ACV), and where do deals stall by stage?

### Marketing
6. Lead→Opp conversion: Which campaigns/channels generate the highest MQL→SQL→Opp conversion by segment?
7. CAC proxy: What’s cost per opportunity and cost per won deal by segment/channel (using spend + opp outcomes)?
8. Speed to revenue: Which channels create pipeline that converts fastest (time from first touch → close) by segment?

### Customer Success / Support
9. Onboarding efficiency: Time-to-first-payroll (or time-to-go-live) by segment; which segment struggles most?
10. Retention: Gross retention and net revenue retention by segment; where is churn concentrated?
11. Support burden: Tickets per 100 employees by segment; do high-severity tickets predict churn/renewal risk?

### Product / Adoption
12. Adoption drives expansion: Which product signals (payroll runs frequency, active admins, module adoption) predict expansion by segment?
13. Bundle attach: Which segments adopt Time/Benefits/Full HCM most often, and what is the uplift in ARR?
14. Feature-led growth: After enabling a module, does retention/NRR improve, and for which segments?

### Finance / Leadership
15. ARR mix & growth: ARR distribution and growth by segment; are we over-reliant on SMB or enterprise?

(These questions and the implementation plan are captured in the attached project PDF.) :contentReference[oaicite:2]{index=2}

---

## Client segmentation (employee bands)

Segments are defined as a governed seed so they are **non-overlapping and consistent** across all marts:

- 1–10
- 11–50
- 51–200
- 201–500
- 501–1000
- 1001–5000
- 5001–10000
- 10001+

Seed example: `seeds/employee_bands.csv`

---

## Warehouse layout (BigQuery datasets)

This project uses three functional layers (plus dev/CI schemas) per the implementation steps. :contentReference[oaicite:3]{index=3}

- `raw`  
  Source tables (synthetic generated for demo; in real life via Fivetran/ELT connectors)

- `dbt_sree2798` (dev)  
  Developer builds from your personal branch

- `dbt_ci` (CI)  
  Pull-request checks build here

- `analytics` (prod/consumption)  
  Production models and marts consumed by stakeholders and BI tools

---

## Data generation (synthetic raw)

Raw layer is generated via a BigQuery SQL script (≥10,000 rows per table). The script produces:

- `raw.crm_accounts`
- `raw.crm_opportunities`
- `raw.crm_opportunity_stage_history`
- `raw.billing_contracts`
- `raw.billing_invoices`
- `raw.marketing_campaigns`
- `raw.marketing_spend_daily`
- `raw.marketing_leads`
- `raw.marketing_touches`
- `raw.product_usage_daily`
- `raw.support_tickets`

In production, these would be ingested via ELT connectors rather than generated.

---

## dbt modeling approach

### 1) Sources
`models/staging/<domain>/src_*.yml` declares raw tables as dbt sources.

### 2) Staging models (cleaning layer)
One `stg_*` model per raw table:
- type casting
- naming standardization
- basic dedupe patterns
- tests: `not_null`, `unique`, `accepted_values`

### 3) Dimensional models
Minimum dims:
- `dim_account` (includes `employee_band_id/label`)
- `dim_product_bundle`
- (Optional) `dim_date`

### 4) Facts
Minimum facts:
- `fact_opportunity` (+ enriched version with account attributes)
- `fact_contract` (+ enriched)
- `fact_invoice`
- `fact_product_usage_daily`
- `fact_support_ticket`

### 5) “Gold” mart (primary serving table)
**`mart_gtm_account_daily`**  
Grain: **one row per account per day** with:
- current ARR (active contracts)
- pipeline created/won daily rollups
- rolling 30-day product signals (payroll runs, active admins, adoption)
- rolling 30-day support (ticket count, sev1 rate)
- segment (employee band)

This mart is designed to answer the majority of GTM questions with consistent metrics. :contentReference[oaicite:4]{index=4}

---

## CI/CD in dbt Cloud

This repo is configured for dbt Cloud CI/CD (the “real job” part). :contentReference[oaicite:5]{index=5}

### Environments
- **Dev** → `dbt_<yourname>`
- **CI** → `dbt_ci`
- **Prod** → `analytics`

### Jobs
- **PR CI Check**
  - Trigger: Pull Request
  - Command: `dbt build`
  - Output: PR pass/fail status

- **Prod Deploy**
  - Trigger: merge to `main` (or schedule)
  - Command: `dbt build`
  - Output: production tables in `analytics`

---

## How to run (dbt)

### Run everything
```bash
dbt build


