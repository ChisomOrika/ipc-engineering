# IPC Data Team — Personal AI Agents

Private operational workspace for **Chisom**, data engineer + analyst at IPC Africa, working across **GoSource**, **Daash**, and **FindEat**.

These agents exist to make Chisom faster, sharper, and more visible in her role. They are not a fake company — they are a personal team that augments one person.

## How to Use

Open Claude Code inside any agent's folder:

```bash
claude ~/ipc-engineering/agents/analytics-engineer    # Tunde
claude ~/ipc-engineering/agents/insights-lead         # Ifeanyi
# ...etc
```

Each agent loads with full context on IPC, the codebase, and Chisom's role.

## The Team

| Agent | Name | Role | Folder |
|-------|------|------|--------|
| Chief of Staff | Ada | Daily brief, task tracking, visibility memos | `chief-of-staff/` |
| Analytics Engineer | Tunde | dbt models, warehouse hygiene, SQL review | `analytics-engineer/` |
| Data Analyst | Nneka | Ad-hoc analysis, cohorts, funnels, retention | `data-analyst/` |
| Insights Lead | Ifeanyi | "So what" memos for product/ops/leadership | `insights-lead/` |
| Dashboard Builder | Kemi | Streamlit apps, BI dashboards, UX | `dashboard-builder/` |
| Business Partner | Obi | Per-arm KPIs (GoSource, Daash, FindEat) | `business-partner/` |
| Growth & Experiments | Chuka | Experiment design, metric definitions, PRD review | `growth-experiments/` |
| Career & Visibility | Zuri | Wins to share, projects to pitch, skills gap | `career-visibility/` |

## Shared Context

- [`shared/company-brief.md`](shared/company-brief.md) — IPC, the three products, the codebase, Chisom's scope
- [`shared/tasks/`](shared/tasks/) — cross-agent task tracking and handoffs
- [`shared/notes/`](shared/notes/) — investigation notes, drafts, scratch
