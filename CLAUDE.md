# Project Overview
Goal: Predicting primary real estate prices using secondary market data as a proxy.
Stack: Python, DVC, Optune, MLFlow.

# Core Workflow Loop
As an AI assistant, you must follow this exact sequence during our interactions:
1. **READ CONTEXT:** At the start of a session, check the Project Notes folder to understand the current state and past decisions.
2. **WORK:** Write code, run experiments, and solve tasks in this working directory.
3. **SEARCH EXPERIENCE:** If we face a hard problem, search my Knowledge Base (do not load it all into context).
4. **WRAP UP:** At the end of the session (when I say `/wrap`), generate insights and save them to the Project Notes.

# Directory Rules

You have access to 3 distinct locations. Treat them exactly as specified:

## 1. Code (This Directory)
- **Role:** The current workspace.
- **Rules:** Standard read/write. Run scripts, modify code, and manage DVC/MLFlow pipelines here.

## 2. Project Notes (Context & Log)
- **Location:** `~/2Areas/Knowledge management/Workbench/Primary vs Secondary/`
- **Role:** Project-specific history, architecture decisions, and session logs.
- **Rules:**
  - **Read on start:** Actively read recent files here to gain project context.
  - **Append-only:** CREATE new files here. NEVER edit or overwrite existing ones. If a filename exists, pick a new name.
  - **Ad-hoc notes:** Mid-session, create a note here ONLY for major architecture decisions, critical research, or non-obvious gotchas. Don't be noisy. Aim for notes you'd want to find 6 months from now.
- **Naming convention:** `YYYY-MM-DD Normal name.md` (Date prefix, followed by a 3–5 word name with spaces. e.g., `2023-10-25 Routing spike.md` or `2023-10-25 Data preprocessing gotcha.md`).
- **Frontmatter required on ALL notes:**
  ```yaml
 ---
creation_date: YYYY-MM-DD
type: decision | research | session | gotcha | note
tags: [primary_vs_secondary, ...]
aliases: []
links:
---
  ```

## 3. Knowledge Base (Experience Library)
- **Location:** `~/2Areas/Knowledge management/Digital Garden/`
- **Role:** My personal accumulated experience across all projects.
- **Rules:**
  - **DO NOT preemptively read or summarize this folder.** Do not load it into context.
  - **Treat as a SEARCH ENGINE.** When hitting an unfamiliar problem, search first:
    - Search text: `grep -ri "<keyword>" "~/2Areas/Knowledge management/Digital Garden/"`
    - Search titles: `find "~/2Areas/Knowledge management/Digital Garden/" -iname "*<topic>*"`
  - Read only the 1-2 most relevant matching files.
  - **Read-only:** DO NOT write or edit anything in this folder.
  - **Cite sources:** If you use a pattern from here, cite it inline (e.g., "per `Digital Garden/patterns/Retry backoff.md`...").

---

# End of Session: The `/wrap` Command

When I type `/wrap`, "wrap up", or "save what we did", you must execute the end-of-session protocol.

**Action:** Create a new markdown file in the Project Notes folder (`~/2Areas/Knowledge management/Workbench/Primary vs Secondary/`).
**Filename:** `YYYY-MM-DD Session summary.md` (or similar normal name, appending `-1`, `-2` if multiple sessions occur in one day).
**Format:** Strictly use the following template. Use concise bullet points, and skip empty sections.

```md
---
date: YYYY-MM-DD
type: session
tags: [primary_vs_secondary, stack, topic]
---

## Worked on
- [Brief bullet points of scripts written, pipelines run, DVC/MLFlow updates]

## Decisions & Gotchas
- [Decision/Problem] -> [Rationale/Solution]

## Learned (worth keeping)
- [Bullet points of generalizable knowledge]
> *Suggestion: Consider promoting [Topic] to `Digital Garden/...`* (Include this line ONLY if the knowledge is highly reusable outside this project).

## Next
- [ ] [Clear actionable steps for the next session]

## Knowledge References
- `path/to/Digital Garden file.md` (List any Knowledge Base files we successfully utilized today)
```
