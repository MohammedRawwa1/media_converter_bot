# security/ — the media_conversion_bot audit instance

Two things live here, and they are meant to be used together:

| Path | What it is |
|------|------------|
| `mediabot-profile.json` | The **instance profile**: this bot's stack, layers, rule configuration, accepted deviations, the real-CVE ingestion settings, and the flaw catalog. Everything else reads from it. |
| `scan_mediabot.py` | The **tailored scanner** — a Helpha-style engine specialised for this codebase (python-telegram-bot, FastAPI/Starlette, Flask, Telethon/Pyrogram, Redis/RabbitMQ/Kafka, MongoDB, S3, ffmpeg). |

`SECURITY_AUDIT_SKILL.md` is the audit playbook the flaw catalog was distilled
from; the profile's `skill_catalog` lists which rules check each of its classes.
The scanner writes everything it generates into `reports/` (gitignored).

## Run it

```bash
# tailored rules + real CVE ingestion (pip-audit resolves the requirement
# ranges, OSV.dev supplies CVE/GHSA ids, severity and fixed versions)
python security/scan_mediabot.py

# no network: use the cached CVE KB (what CI does)
python security/scan_mediabot.py --offline

# force a fresh OSV pull, or skip dependencies entirely
python security/scan_mediabot.py --refresh-cve
python security/scan_mediabot.py --no-cve

# include test-file hits (rules skip tests by default)
python security/scan_mediabot.py --include-tests
```

Exit codes: `0` clean · `2` confirmed CRITICAL/HIGH · `3` candidates only · `1` broken.

## Output

| File | Use |
|------|-----|
| `reports/MEDIABOT_SECURITY_AUDIT.md` | Human report: severity summary, findings with `file:line` evidence and fixes, the **coverage table** against the flaw catalog, and the CVE table. |
| `reports/mediabot-audit.json` | Machine-readable findings, coverage, CVE KB summary and tool notes. |
| `reports/mediabot-audit.sarif` | For GitHub code scanning (uploaded by the *Security Scan* workflow). |
| `reports/cve-kb.json` | The ingested advisory corpus for the resolved dependency set; also mirrored to `.vulnclaw-local/kb/cve/` for the agent. |

### Confirmed vs candidate

This is the single most important property of the report, and it comes straight
from the post-mortem audits: a confidently reported non-issue costs more than a
missed one.

* **CONFIRMED** — carries a literal `file:line` excerpt from the scanned
  revision and a concrete mechanism.
* **CANDIDATE** — a heuristic hit (a route that *looks* privileged, a pattern
  that *may* be exploitable). It must be adjudicated by hand before it is treated
  as real. Never quote a candidate as a finding.

Secrets are masked in the report by construction; the scanner never re-emits a
matched credential.

## The VulnClaw instance

`../.vulnclaw-local/` makes an LLM agent use this repo's own profile. It is
**local state, not project source**: gitignored, excluded from the Docker and
Railway build contexts, and nothing in the checkout depends on it. A runner that
is not vendored here points at it via `<repo>/.vulnclaw-local/config.yaml` ahead
of `~/.vulnclaw`; the scanner recreates `kb/cve/` on demand, so a missing tree
costs nothing.

The instance contains:

* `config.yaml` — scoped to a local-repo review: snippets run restricted, recon
  providers off, destructive actions blocked, `evidence_min_report_level: L4` so
  only reproducible findings can reach the report. **No credential is stored**;
  the agent reads `VULNCLAW_LLM_API_KEY` from the environment.
* `skills/media-bot-security/` — the audit playbook plus references: a
  rule→flaw-class catalog and a description of this bot's entry points and assets.
* `kb/` — hand-written technique entries (webhook fail-open, session-string
  takeover, ffmpeg argv injection, worker SSRF) plus the `cve/` entries the
  scanner ingests from OSV.

`security/` and `.vulnclaw-local/` are excluded from `.dockerignore` and
`.railwayignore`: audit tooling and reports must never reach a production image
or a deploy.

## Adding an accepted risk

If a finding is real but you are deliberately not fixing it, record it in
`mediabot-profile.json`:

```json
{
  "rule": "API-DOCS-EXPOSED",
  "glob": "main.py",
  "reason": "Docs are intentional for the public API surface; no auth on /docs."
}
```

It then appears under “Accepted deviations” in the report instead of being
silently dropped — the difference between a documented decision and a blind spot.

## Silencing a finding in place

A generic `# nosec` / `# noqa` on a line is honoured by most rules, but **not** by
the injection family (`INJ-*`). Those rules match code that executes commands,
deserializes data or assembles queries, and the classes with no bandit
equivalent (`INJ-CMD-FSTRING`, `INJ-NOSQL`, `INJ-PATH-LOCAL`) have no other guard
at all — so the comment must name the class it is silencing:

```python
os.system(cmd)  # nosec B605  # argv is a fixed literal
cursor.execute(f"...{v}")  # nosec INJ-SQL  # v is an int from the schema
```

Naming one class never silences another: `# nosec B602` asserts “this call passes
an argument list” and leaves `INJ-CMD-FSTRING` armed on the same line. That is
deliberate — it is what stops a bandit-shaped annotation (`# nosec B603`, which
the subprocess call sites carry) from quietly removing argv-injection coverage.
The reason goes after a second `#`, because bandit parses everything up to the
next `#` as candidate test ids and warns about each word it cannot resolve.
