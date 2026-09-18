#!/usr/bin/env python3
"""
scan_mediabot.py — customized Helpha-style security instance for media_conversion_bot.
======================================================================================

A *target-specific* scanner, not a generic one. It is driven by
``security/mediabot-profile.json``, which describes this bot's stack, its layers,
which rules to run, the flaw catalog distilled from the audit playbook in this
folder, and the real-CVE ingestion settings.

What it does
------------
1. **Rules** tailored to a python-telegram-bot v21 + FastAPI/Starlette + Flask +
   Telethon/Pyrogram + Redis/RabbitMQ/Kafka + MongoDB + S3 + ffmpeg codebase:
   secrets, fail-open auth guards, unauthenticated web routes, CORS/host/config
   misconfiguration, command/NoSQL/path/ffmpeg-argv injection, SSRF, log leakage,
   infra (Docker/compose/CI/gitignore) and supply chain.
2. **Real CVE ingestion**: resolved dependencies come from ``pip-audit`` (which
   resolves the requirement ranges exactly as a deploy would), are checked against
   the **OSV.dev** database, and each OSV id is expanded to its CVE/GHSA alias,
   severity, summary and fixed version. The result is cached to
   ``security/reports/cve-kb.json`` and mirrored into the VulnClaw instance KB at
   ``.vulnclaw-local/kb/cve/`` so the agent can retrieve it during an LLM run.
3. **Coverage matrix**: every finding is mapped back to the flaw classes the
   profile's ``skill_catalog`` records, so a gap is visible as a gap instead of
   being mistaken for "clean".
4. **Evidence gate** (the hard lesson from the post-mortem audits): a finding is only
   ``CONFIRMED`` when it carries a file:line excerpt from the *current* source.
   Heuristic hits are labelled ``CANDIDATE`` and are never treated as proven.

Outputs: ``mediabot-audit.json``, ``MEDIABOT_SECURITY_AUDIT.md``,
``mediabot-audit.sarif`` (for GitHub code scanning), ``cve-kb.json``.

Usage
-----
    python security/scan_mediabot.py                 # full run (rules + CVE)
    python security/scan_mediabot.py --offline       # no network, cached/absent CVEs
    python security/scan_mediabot.py --refresh-cve   # force a fresh OSV pull
    python security/scan_mediabot.py --no-cve        # rules only
    python security/scan_mediabot.py --include-tests # also report test-file hits

Exit codes: 0 = clean · 2 = confirmed CRITICAL/HIGH · 3 = candidates only ·
            1 = scanner/env error.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path

SCANNER_VERSION = "1.0.0"
PROFILE_FILENAME = "mediabot-profile.json"


# ─────────────────────────────────────────────────────────────────────────────
# Model
# ─────────────────────────────────────────────────────────────────────────────

class Severity(Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


SEV_RANK = {s: i for i, s in enumerate(
    [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW, Severity.INFO]
)}
SEV_WEIGHT = {Severity.CRITICAL: 25, Severity.HIGH: 12, Severity.MEDIUM: 5,
              Severity.LOW: 2, Severity.INFO: 0}


def sev(value: str) -> Severity:
    """Normalize a severity string from any source (OSV/GHSA/bandit style)."""
    v = (value or "").strip().upper()
    if v in ("MODERATE", "MEDIUM", "MED"):
        return Severity.MEDIUM
    for s in Severity:
        if s.value == v:
            return s
    return Severity.MEDIUM


@dataclass
class Finding:
    rule: str
    title: str
    severity: Severity
    owasp: str
    layer: str
    category: str
    verdict: str  # CONFIRMED | CANDIDATE
    description: str
    evidence: str = ""
    file: str = ""
    line: int = 0
    fix: str = ""
    skills: list[str] = field(default_factory=list)
    cve: str = ""
    package: str = ""
    installed: str = ""
    fixed: str = ""
    cvss: float = 0.0

    @property
    def location(self) -> str:
        return f"{self.file}:{self.line}" if self.file else "(repo-level)"


@dataclass
class Report:
    instance: str = ""
    scanner_version: str = SCANNER_VERSION
    target: str = ""
    timestamp: str = ""
    findings: list[Finding] = field(default_factory=list)
    cve_kb: dict = field(default_factory=dict)
    coverage: list[dict] = field(default_factory=list)
    suppressions: list[dict] = field(default_factory=list)
    files_scanned: int = 0
    scan_seconds: float = 0.0
    tool_errors: list[str] = field(default_factory=list)

    def by_severity(self) -> dict[str, int]:
        out = {s.value: 0 for s in Severity}
        for f in self.findings:
            out[f.severity.value] += 1
        return out

    def by_layer(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for f in self.findings:
            out[f.layer] = out.get(f.layer, 0) + 1
        return out

    def by_verdict(self) -> dict[str, int]:
        out = {"CONFIRMED": 0, "CANDIDATE": 0}
        for f in self.findings:
            out[f.verdict] = out.get(f.verdict, 0) + 1
        return out

    @property
    def score(self) -> int:
        """100 minus the penalty of each *distinct* rule at its worst severity.

        Scoring per rule instead of per finding keeps a repeated low-severity
        pattern (e.g. 20 unpinned requirements) from drowning out the signal of a
        single serious one, which is how the skill-pack score is meant to read.
        """
        worst_by_rule: dict[str, Severity] = {}
        for f in self.findings:
            current = worst_by_rule.get(f.rule)
            if current is None or SEV_RANK[f.severity] < SEV_RANK[current]:
                worst_by_rule[f.rule] = f.severity
        return max(0, 100 - sum(SEV_WEIGHT[s] for s in worst_by_rule.values()))

    def worst(self) -> Severity | None:
        return min((f.severity for f in self.findings), key=lambda s: SEV_RANK[s], default=None)


# ─────────────────────────────────────────────────────────────────────────────
# Rule definitions
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RegexRule:
    """A line-oriented rule. Any pattern hit in an eligible file is a finding."""
    id: str
    title: str
    severity: Severity
    owasp: str
    layer: str
    category: str
    desc: str
    fix: str
    patterns: tuple[str, ...]
    globs: tuple[str, ...] = ("**/*.py",)
    require: tuple[str, ...] = ()      # every regex must also appear in the file
    exclude: tuple[str, ...] = ()      # any regex present neutralizes the rule
    line_exempt: tuple[str, ...] = ()  # a matched line matching any of these is skipped
    include_tests: bool = False
    verdict: str = "CONFIRMED"
    mask: bool = False                 # mask the matched text in evidence
    max_hits: int = 12

    def compiled(self) -> tuple[re.Pattern, ...]:
        return tuple(re.compile(p) for p in self.patterns)

    def compiled_list(self, group: str) -> tuple[re.Pattern, ...]:
        return tuple(re.compile(p) for p in getattr(self, group))


# Neutralizers that indicate an intentional, reviewed use.
NOSEc = (r"#\s*(noqa|nosec)",)

def _named_exempt(*names: str) -> tuple[str, ...]:
    """An exemption a suppression comment must NAME for the rule to honour it.

    Every rule in the injection family uses one of these instead of ``NOSEc``.

    A bandit-targeted suppression such as ``# nosec B603`` asserts "this
    subprocess call passes an argument list" — exactly the review B603 asks for —
    and says nothing about whether that argv can be influenced. Because a generic
    ``noqa``/``nosec`` neutralizes any rule in this scanner, honouring it in the
    injection family would mean that annotating a call for bandit silently drops
    it out of argv-injection coverage. For the classes with no bandit equivalent
    (INJ-CMD-FSTRING, INJ-NOSQL, INJ-PATH-LOCAL) nothing else would catch the
    regression at all.

    Each rule therefore accepts only a comment naming ITS OWN class — the bandit
    id or the scanner's rule id: ``# nosec B602  # reviewed literal argv``,
    ``# nosec INJ-SQL  # built by the query builder``. Naming one class never
    silences another. The reason goes after a second ``#`` because bandit parses
    everything up to the next ``#`` as test ids and warns for each word it can
    not resolve.
    """
    return (r"#\s*(?:noqa|nosec)[^\n]*\b(?:" + "|".join(names) + r")\b",)


INJ_SHELL_EXEMPT = _named_exempt("B602", "B604", "S602", "S604", "INJ-SHELL")
INJ_OS_SHELL_EXEMPT = _named_exempt("B605", "S605", "INJ-OS-SYSTEM")
INJ_EVAL_EXEMPT = _named_exempt("B307", "S307", "INJ-EVAL")
INJ_ARGV_EXEMPT = _named_exempt("INJ-CMD-FSTRING")
INJ_PICKLE_EXEMPT = _named_exempt("B301", "S301", "INJ-PICKLE")
INJ_YAML_EXEMPT = _named_exempt("B506", "S506", "INJ-YAML")
INJ_NOSQL_EXEMPT = _named_exempt("INJ-NOSQL")
INJ_PATH_EXEMPT = _named_exempt("INJ-PATH-LOCAL")
INJ_SQL_EXEMPT = _named_exempt("B608", "S608", "INJ-SQL")

SECRET_RULES: tuple[RegexRule, ...] = (
    RegexRule(
        "S-TOKEN", "Telegram bot token in source", Severity.CRITICAL, "A02:2021",
        "secrets", "SECRETS",
        "A Bot API token grants full control of the bot (send, forward, read updates).",
        "Revoke with @BotFather, store the new token only in the environment.",
        (r"\b\d{8,10}:[A-Za-z0-9_-]{35}\b",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md", "**/*.txt"),
        line_exempt=(r"\b\d{8,10}:AA[A-Za-z0-9_-]{33}\b" , r"<redacted>", r"BOT_TOKEN\s*=\s*[\"']{2}"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-AWS-AK", "AWS access key id in source", Severity.HIGH, "A02:2021",
        "secrets", "SECRETS",
        "Long-lived AWS credentials in the tree can be used against the storage bucket.",
        "Rotate the key, move it to the platform secret store, prefer instance roles.",
        (r"\b(AKIA|ASIA)[0-9A-Z]{16}\b",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md", "**/*.txt"),
        line_exempt=(r"<redacted>", r"EXAMPLE", r"XXXX"), include_tests=True, mask=True,
    ),
    RegexRule(
        "S-AWS-SK", "AWS secret access key literal", Severity.CRITICAL, "A02:2021",
        "secrets", "SECRETS",
        "A literal AWS secret access key is a full bucket/account compromise until rotated.",
        "Rotate immediately and load from the environment only.",
        (r"(?i)aws_?secret_?access_?key\s*[=:]\s*[\"'][A-Za-z0-9/+=]{40}[\"']",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.txt"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-MONGO", "MongoDB URI with inline credentials", Severity.HIGH, "A02:2021",
        "secrets", "SECRETS",
        "An inline connection string embeds the database password and leaks through logs/dumps.",
        "Keep credentials in env vars; the app already supports MONGO_URI/MONGODB_URI.",
        (r"(?i)mongodb(\+srv)?://[^:\s\"']+:[^@\s\"'${}]+@",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md"),
        line_exempt=(r"\$\{", r"os\.getenv", r"os\.environ", r"getenv\(", r"<", r"\*{4,}",
                     r"redact", r"^\s*#", r"user:pass", r"user:password", r"example",
                     r"your-connection", r"\.\.\."),
        include_tests=True, mask=True, max_hits=3,
    ),
    RegexRule(
        "S-REDIS", "Redis URI with inline password", Severity.HIGH, "A02:2021",
        "secrets", "SECRETS",
        "An inline Redis password in source or docs compromises the job queue.",
        "Load REDIS_URL from the environment; never commit a credentialed URL.",
        (r"rediss?://[^:\s\"']*:[^@\s\"'${}]{6,}@",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md"),
        line_exempt=(r"\$\{", r"os\.getenv", r"os\.environ", r"<", r"\*{4,}", r"redis://:[^@]*@"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-KAFKA", "Kafka SASL password literal", Severity.HIGH, "A02:2021",
        "secrets", "SECRETS",
        "A literal SASL password grants publish/consume on the event bus.",
        "Rotate and load KAFKA_SASL_PASSWORD from the environment only.",
        (r"(?i)KAFKA_SASL_PASSWORD\s*=\s*[\"']?[^\s\"'#]{8,}",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.env"),
        line_exempt=(r"\$\{", r"os\.getenv", r"os\.environ", r"<", r"change-me", r"\*{4,}"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-GOOGLE-KEY", "Google/Gemini API key literal", Severity.HIGH, "A02:2021",
        "secrets", "SECRETS",
        "A leaked generative-AI key can be abused for billable traffic.",
        "Rotate the key and keep it server-side in the environment.",
        (r"\bAIza[0-9A-Za-z\-_]{35}\b",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-OPENAI", "Bearer-style API key literal (sk-…)", Severity.HIGH, "A02:2021",
        "secrets", "SECRETS",
        "A provider API key committed to the tree is billable and abusable until rotated.",
        "Rotate and load from the environment.",
        (r"\bsk-[A-Za-z0-9]{20,}\b",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md"),
        line_exempt=(r"sk-proj-xxx", r"sk-\.\.\."), include_tests=True, mask=True,
    ),
    RegexRule(
        "S-PRIVATE-KEY", "Private key material in the repository", Severity.CRITICAL,
        "A02:2021", "secrets", "SECRETS",
        "A committed private key (TLS, SSH, CA) must be treated as compromised.",
        "Remove from history, rotate the key pair, keep only public certificates.",
        (r"-----BEGIN (RSA |EC |OPENSSH |PGP |DSA )?PRIVATE KEY-----",),
        globs=("**/*",), include_tests=True, mask=True,
    ),
    RegexRule(
        "S-SESSION-STR", "Telethon/Pyrogram session string literal", Severity.CRITICAL,
        "A02:2021", "secrets", "SECRETS",
        "A userbot session string is a full account login: it can read and send as the user.",
        "Revoke the session in Telegram, keep session strings only in the secret store, "
        "and never log or persist them in the repo.",
        (r"(?i)(session[_ ]?string|SESSION_STRING)\s*[=:]\s*[\"'][A-Za-z0-9+/=_-]{60,}[\"']",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.yaml", "**/*.sh", "**/*.md", "**/*.txt"),
        line_exempt=(r"os\.getenv", r"os\.environ", r"\$\{", r"<", r"\.session\.json"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-SENTRY-DSN", "Sentry DSN literal", Severity.MEDIUM, "A09:2021",
        "secrets", "SECRETS",
        "A DSN allows injecting arbitrary error events into the project dashboard.",
        "Load SENTRY_DSN from the environment.",
        (r"https://[0-9a-f]{32}@[a-z0-9.-]+\.ingest\.sentry\.io/\d+",),
        globs=("**/*.py", "**/*.json", "**/*.yml", "**/*.sh", "**/*.md"),
        include_tests=True, mask=True,
    ),
    RegexRule(
        "S-GENERIC", "Generic hardcoded secret assignment", Severity.MEDIUM, "A02:2021",
        "secrets", "SECRETS",
        "A literal secret bound in code (not read from the environment) tends to leak "
        "through the repo, logs and error reports.",
        "Move the value to an environment variable and reference it via os.getenv().",
        (r"(?i)\b(api[_-]?key|apikey|client[_-]?secret|auth[_-]?token|access[_-]?token|"
         r"password|passwd|webhook[_-]?secret|upload[_-]?secret|diag[_-]?token|debug[_-]?secret)"
         r"\s*[=:]\s*[\"'][A-Za-z0-9_\-./+=]{12,}[\"']",),
        globs=("**/*.py", "**/*.sh", "**/*.yml", "**/*.yaml"),
        line_exempt=(r"os\.getenv", r"os\.environ", r"getenv\(", r"^\s*#", r"change-me",
                     r"YOUR_", r"REPLACE", r"PLACEHOLDER", r"example", r"EXAMPLE", r"<",
                     r"getenv", r"os\.environ\.get", r"\*{4,}"),
        include_tests=True, mask=True,
    ),
)

INJECTION_RULES: tuple[RegexRule, ...] = (
    RegexRule(
        "INJ-SHELL", "subprocess call with shell=True", Severity.CRITICAL, "A03:2021",
        "worker", "INJECTION",
        "Shell interpretation turns any interpolated user value into command execution.",
        "Pass an argument list and keep shell=False (the project already does this for ffmpeg).",
        (r"subprocess\.[A-Za-z_]+\([^)]*shell\s*=\s*True", r"shell\s*=\s*True"),
        line_exempt=INJ_SHELL_EXEMPT,
    ),
    RegexRule(
        "INJ-OS-SYSTEM", "os.system / os.popen usage", Severity.HIGH, "A03:2021",
        "worker", "INJECTION",
        "os.system always goes through a shell and cannot be safely parameterized.",
        "Use subprocess.run([...], shell=False).",
        (r"\bos\.(system|popen)\s*\(",), line_exempt=INJ_OS_SHELL_EXEMPT,
    ),
    RegexRule(
        "INJ-EVAL", "eval / exec / __import__ on dynamic input", Severity.HIGH, "A03:2021",
        "worker", "INJECTION",
        "Dynamic code evaluation converts data into code execution.",
        "Replace with an explicit dispatch table or json.loads.",
        (r"(?<![\w.])(eval|exec)\s*\((?![^)]*__doc__)", r"\b__import__\s*\("),
        line_exempt=(*INJ_EVAL_EXEMPT, r"exec\(.*\)\s*#\s*allowed", r"exec\s*\(\s*\)",
                     r"^\s*(async\s+)?def\s+eval\b", r"^\.eval\(", r"\.eval\("),
        include_tests=False,
    ),
    RegexRule(
        "INJ-CMD-FSTRING", "Command argument built by interpolation", Severity.HIGH,
        "A03:2021", "worker", "INJECTION",
        "Interpolating into a command line lets a crafted filename or URL add arguments "
        "(ffmpeg argv injection, option smuggling).",
        "Keep argv as a list of literal strings; validate and sanitize every user-supplied path.",
        (r"subprocess\.[A-Za-z_]+\(\s*f[\"']", r"subprocess\.[A-Za-z_]+\([^)]*\.format\(",
         r"Popen\(\s*f[\"']"),
        line_exempt=INJ_ARGV_EXEMPT,
    ),
    RegexRule(
        "INJ-PICKLE", "Unsafe deserialization (pickle / marshal)", Severity.CRITICAL,
        "A08:2021", "worker", "DESERIALIZATION",
        "Unpickling attacker-influenced bytes is arbitrary code execution.",
        "Use json (or msgpack) for serialized payloads; never unpickle queue/cache data.",
        (r"\b(pickle|marshal)\.loads?\s*\(", r"\bcPickle\.loads?\s*\("), line_exempt=INJ_PICKLE_EXEMPT,
    ),
    RegexRule(
        "INJ-YAML", "yaml.load without a safe loader", Severity.HIGH, "A08:2021",
        "worker", "DESERIALIZATION",
        "yaml.load with the default loader can construct arbitrary Python objects.",
        "Use yaml.safe_load (or Loader=yaml.SafeLoader).",
        (r"yaml\.load\s*\((?![^)]*(SafeLoader|safe_load|Loader\s*=\s*yaml\.SafeLoader))",),
        line_exempt=INJ_YAML_EXEMPT,
    ),
    RegexRule(
        "INJ-NOSQL", "NoSQL operator/query built from a variable", Severity.HIGH,
        "A03:2021", "bot", "INJECTION",
        "Passing user input into Mongo operators ($where/$regex/$ne/$gt) or raw find() "
        "payloads allows query injection and auth bypass.",
        "Route queries through utils.data_layer.query_builder with field whitelists.",
        (r"[\"']\$(where|expr|function|accumulator|near|geoNear|regex)[\"']\s*:\s*[A-Za-z_]",
         r"\$(where|expr|function|accumulator)\s*[\"']\s*:"),
        globs=("**/*.py",),
        exclude=(r"data_layer/", r"dangerous_ops", r"whitelist", r"ALLOWED_FIELDS", r"Prohibits"),
        line_exempt=(*INJ_NOSQL_EXEMPT, r"^\s*#", r"^\s*[\"']", r"dangerous", r"allowlist"),
        verdict="CANDIDATE", max_hits=3,
    ),
    RegexRule(
        "INJ-PATH-LOCAL", "Filesystem path built from request/user input", Severity.HIGH,
        "A03:2021", "web", "INJECTION",
        "Joining user input into a path without basename/allowlist checks enables traversal.",
        "Reduce to os.path.basename, validate the extension against the shared allowlist, "
        "and confirm the resolved path stays inside the intended directory.",
        (r"open\s*\(\s*os\.path\.join\([^)]*(request|user|name|filename|path\b)",
         r"os\.path\.join\([^)]*request\.(args|form|json)",
         r"send_file\s*\(\s*[A-Za-z_]\w*\s*\+"),
        globs=("**/*.py",), line_exempt=(*INJ_PATH_EXEMPT, r"basename", r"safe_extension", r"realpath"),
    ),
    RegexRule(
        "INJ-SQL", "SQL built by string interpolation", Severity.CRITICAL, "A03:2021",
        "worker", "INJECTION",
        "Interpolated SQL is classic injection; parameterize instead.",
        "Use bound parameters (?) or the query builder.",
        (r"(?i)(execute|executemany)\s*\(\s*f[\"']", r"(?i)(execute|executemany)\s*\([^)]*%\s*\("),
        globs=("**/*.py",), line_exempt=INJ_SQL_EXEMPT,
    ),
)

CONFIG_RULES: tuple[RegexRule, ...] = (
    RegexRule(
        "CFG-DEBUG-TRUE", "debug=True enabled", Severity.HIGH, "A05:2021",
        "web", "MISCONFIG",
        "Debug mode exposes the Werkzeug/Starlette debugger and can grant code execution.",
        "Never enable debug outside local development; gate it on an env var defaulting to false.",
        (r"\bdebug\s*=\s*True",), line_exempt=NOSEc,
    ),
    RegexRule(
        "CFG-HOST-0000", "Service bound to 0.0.0.0", Severity.MEDIUM, "A05:2021",
        "infra", "MISCONFIG",
        "Binding all interfaces exposes the service to every reachable network.",
        "Bind 127.0.0.1 and let the platform proxy/ingress do the exposure (add `# nosec` "
        "with justification where all-interface binding is deliberate).",
        (r"HOST\s*=\s*[\"']0\.0\.0\.0[\"']", r"[\"']0\.0\.0\.0[\"']"), line_exempt=NOSEc,
    ),
    RegexRule(
        "CFG-DETAIL-STR", "Exception string returned to the client", Severity.MEDIUM,
        "A09:2021", "web", "INFO_DISCLOSURE",
        "Returning str(e) leaks internal paths, schema and library versions to callers.",
        'Return a generic message and log the exception server-side '
        '("Internal error. Check server logs.").',
        (r"HTTPException\([^)]*detail\s*=\s*(str\(|f[\"'])",
         r"jsonify\([^)]*[\"']detail[\"']\s*:\s*(str\(|f[\"'])",
         r"detail\s*=\s*str\(e\)"),
        globs=("**/*.py",),
        line_exempt=(*NOSEc, r"Internal error", r"Check server logs", r"not found", r"invalid",
                     r"required", r"unsupported"),
        max_hits=4,
    ),
    RegexRule(
        "CFG-EXC-CLASS-LEAK", "Exception class name exposed to the caller", Severity.MEDIUM,
        "A09:2021", "web", "INFO_DISCLOSURE",
        "__class__.__name__ in a user-facing message reveals internals and library versions.",
        "Log the class name server-side; reply with a generic message.",
        (r"__class__\.__name__",), globs=("**/*.py",),
        exclude=(r"logger\.",),
    ),
    RegexRule(
        "CFG-ENV-ECHO", "Process environment serialized", Severity.HIGH, "A05:2021",
        "web", "INFO_DISCLOSURE",
        "Dumping os.environ (or copying it wholesale into a response) leaks every secret.",
        "Expose an explicit allowlist of non-secret keys; mask values before returning them.",
        (r"json\.dumps\(\s*(dict\()?\s*os\.environ", r"str\(\s*os\.environ\s*\)",
         r"[\"']env[\"']\s*:\s*dict\(os\.environ\)"),
        globs=("**/*.py",), line_exempt=NOSEc,
    ),
    RegexRule(
        "API-DOCS-EXPOSED", "API schema/docs served without gating", Severity.LOW,
        "A05:2021", "web", "MISCONFIG",
        "OpenAPI docs and /redoc enumerate every route; useful for an attacker's recon.",
        "Disable docs in production (docs_url=None, redoc_url=None) or gate them behind auth.",
        (r"=\s*FastAPI\(",), globs=("**/*.py",),
        exclude=(r"docs_url\s*=\s*None", r"openapi_url\s*=\s*None"),
        line_exempt=(r"^\s*#",), verdict="CANDIDATE", max_hits=2,
    ),
)

SSRF_RULES: tuple[RegexRule, ...] = (
    RegexRule(
        "SSRF-REDIRECT", "Outbound fetch allows redirects", Severity.MEDIUM, "A10:2021",
        "web", "SSRF",
        "Following redirects lets a validated public URL bounce to an internal address.",
        "Set allow_redirects=False and validate again after any manual redirect.",
        (r"allow_redirects\s*=\s*True",), globs=("**/*.py",), line_exempt=NOSEc,
    ),
    RegexRule(
        "SSRF-URL-USER", "User-supplied URL fetched without local validation",
        Severity.HIGH, "A10:2021", "web", "SSRF",
        "Fetching a URL taken directly from a request can reach cloud metadata and "
        "internal services.",
        "Validate with utils.url_validation._validate_url_safe and refuse redirects.",
        (r"(requests|session|client|httpx)\.?(get|post|stream|head)\(\s*(source_url|url|target_url|link)\b",
         r"urlopen\(\s*(source_url|url|target_url)\b"),
        globs=("**/*.py",),
        exclude=(r"_validate_url_safe", r"ALLOWED_HOSTS", r"validate_url"),
        verdict="CANDIDATE",
    ),
)

LOG_RULES: tuple[RegexRule, ...] = (
    RegexRule(
        "LOG-SECRET", "Secret-shaped value written to a log", Severity.MEDIUM, "A09:2021",
        "utils", "LOGGING",
        "Tokens, passwords and session strings written to logs end up in the platform log "
        "store and are often the easiest path to full account takeover.",
        "Log a constant identifier instead, or mask with a fixed-width placeholder.",
        (r"(logger|logging)\.\w+\(\s*f[\"'][^\"']*\{[^}]*(token|secret|password|passwd|"
         r"session_string|api_key|authorization|bearer)\b",
         r"(logger|logging)\.\w+\([^)]*,\s*\w*(token|secret|password|passwd|api_key|"
         r"session_string)\w*\s*[,)]",
         r"print\(\s*f[\"'][^\"']*\{[^}]*(token|password|session_string|api_key)\b"),
        globs=("**/*.py",),
        line_exempt=(*NOSEc, r"mask", r"MASK", r"\*{4}", r"redact", r"present", r"bool\(",
                     r"^\.\.\."),
        verdict="CANDIDATE", max_hits=3,
    ),
    RegexRule(
        "LOG-PII", "Personal data written to a log", Severity.LOW, "A09:2021",
        "utils", "LOGGING",
        "Phone numbers, chat ids and raw message payloads are personal data; "
        "they should not land in shared logs unmasked.",
        "Mask identifiers and log a hash/correlation id instead.",
        (r"(logger|logging)\.\w+\(\s*f[\"'][^\"']*\{(data|update|payload|message|phone)\b",
         r"(logger|logging)\.\w+\([^)]*,\s*phone\b",
         r"(logger|logging)\.\w+\(\s*f[\"'][^\"']*\b(phone|msisdn|phone_number)\b"),
        globs=("**/*.py",), line_exempt=(*NOSEc, r"mask", r"\*{4}"),
        verdict="CANDIDATE", max_hits=3,
    ),
)

AUTH_RULES: tuple[RegexRule, ...] = (
    RegexRule(
        "AUTH-QUERY-TOKEN", "Credential accepted from the query string", Severity.LOW,
        "A07:2021", "web", "AUTH",
        "Query-string tokens leak into access logs, browser history and Referer headers.",
        "Accept credentials only from a header or the request body.",
        (r"args\.get\(\s*[\"'](token|upload_token|debug_token|api_key)[\"']",
         r"query_params\.get\(\s*[\"'](token|upload_token|api_key)[\"']"),
        globs=("**/*.py",), verdict="CANDIDATE", max_hits=2,
    ),
    RegexRule(
        "CRYPTO-CMP", "Secret compared with == / !=", Severity.LOW, "A02:2021",
        "web", "CRYPTO",
        "Non-constant-time comparison leaks the secret one byte at a time to a patient "
        "attacker (relevant for long-lived tokens over many requests).",
        "Use hmac.compare_digest(incoming, expected).",
        (r"\bincoming\w*\s*(==|!=)\s*\w+", r"\b(token|secret)\w*\s*(==|!=)\s*\w+",
         r"(==|!=)\s*(API_KEY|WEBHOOK_SECRET|DIAG_TOKEN|UPLOAD_SECRET|DEBUG_SECRET)\b"),
        globs=("**/*.py",),
        # `compare_digest` is the stdlib primitive; `constant_time_eq` is this
        # project's wrapper around it (utils/secure_compare.py). Either one means
        # the comparison is already timing-safe.
        exclude=(r"compare_digest", r"constant_time_eq"),
        line_exempt=(r"^\s*#", r"is not None", r"None"), max_hits=2,
    ),
    RegexRule(
        "CRYPTO-RANDOM", "Non-cryptographic RNG near a secret", Severity.MEDIUM,
        "A02:2021", "utils", "CRYPTO",
        "random.* is seeded from the clock and predictable; secrets and tokens need "
        "the secrets module or os.urandom.",
        "Use secrets.token_urlsafe()/token_hex() or os.urandom().",
        (r"random\.(choice|choices|randint|random|sample|shuffle)\s*\([^)]*\)",
         ),
        globs=("**/*.py",),
        require=(r"(?i)(token|secret|nonce|salt|session|password|webhook)",),
        exclude=(r"secrets\.", r"# non-security", r"uuid"),
        line_exempt=(*NOSEc, r"^\s*#"), verdict="CANDIDATE",
    ),
)

DEP_RULE_IDS = ("DEP-CVE", "DEP-UNPINNED", "DEP-DIRECT-URL")


# ─────────────────────────────────────────────────────────────────────────────
# Repository context
# ─────────────────────────────────────────────────────────────────────────────

class Repo:
    def __init__(self, root: Path, profile: dict) -> None:
        self.root = root
        self.profile = profile
        cfg = profile["scan"]
        self.exclude_dirs = set(cfg["exclude_dirs"])
        self.exclude_globs = [self._compile_glob(g) for g in cfg.get("exclude_globs", [])]
        self.test_globs = [self._compile_glob(g) for g in cfg.get("test_globs", [])]
        self.max_bytes = cfg["max_file_bytes"]
        self.binary_exts = set(cfg["binary_extensions"])
        self._cache: dict[str, str | None] = {}
        self._docstrings: dict[str, set[int]] = {}

    def docstring_lines(self, path: Path) -> set[int]:
        """Line numbers covered by a module/class/function docstring.

        Docstring examples are documentation, not code: counting them produces
        the classic false positive (a usage sample that shows `@app.route(...)`
        or a redacted connection string).
        """
        rel = self.rel(path)
        if rel in self._docstrings:
            return self._docstrings[rel]
        lines: set[int] = set()
        if path.suffix == ".py":
            text = self.text(path)
            if text:
                try:
                    tree = ast.parse(text)
                except (SyntaxError, ValueError):
                    tree = None
                if tree is not None:
                    for node in ast.walk(tree):
                        if not isinstance(node, (ast.Module, ast.ClassDef,
                                                ast.FunctionDef, ast.AsyncFunctionDef)):
                            continue
                        body = getattr(node, "body", None) or []
                        if body and isinstance(body[0], ast.Expr) \
                                and isinstance(body[0].value, ast.Constant) \
                                and isinstance(body[0].value.value, str):
                            start = body[0].lineno
                            end = getattr(body[0], "end_lineno", start) or start
                            lines.update(range(start, end + 1))
        self._docstrings[rel] = lines
        return lines

    # ── file walking ────────────────────────────────────────────────────────
    def _excluded_dir(self, rel: str) -> bool:
        parts = Path(rel).parts
        return any(p in self.exclude_dirs for p in parts)

    def files(self, globs: tuple[str, ...] = ("**/*.py",)) -> list[Path]:
        out: list[Path] = []
        wants_all = "**/*" in globs
        patterns = [self._compile_glob(g) for g in globs]
        for dirpath, dirnames, filenames in os.walk(self.root):
            rel_dir = os.path.relpath(dirpath, self.root).replace("\\", "/")
            if rel_dir == ".":
                rel_dir = ""
            dirnames[:] = [d for d in dirnames if d not in self.exclude_dirs]
            for fn in filenames:
                rel = f"{rel_dir}/{fn}" if rel_dir else fn
                if self._excluded_dir(rel):
                    continue
                if any(p.match(rel) for p in self.exclude_globs):
                    continue
                if not wants_all and not any(p.match(rel) for p in patterns):
                    continue
                out.append(self.root / rel)
        return sorted(out)

    @staticmethod
    def _compile_glob(pattern: str) -> re.Pattern:
        rx = ""
        i = 0
        while i < len(pattern):
            ch = pattern[i]
            if ch == "*":
                if pattern[i:i + 3] == "**/":
                    rx += "(?:.*/)?"
                    i += 3
                    continue
                rx += "[^/]*"
            elif ch == "?":
                rx += "[^/]"
            else:
                rx += re.escape(ch)
            i += 1
        return re.compile(rx + "$")

    def is_test(self, rel: str) -> bool:
        return any(p.match(rel) for p in self.test_globs)

    def text(self, path: Path) -> str | None:
        rel = self.rel(path)
        if rel in self._cache:
            return self._cache[rel]
        try:
            if path.stat().st_size > self.max_bytes:
                self._cache[rel] = None
                return None
            self._cache[rel] = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            self._cache[rel] = None
        return self._cache[rel]

    def rel(self, path: Path) -> str:
        return str(path.relative_to(self.root)).replace("\\", "/")

    def read_rel(self, rel: str) -> str | None:
        p = self.root / rel
        return self.text(p) if p.is_file() else None

    def git(self, *args: str) -> str | None:
        try:
            proc = subprocess.run(
                ["git", *args], cwd=self.root, capture_output=True, text=True,
                timeout=25, encoding="utf-8", errors="replace",
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return proc.stdout if proc.returncode == 0 else None


# ─────────────────────────────────────────────────────────────────────────────
# Regex rule execution
# ─────────────────────────────────────────────────────────────────────────────

def mask(text: str, keep: int = 6) -> str:
    """Never re-leak a secret into the report."""
    if len(text) <= keep * 2:
        return "*" * len(text)
    return f"{text[:keep]}{'*' * 8}{text[-2:]}"


def run_regex_rule(rule: RegexRule, repo: Repo, include_tests: bool,
                   suppressions: list[dict], errors: list[str],
                   severity_override: Severity | None) -> list[Finding]:
    findings: list[Finding] = []
    pats = rule.compiled()
    requires = rule.compiled_list("require")
    excludes = rule.compiled_list("exclude")
    exempts = rule.compiled_list("line_exempt")
    for path in repo.files(rule.globs):
        rel = repo.rel(path)
        if repo.is_test(rel) and not (include_tests and rule.include_tests):
            continue
        if suppressed(rule.id, rel, suppressions):
            continue
        text = repo.text(path)
        if text is None:
            continue
        if any(rx.search(text) for rx in excludes):
            continue
        if requires and not all(rx.search(text) for rx in requires):
            continue
        hits = 0
        # Secrets in a docstring are still secrets; other rules skip docstring
        # examples so documentation snippets don't read as live code.
        doc = set() if rule.category == "SECRETS" else repo.docstring_lines(path)
        for lineno, line in enumerate(text.splitlines(), 1):
            if hits >= rule.max_hits:
                break
            if lineno in doc:
                continue
            for pat in pats:
                m = pat.search(line)
                if not m:
                    continue
                if any(rx.search(line) for rx in exempts):
                    break
                snippet = m.group(0).strip()
                if rule.mask:
                    snippet = pat.sub(lambda mm: mask(mm.group(0)), line.strip(), count=1)
                    snippet = snippet[:160]
                else:
                    snippet = line.strip()[:160]
                findings.append(Finding(
                    rule=rule.id, title=rule.title,
                    severity=severity_override or rule.severity,
                    owasp=rule.owasp, layer=rule.layer, category=rule.category,
                    verdict=rule.verdict, description=rule.desc,
                    evidence=snippet, file=rel, line=lineno, fix=rule.fix,
                ))
                hits += 1
                break
    return findings


def suppressed(rule_id: str, rel: str, suppressions: list[dict]) -> bool:
    for sup in suppressions:
        if sup.get("rule") not in (None, "*", rule_id):
            continue
        glob = sup.get("glob", "**")
        rx = Repo._compile_glob(glob)
        if rx.match(rel):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Semantic checks (indentation/structure aware)
# ─────────────────────────────────────────────────────────────────────────────

def _indented_block(lines: list[str], start: int) -> list[str]:
    """Return the lines of the block that starts at `start` (indent-scoped)."""
    base = len(lines[start]) - len(lines[start].lstrip())
    body: list[str] = []
    for line in lines[start + 1:]:
        if not line.strip():
            body.append(line)
            continue
        indent = len(line) - len(line.lstrip())
        if indent <= base:
            break
        body.append(line)
    return body


def _defs(text: str) -> list[tuple[str, int, list[str]]]:
    """(name, 0-based line index, body lines) for every function definition."""
    lines = text.splitlines()
    out = []
    for i, line in enumerate(lines):
        m = re.match(r"\s*(?:async\s+)?def\s+(\w+)\s*\(", line)
        if m:
            out.append((m.group(1), i, _indented_block(lines, i)))
    return out


SECRET_ENV_VARS = ("WEBHOOK_SECRET", "UPLOAD_SECRET", "DEBUG_SECRET", "DIAG_TOKEN",
                   "TELEGRAM_SECRET_TOKEN")

ROUTE_RE = re.compile(
    r"@\s*\w+\s*\.\s*(?:get|post|put|patch|delete|api_route|route|websocket)\s*\(\s*"
    r"[\"']([^\"']+)[\"']"
)
ROUTE_PRIVILEGED = re.compile(r"(debug|diag|internal|admin|metrics|presign|enqueue|upload|get_input|get_output)")
ROUTE_GUARDED = re.compile(r"(unauthorized|401|403|check_limit|_verify|token|secret|compare_digest|Depends\()")


def check_fail_open_auth(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """`if SECRET:` around a token check means the surface is open when unset."""
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if suppressed("AC-WEBHOOK-FAILOPEN", rel, suppressions):
            continue
        text = repo.text(path)
        if text is None or not text:
            continue
        lines = text.splitlines()
        env_map: dict[str, str] = {}
        for line in lines:
            m = re.search(r"(\w+)\s*=\s*os\.(?:environ\.get|getenv)\(\s*[\"']([A-Z_]+)[\"']", line)
            if m:
                env_map[m.group(1)] = m.group(2)
        for i, line in enumerate(lines):
            m = re.match(r"\s*if\s+(not\s+)?(\w+)\s*:\s*$", line)
            if not m:
                continue
            if m.group(1):
                # `if not SECRET:` is the safe shape: it rejects when the secret
                # is missing. Only `if SECRET:` can silently fail open.
                continue
            var = m.group(2)
            env_name = env_map.get(var)
            if not env_name and re.fullmatch(r"[A-Z][A-Z0-9_]*(SECRET|TOKEN)[A-Z0-9_]*", var):
                # e.g. `WEBHOOK_SECRET` imported from config rather than read here
                env_name = var
            if not env_name or not any(s in env_name for s in SECRET_ENV_VARS):
                continue
            block = "\n".join(_indented_block(lines, i))
            rejects = re.search(r"(unauthorized|401|403|HTTPException|raise\s+\w*Error|return\s+.*(?:401|403))", block)
            if not rejects:
                continue
            # An `else:` that still rejects = fail-closed; no else = fail-open.
            has_else_reject = False
            for j in range(i + 1, len(lines)):
                if re.match(r"\s*else\s*:", lines[j]):
                    else_block = "\n".join(_indented_block(lines, j))
                    has_else_reject = bool(re.search(
                        r"(unauthorized|401|403|HTTPException|raise\s+\w*Error)", else_block))
                    break
                if lines[j].strip() and len(lines[j]) - len(lines[j].lstrip()) <= len(line) - len(line.lstrip()):
                    break
            if has_else_reject:
                continue
            critical = env_name in ("WEBHOOK_SECRET", "TELEGRAM_SECRET_TOKEN", "DIAG_TOKEN")
            out.append(Finding(
                rule="AC-WEBHOOK-FAILOPEN",
                title=f"Fail-open auth guard: {env_name} optional",
                severity=Severity.HIGH if critical else Severity.MEDIUM,
                owasp="A07:2021", layer="web", category="ACCESS_CONTROL",
                verdict="CONFIRMED",
                description=(f"The guard `if {var}:` only enforces authentication when {env_name} is "
                             "set, so with the variable unset — a fresh deploy, a mis-typed name, "
                             "or a cleared platform variable — this surface becomes fully "
                             "unauthenticated. "
                             + ("For the Telegram webhook that means anyone who learns the path "
                                "can inject forged updates and drive the bot."
                                if critical else
                                "The code comments document this as an optional control, which "
                                "makes the fail-open state a deployment accident waiting to happen.")),
                evidence=line.strip()[:160], file=rel, line=i + 1,
                fix=(f"Fail closed: refuse the request when {env_name} is missing, and compare "
                     "with hmac.compare_digest."),
            ))
    return out


def check_cors(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        text = repo.text(path)
        if not text or suppressed("AC-CORS-WILDCARD", rel, suppressions):
            continue
        for i, line in enumerate(text.splitlines(), 1):
            wildcard = re.search(r"allow_origins\s*=\s*\[\s*[\"']\*[\"']\s*\]", line) or \
                re.match(r"\s*CORS\(\s*(app|application)?\s*\)\s*$", line)
            if not wildcard:
                continue
            creds = "allow_credentials" in text and "True" in text
            out.append(Finding(
                rule="AC-CORS-WILDCARD", title="Permissive CORS policy",
                severity=Severity.MEDIUM, owasp="A05:2021", layer="web", category="MISCONFIG",
                verdict="CANDIDATE",
                description=("CORS is enabled without an origin allowlist, so any website can "
                             "script the public job API from a victim browser"
                             + (" with credentials" if creds else "") + "."),
                evidence=line.strip()[:160], file=rel, line=i,
                fix="Pass origins=<allowlist> (and support_credentials only with that allowlist).",
            ))
            break
    return out


def check_routes_auth(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Privileged-looking routes whose handler never performs an auth check."""
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        text = repo.text(path)
        if not text:
            continue
        lines = text.splitlines()
        doc = repo.docstring_lines(path)
        for i, line in enumerate(lines):
            m = ROUTE_RE.search(line)
            if not m:
                continue
            route = m.group(1)
            if not ROUTE_PRIVILEGED.search(route):
                continue
            if suppressed("AC-DEBUG-ENDPOINT", rel, suppressions) or (i + 1) in doc:
                continue
            # Resolve the decorated function, then take *its* block: the
            # decorator and the `def` line share an indent, so the block that
            # belongs to the route starts at the def, not at the decorator.
            base = len(line) - len(line.lstrip())
            def_idx = None
            for j in range(i + 1, min(i + 8, len(lines))):
                stripped = lines[j].strip()
                if not stripped:
                    continue
                indent = len(lines[j]) - len(lines[j].lstrip())
                if indent <= base and re.match(r"(async\s+)?def\s", stripped):
                    def_idx = j
                    break
                if indent <= base:
                    break
            if def_idx is None:
                continue
            block = _indented_block(lines, def_idx)
            body = "\n".join(block)
            if ROUTE_GUARDED.search(body):
                continue
            # A pure redirect delegates to the mounted app, which enforces its own
            # auth — the redirect target is not the security boundary.
            non_empty = [ln for ln in block if ln.strip()]
            if non_empty and all("RedirectResponse(" in ln or ln.strip().startswith(("\"\"\"", "'''", "#"))
                                 for ln in non_empty):
                continue
            sev = Severity.MEDIUM if route.strip("/") in ("metrics",) else Severity.HIGH
            out.append(Finding(
                rule="AC-DEBUG-ENDPOINT",
                title=f"Privileged route without an auth check: {route}",
                severity=sev, owasp="A01:2021", layer="web", category="ACCESS_CONTROL",
                verdict="CANDIDATE",
                description=(f"`{route}` looks like a diagnostic/privileged surface but its handler "
                             "contains no token check, auth dependency or rate limiter, so it is "
                             "reachable by anyone who can reach the app."),
                evidence=f"route {route}", file=rel, line=i + 1,
                fix=("Require an admin/diagnostic token (fail closed when unset), or remove the "
                     "route from production builds."),
            ))
    return out


def check_admin_guard(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    out: list[Finding] = []
    name_re = re.compile(r"(?i)(admin|owner|broadcast|revoke|unban|allowlist|_allow|kick)")
    check_re = re.compile(r"(is_admin_user|is_owner|is_authorized|ADMIN_USER_ID|ADMIN_USER_IDS|is_user_allowed|require_admin)")
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if suppressed("AC-ADMIN-GUARD", rel, suppressions):
            continue
        text = repo.text(path)
        if not text or "reply_text" not in text:
            continue
        for name, idx, body in _defs(text):
            if name.startswith("_") or not name_re.search(name):
                continue
            joined = "\n".join(body)
            if "reply_text" not in joined and "context.bot" not in joined:
                continue
            if check_re.search(joined):
                continue
            out.append(Finding(
                rule="AC-ADMIN-GUARD", title=f"Privileged handler without an admin check: {name}()",
                severity=Severity.HIGH, owasp="A01:2021", layer="bot", category="ACCESS_CONTROL",
                verdict="CANDIDATE",
                description=(f"`{name}` is named like a privileged command and replies to the user, "
                             "but never calls config.is_admin_user/is_owner or checks ADMIN_USER_ID."),
                evidence=f"def {name}() — no is_admin_user/is_owner guard in body",
                file=rel, line=idx + 1,
                fix="Gate on config.is_admin_user(uid) / config.is_owner(uid) before any side effect.",
            ))
    return out


def check_rate_limit(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """A web module with routes but no rate limiting anywhere in it."""
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        text = repo.text(path)
        if not text or suppressed("DES-RATE-LIMIT", rel, suppressions):
            continue
        doc = repo.docstring_lines(path)
        code_lines = [ln for n, ln in enumerate(text.splitlines(), 1) if n not in doc]
        routes = ROUTE_RE.findall("\n".join(code_lines))
        if not routes:
            continue
        if re.search(r"(rate_limit|ratelimit|RateLimiter|check_limit|throttle)", text, re.I):
            continue
        out.append(Finding(
            rule="DES-RATE-LIMIT", title="Web routes without rate limiting",
            severity=Severity.MEDIUM, owasp="A04:2021", layer="web", category="DESIGN",
            verdict="CANDIDATE",
            description=(f"{len(routes)} route(s) are defined in this module and no rate limiter "
                         "appears anywhere in it, so the surface is open to cheap flooding."),
            evidence=f"{len(routes)} route(s), no limiter in file: {', '.join(routes[:4])}",
            file=rel, line=1,
            fix="Apply the shared web_rate_limiter (or SlowAPI) to every route.",
        ))
    return out


def check_ssrf_validator(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Audit the shared URL validator against the A10 checklist in the skill file."""
    rel = "utils/url_validation.py"
    text = repo.read_rel(rel)
    if not text:
        return [Finding(
            rule="SSRF-VALIDATOR-WEAK", title="Shared SSRF validator missing",
            severity=Severity.HIGH, owasp="A10:2021", layer="utils", category="SSRF",
            verdict="CANDIDATE",
            description="The A10 checklist requires one shared _validate_url_safe helper; it was not found.",
            evidence=f"{rel} not found", file=rel, line=1,
            fix="Add utils/url_validation.py and import it in every URL-fetching call site.",
        )]
    checks = {
        "scheme allowlist": r"scheme\s+(not\s+)?in\s*\(|scheme\s*==\s*[\"']https?[\"']",
        "empty hostname rejected": r"netloc|hostname",
        "private/loopback blocked": r"is_private|is_loopback",
        "link-local/multicast blocked": r"is_link_local|is_multicast|is_reserved",
        "DNS resolution of hostnames": r"getaddrinfo|gethostbyname|socket\.",
    }
    missing = [name for name, rx in checks.items() if not re.search(rx, text)]
    if not missing:
        return []
    return [Finding(
        rule="SSRF-VALIDATOR-WEAK", title="SSRF validator is incomplete",
        severity=Severity.MEDIUM, owasp="A10:2021", layer="utils", category="SSRF",
        verdict="CANDIDATE",
        description=("The A10 skill checklist is not fully implemented in the shared validator. "
                     f"Missing: {', '.join(missing)}. A hostname that resolves to an internal "
                     "address still passes a literal-IP-only check."),
        evidence=f"missing checks: {', '.join(missing)}", file=rel, line=1,
        fix="Resolve the hostname and reject private/loopback/link-local/multicast results before fetching.",
    )]


def check_dependencies(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    out: list[Finding] = []
    unpinned_hits = 0
    for req_rel in profile["cve"]["requirements"]:
        text = repo.read_rel(req_rel)
        if not text:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            raw = line.strip()
            if not raw or raw.startswith("#") or raw.startswith("-"):
                continue
            if re.search(r"@\s*(https?://|git\+)", raw):
                if suppressed("DEP-DIRECT-URL", req_rel, suppressions):
                    continue
                out.append(Finding(
                    rule="DEP-DIRECT-URL", title="Dependency installed from a URL/git ref",
                    severity=Severity.MEDIUM, owasp="A08:2021", layer="deps", category="SUPPLY_CHAIN",
                    verdict="CONFIRMED",
                    description="Direct URL/VCS requirements bypass index integrity checks and pin no artifact hash.",
                    evidence=raw[:120], file=req_rel, line=i,
                    fix="Depend on a released version from the index, or pin a commit plus a hash.",
                ))
                continue
            spec = raw.split(";")[0].strip()
            if re.search(r"[<>]=?|==", spec) and not re.search(r"<", spec):
                if suppressed("DEP-UNPINNED", req_rel, suppressions):
                    continue
                unpinned_hits += 1
                if unpinned_hits > 8:
                    continue
                out.append(Finding(
                    rule="DEP-UNPINNED", title="Dependency without an upper bound",
                    severity=Severity.LOW, owasp="A06:2021", layer="deps", category="SUPPLY_CHAIN",
                    verdict="CONFIRMED",
                    description=("A lower-bound-only specifier lets a future major release — or a "
                                 "compromised release — install silently on the next build."),
                    evidence=spec[:120], file=req_rel, line=i,
                    fix="Add an upper bound (<next-major) or install from a hash-pinned lock file.",
                ))
    return out


def check_repo_hygiene(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    out: list[Finding] = []

    # 1) .env / session files tracked by git
    for candidate in (".env", "telethon_ingest.session.json"):
        if repo.git("ls-files", "--error-unmatch", candidate) is not None:
            out.append(Finding(
                rule="INFRA-ENV-TRACKED", title=f"Secret-bearing file is tracked by git: {candidate}",
                severity=Severity.CRITICAL, owasp="A02:2021", layer="infra", category="SECRETS",
                verdict="CONFIRMED",
                description="A tracked dotenv/session file ships every production secret with the repo.",
                evidence=f"git ls-files --error-unmatch {candidate} succeeded", file=candidate, line=1,
                fix="git rm --cached it, add it to .gitignore, and rotate every secret it contained.",
            ))

    # 2) .gitignore coverage
    gi = repo.read_rel(".gitignore")
    if gi:
        # label -> acceptable alternatives (any one satisfies the requirement)
        required = {
            "dotenv files": (".env",),
            "telethon/pyrogram session files": ("*.session", "telethon_ingest.session", "*.session.json"),
            "private key material": ("certs/", "*.pem", "*.key"),
            "runtime storage": ("storage",),
            "logs": ("logs",),
        }
        missing = [label for label, alts in required.items()
                   if not any(a.lower() in gi.lower() for a in alts)]
        if missing:
            out.append(Finding(
                rule="INFRA-GITIGNORE", title=".gitignore does not exclude secret/runtime paths",
                severity=Severity.MEDIUM, owasp="A05:2021", layer="infra", category="MISCONFIG",
                verdict="CONFIRMED",
                description=("Missing ignore rules make it likely that a secret or user media file "
                             "gets committed. The session-file rule matters most: the app persists "
                             "Telethon/Pyrogram session strings to JSON files next to the repo root, "
                             "and a session string is a full userbot login."),
                evidence=f"missing: {', '.join(missing)}", file=".gitignore", line=1,
                fix="Add `*.session*` and `telethon_ingest.session*` (plus any other missing pattern).",
            ))

    # 3) .dockerignore coverage
    di = repo.read_rel(".dockerignore")
    if di is not None and ".env" not in di:
        out.append(Finding(
            rule="INFRA-DOCKERIGNORE", title="Docker build context includes .env",
            severity=Severity.HIGH, owasp="A02:2021", layer="infra", category="SECRETS",
            verdict="CONFIRMED",
            description="Without .env in .dockerignore the dotenv file is copied into the image layers.",
            evidence=".dockerignore has no .env entry", file=".dockerignore", line=1,
            fix="Add .env* to .dockerignore (and never COPY it in the Dockerfile).",
        ))

    # 4) Dockerfile: root user + secrets in ENV/ARG
    df = repo.read_rel("Dockerfile")
    if df is not None:
        if not re.search(r"(?m)^\s*USER\s+\S+", df):
            out.append(Finding(
                rule="INFRA-DOCKER-ROOT", title="Container runs as root",
                severity=Severity.MEDIUM, owasp="A05:2021", layer="infra", category="MISCONFIG",
                verdict="CONFIRMED",
                description="No USER directive: a container escape or RCE runs with root in the image.",
                evidence="no USER directive in Dockerfile", file="Dockerfile", line=1,
                fix="Create an unprivileged user and switch to it before CMD/ENTRYPOINT.",
            ))
        for i, line in enumerate(df.splitlines(), 1):
            if re.match(r"\s*(ENV|ARG)\s+\w*(SECRET|TOKEN|KEY|PASSWORD)\w*\s*=", line, re.I):
                out.append(Finding(
                    rule="INFRA-DOCKER-SECRET", title="Secret baked in via Dockerfile ENV/ARG",
                    severity=Severity.HIGH, owasp="A02:2021", layer="infra", category="SECRETS",
                    verdict="CONFIRMED",
                    description="ENV/ARG values persist in the image history and are readable by anyone with the image.",
                    evidence=mask(line.strip()), file="Dockerfile", line=i,
                    fix="Pass secrets at runtime (platform secret store, BuildKit secret mounts).",
                ))

    # 5) compose: databases/queues published on all interfaces
    for rel in ("docker-compose.yml", "docker-compose.eventbus.yml", "docker-compose.fetcher.yml"):
        text = repo.read_rel(rel)
        if not text:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            m = re.search(r"[\"']?(\d{2,5}):(\d{2,5})[\"']?", line)
            if not m:
                continue
            port = m.group(2)
            if port in ("6379", "5432", "27017", "5672", "9092", "3306", "9000", "9001"):
                if "127.0.0.1" in line:
                    continue
                out.append(Finding(
                    rule="INFRA-COMPOSE-PORT", title=f"Service port {port} published on all interfaces",
                    severity=Severity.HIGH, owasp="A05:2021", layer="infra", category="MISCONFIG",
                    verdict="CONFIRMED",
                    description="Publishing datastore/queue/broker ports to the host exposes them to the whole network.",
                    evidence=line.strip()[:120], file=rel, line=i,
                    fix=f"Bind explicitly: 127.0.0.1:{port}:{port}, or keep the service on the internal network only.",
                ))

    # 6) CI: unpinned third-party actions (cap the list — they are one class of risk)
    ci_hits = 0
    for path in repo.files((".github/workflows/*.yml", ".github/workflows/*.yaml")):
        if ci_hits >= 5:
            break
        rel = repo.rel(path)
        text = repo.text(path)
        if not text:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            m = re.match(r"\s*-?\s*uses:\s*([^\s#]+)", line)
            if not m:
                continue
            ref = m.group(1)
            if ref.startswith("./") or "@" not in ref or ci_hits >= 5:
                continue
            _, _, version = ref.partition("@")
            if not re.fullmatch(r"[0-9a-f]{40}", version):
                ci_hits += 1
                out.append(Finding(
                    rule="INT-CI-UNPINNED", title=f"CI action not pinned to a commit: {ref}",
                    severity=Severity.MEDIUM, owasp="A08:2021", layer="infra", category="SUPPLY_CHAIN",
                    verdict="CONFIRMED",
                    description="A moving tag on a third-party action lets an upstream compromise run in CI with repo secrets.",
                    evidence=line.strip()[:120], file=rel, line=i,
                    fix="Pin to the full 40-char commit SHA (Dependabot can keep the comment tag).",
                ))

    # 7) reload/debug in the process launcher
    for rel in ("Procfile", "start.sh", "railway.json", "docker-compose.yml"):
        text = repo.read_rel(rel)
        if not text:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if re.search(r"--reload\b|--debug\b|FLASK_DEBUG\s*=\s*1", line):
                out.append(Finding(
                    rule="INFRA-DEBUG-LAUNCH", title="Auto-reload/debug enabled in the launcher",
                    severity=Severity.HIGH, owasp="A05:2021", layer="infra", category="MISCONFIG",
                    verdict="CONFIRMED",
                    description="Auto-reload in a production process enables the debugger surface and restarts on file writes.",
                    evidence=line.strip()[:120], file=rel, line=i,
                    fix="Run without --reload/--debug in production.",
                ))
    return out


def check_supply_chain_pins(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Flag dependencies installed without hash verification.

    Two shapes satisfy this: requirements.txt pinned with hashes directly, or a
    hash-pinned lock that the install path actually consumes with
    ``--require-hashes``. The second half of that check matters as much as the
    first — a lock nobody installs is a document, not a control — so the
    Dockerfile has to name the lock on a ``--require-hashes`` install line.
    """
    out: list[Finding] = []
    text = repo.read_rel("requirements.txt") or ""
    # Honour an accepted deviation like every other check does, instead of
    # reporting a finding the profile has already adjudicated by hand.
    if suppressed("DEP-NO-HASHES", "requirements.txt", suppressions):
        return []
    if "--require-hashes" in text or "--hash=" in text:
        return out
    lock = repo.read_rel("requirements.lock") or ""
    dockerfile = repo.read_rel("Dockerfile") or ""
    lock_pinned = "--hash=sha256:" in lock
    lock_installed = "requirements.lock" in dockerfile and "--require-hashes" in dockerfile
    if lock_pinned and lock_installed:
        return out
    if lock_pinned and not lock_installed:
        evidence = "requirements.lock is hash-pinned but no install line consumes it with --require-hashes"
    elif lock and not lock_pinned:
        evidence = "requirements.lock exists but carries no --hash= entries"
    else:
        evidence = "no --hash= entries in requirements.txt and no hash-pinned lock"
    out.append(Finding(
        rule="DEP-NO-HASHES", title="dependencies are not hash-pinned",
        severity=Severity.LOW, owasp="A08:2021", layer="deps", category="SUPPLY_CHAIN",
        verdict="CONFIRMED",
        description="Without hashes, a compromised index or a tampered wheel installs silently.",
        evidence=evidence, file="requirements.txt", line=1,
        fix="Generate a hash-pinned lock (uv pip compile --generate-hashes, resolved for the deploy platform) and install it with --require-hashes.",
    ))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Catalog-completeness checks
#
# Every rule id referenced by the skill catalog in the profile must exist here,
# otherwise the coverage table would claim a flaw class is "checked" when no
# code implements it. These checks close that gap for the Python/Telegram/FastAPI
# surface this bot actually has.
# ─────────────────────────────────────────────────────────────────────────────

def _finding(rule_id: str, title: str, severity: Severity, owasp: str, layer: str,
             category: str, verdict: str, desc: str, evidence: str, file: str, line: int,
             fix: str) -> Finding:
    return Finding(rule=rule_id, title=title, severity=severity, owasp=owasp, layer=layer,
                   category=category, verdict=verdict, description=desc, evidence=evidence,
                   file=file, line=line, fix=fix)


HTTP_CALL_RE = re.compile(
    r"\b(requests|session|client|httpx|aiohttp|_session)\.(get|post|put|patch|delete|head|request)\("
)
# The first argument must look like a URL/target, not a dict key: `session.get("k")`
# on a plain dict is not an outbound request.
URL_ARG_RE = re.compile(r"[\(,]\s*(f?[\"']http|url|source_url|target_url|link|endpoint|webhook_url|api_url)")


def check_outbound_timeouts(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Outbound HTTP without a timeout can hang a worker forever (A04)."""
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("DES-NO-TIMEOUT", rel, suppressions):
            continue
        text = repo.text(path)
        if not text:
            continue
        lines = text.splitlines()
        for i, line in enumerate(lines):
            if not HTTP_CALL_RE.search(line) or not URL_ARG_RE.search(line):
                continue
            statement = " ".join(lines[i:i + 6])
            if "timeout" in statement or "noqa" in line or "nosec" in line:
                continue
            if len(out) >= 6:
                return out
            out.append(_finding(
                "DES-NO-TIMEOUT", "Outbound HTTP call without a timeout", Severity.MEDIUM,
                "A04:2021", "web", "DESIGN", "CANDIDATE",
                "An outbound request with no timeout can block the handler or worker task "
                "indefinitely, which is a cheap denial-of-service on a shared event loop.",
                line.strip()[:160], rel, i + 1,
                "Pass timeout=<seconds> (and a retry/backoff policy) on every outbound call.",
            ))
    return out


def check_flood_wait(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Telegram API surface without FloodWait/RetryAfter handling (A04)."""
    out: list[Finding] = []
    api_re = re.compile(
        r"\.(send_message|send_photo|send_video|send_audio|send_document|send_media_group|"
        r"forward_message|copy_message|edit_message_text|edit_message_reply_markup|"
        r"answer_callback_query|delete_message|send_chat_action)\("
        # ``get_file`` also exists on storage backends (S3/MinIO/...), where it is
        # an object read rather than a Telegram call, so it only counts on a bot.
        r"|[\w.]*[Bb]ot\w*\.get_file\("
    )
    flood_re = re.compile(r"(RetryAfter|FloodWait|retry_after|flood_wait)", re.I)
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("BOT-FLOOD-WAIT", rel, suppressions):
            continue
        text = repo.text(path)
        if not text:
            continue
        # Docstrings are documentation, not call sites: a usage example in a
        # module header is not an unguarded Telegram call, and a docstring that
        # mentions RetryAfter does not mean the code handles it.
        doc = repo.docstring_lines(path)
        code = "\n".join(ln for n, ln in enumerate(text.splitlines(), 1) if n not in doc)
        if not api_re.search(code) or flood_re.search(code):
            continue
        if len(out) >= 3:
            break
        out.append(_finding(
            "BOT-FLOOD-WAIT", "Telegram API calls without FloodWait handling", Severity.MEDIUM,
            "A04:2021", "bot", "DESIGN", "CANDIDATE",
            "This module issues Telegram API calls but never handles FloodWait/RetryAfter. "
            "A flood wait it ignores becomes failed deliveries and, if retried in a tight "
            "loop, an escalating restriction.",
            f"api calls present, no FloodWait/RetryAfter in {rel}", rel, 1,
            "Catch FloodWait/RetryAfter, sleep the given seconds, and back off exponentially.",
        ))
        if len(out) >= 6:
            break
    return out


def check_url_length(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """User-supplied URL accepted with no length cap (ReDoS/huge-URL)."""
    out: list[Finding] = []
    url_re = re.compile(r"(source_url|url|link)\s*=\s*(request|data|body|payload)[\w.]*\.get\(")
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("DES-URL-LENGTH", rel, suppressions):
            continue
        text = repo.text(path)
        if not text:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if not url_re.search(line):
                continue
            if re.search(r"len\(|\[:\s*\d|max_length|MAX_URL", text):
                continue
            out.append(_finding(
                "DES-URL-LENGTH", "User-supplied URL is not length-capped", Severity.LOW,
                "A04:2021", "web", "DESIGN", "CANDIDATE",
                "The URL comes straight from the request body with no length bound, so a "
                "multi-megabyte value is accepted before any validation runs.",
                line.strip()[:160], rel, i,
                "Reject URLs longer than a few KB before validating or fetching them.",
            ))
            break
    return out


def check_trusted_host(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """FastAPI app behind a proxy without TrustedHostMiddleware (PACK-S03)."""
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("CFG-TRUSTED-HOST", rel, suppressions):
            continue
        text = repo.text(path)
        if not text or not re.search(r"=\s*FastAPI\(", text):
            continue
        if re.search(r"TrustedHostMiddleware|allowed_hosts|ALLOWED_HOSTS", text):
            continue
        out.append(_finding(
            "CFG-TRUSTED-HOST", "FastAPI app accepts any Host header", Severity.LOW,
            "A05:2021", "web", "MISCONFIG", "CANDIDATE",
            "No TrustedHostMiddleware and no ALLOWED_HOSTS check: behind a proxy the Host "
            "header is attacker-controlled, which enables host-header poisoning of any "
            "absolute URL the app builds (webhook registration, links, redirects).",
            "FastAPI app with no TrustedHostMiddleware/allowed_hosts", rel, 1,
            "Add TrustedHostMiddleware(allowed_hosts=[...]) sourced from an env allowlist.",
        ))
    return out


def check_session_file_hygiene(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Telethon/Pyrogram session JSON files must not be committable (A07)."""
    ts = repo.read_rel("utils/telethon_session.py") or ""
    if not ts:
        return []
    if suppressed("AUTH-SESSION-FILE", "utils/telethon_session.py", suppressions):
        return []
    m = re.search(r"telethon_ingest\.session", ts) or re.search(r"\.session\.json", ts)
    if not m:
        return []
    sample = "telethon_ingest.session.12345.json"
    ignored = repo.git("check-ignore", "-q", sample) is not None
    if ignored:
        return []
    return [_finding(
        "AUTH-SESSION-FILE", "Userbot session files are writable next to the repo root and not ignored",
        Severity.MEDIUM, "A07:2021", "infra", "SECRETS", "CONFIRMED",
        "The session store writes `telethon_ingest.session.<user>.json` beside the app. A "
        "session string is a complete userbot login (read and send as the user), so if one "
        "is ever committed or copied into a build artifact it is an account takeover. "
        "`git check-ignore` reports the pattern as un-ignored.",
        f"sample {sample} is not matched by .gitignore", "utils/telethon_session.py", 1,
        "Add `*.session*` / `telethon_ingest.session*` to .gitignore and prefer writing "
        "session state outside the repository tree.",
    )]


def check_payload_identity_fields(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Role/identity taken from the request payload (PACK-S06, PACK-S07)."""
    out: list[Finding] = []
    role_re = re.compile(
        r"(request|data|body|payload)[\w.]*\.(get|\[)\s*\(?\s*[\"']"
        r"(role|is_admin|is_staff|permissions|user_id|owner_id|telegram_id)[\"']"
    )
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("AC-ROLE-FROM-PAYLOAD", rel, suppressions):
            continue
        text = repo.text(path)
        if not text or "request" not in text:
            continue
        doc = repo.docstring_lines(path)
        for i, line in enumerate(text.splitlines(), 1):
            if i in doc or not role_re.search(line):
                continue
            privileged = re.search(r"(role|is_admin|is_staff|permissions)", line) is not None
            rule = "AC-ROLE-FROM-PAYLOAD" if privileged else "AC-USERID-FROM-PAYLOAD"
            out.append(_finding(
                rule,
                "Client-supplied " + ("role/permission" if privileged else "user identity")
                + " field trusted",
                Severity.HIGH if privileged else Severity.MEDIUM,
                "A01:2021", "web", "ACCESS_CONTROL", "CANDIDATE",
                "A privilege or identity field is read directly from the request payload. "
                "Role selection and caller identity must be derived server-side from the "
                "authenticated session, never from the body the caller controls.",
                line.strip()[:160], rel, i,
                "Drop the field from the accepted schema and resolve the role/id from the "
                "server-side session record.",
            ))
            if len(out) >= 6:
                return out
    return out


def check_job_idor(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Job-keyed routes where the job id alone is the authorization check.

    Two controls discharge this: an ownership field, or a per-job capability
    token (utils/job_access.py), which is strictly stronger — it is neither
    guessable from the id nor shared between jobs, and it dies with the job.
    """
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("AC-IDOR-JOB", rel, suppressions):
            continue
        text = repo.text(path)
        if not text:
            continue
        doc = repo.docstring_lines(path)
        code = "\n".join(ln for n, ln in enumerate(text.splitlines(), 1) if n not in doc)
        job_routes = re.findall(r"[\"'](/(?:status|download|events|get_output)/\{?(?:job_id|id)\}?)[\"']", code)
        if not job_routes:
            continue
        if re.search(
            r"(user_id|owner|owner_id|allowed_user|is_user_allowed|job_token_ok|access_token_hash)",
            code,
        ):
            continue
        out.append(_finding(
            "AC-IDOR-JOB", "Job-keyed route with no ownership verification", Severity.MEDIUM,
            "A01:2021", "web", "ACCESS_CONTROL", "CANDIDATE",
            "These routes expose a job only by its opaque id, and neither consult the "
            "job's owner nor require a per-job capability token, so possession of the id "
            "is the entire authorization check. That is acceptable while ids are "
            "unguessable AND never shared, but it is one leak away from cross-user "
            "access.",
            f"routes: {', '.join(sorted(set(job_routes))[:4])}", rel, 1,
            "Compare the job's user_id against the authenticated caller before returning "
            "status, events or output bytes.",
        ))
    return out


def check_body_limit(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Upload/body handling with no size ceiling (PACK-S08, PACK-S13)."""
    out: list[Finding] = []
    upload_re = re.compile(r"(request\.files|request\.form|await request\.body\(|UploadFile)")
    limit_re = re.compile(r"(MAX_FILE_SIZE|MAX_CONTENT_LENGTH|content_length|max_size|max_upload)", re.I)
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("DES-BODY-LIMIT", rel, suppressions):
            continue
        text = repo.text(path)
        if not text or not upload_re.search(text) or limit_re.search(text):
            continue
        out.append(_finding(
            "DES-BODY-LIMIT", "Upload path without a body-size ceiling", Severity.MEDIUM,
            "A04:2021", "web", "DESIGN", "CANDIDATE",
            "This module accepts uploads but contains no size limit check, so the platform "
            "memory/disk ceiling is the only bound.",
            "upload handling present, no MAX_FILE_SIZE/content-length check", rel, 1,
            "Reject requests whose declared size exceeds MAX_FILE_SIZE before reading the body.",
        ))
    return out


def check_filename_sanitizer(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """A shared filename sanitizer/extension allowlist must exist (TIF-SANITIZE-01)."""
    fu = repo.read_rel("utils/file_utils.py") or ""
    if not fu:
        return []
    if re.search(r"(def safe_extension|def _?sanitize_filename|ALLOWED_(MEDIA_)?EXTENSIONS)", fu):
        return []
    return [_finding(
        "SANITIZE-FILENAME", "No shared filename sanitizer / extension allowlist", Severity.HIGH,
        "A03:2021", "utils", "INJECTION", "CANDIDATE",
        "User-supplied names reach the filesystem and ffmpeg argv. Without one shared "
        "sanitizer plus an extension allowlist, each call site invents its own (weaker) check.",
        "utils/file_utils.py has no safe_extension/_sanitize_filename/allowlist",
        "utils/file_utils.py", 1,
        "Add safe_extension() + _sanitize_filename() and use them at every call site.",
    )]


def check_upload_validation(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Upload endpoint must validate the media type (PACK-S13)."""
    out: list[Finding] = []
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("UPLOAD-MIME-CHECK", rel, suppressions):
            continue
        text = repo.text(path)
        if not text or "request.files" not in text:
            continue
        if re.search(r"(safe_extension|ALLOWED_EXTENSIONS|allowed_extensions|mimetypes|IMGHDR|magic)", text):
            continue
        out.append(_finding(
            "UPLOAD-MIME-CHECK", "Upload accepted without media-type validation", Severity.MEDIUM,
            "A04:2021", "web", "DESIGN", "CANDIDATE",
            "The upload handler does not validate an allowance list of extensions or content "
            "types, so any file type is stored and later handed to ffmpeg.",
            "request.files handled with no extension/MIME allowlist", rel, 1,
            "Validate against the shared media allowlist and reject unknown types.",
        ))
    return out


def check_proxy_ip(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Client IP must be proxy-aware for rate limiting to be meaningful (PACK-S11)."""
    rel = "utils/web_rate_limiter.py"
    text = repo.read_rel(rel)
    if not text or suppressed("IP-PROXY-HEADER", rel, suppressions):
        return []
    if re.search(r"(X-Forwarded-For|X-Real-IP|CF-Connecting-IP)", text, re.I):
        return []
    return [_finding(
        "IP-PROXY-HEADER", "Client IP resolved without proxy headers", Severity.LOW,
        "A09:2021", "web", "LOGGING", "CANDIDATE",
        "Behind Railway/a load balancer the socket peer is the proxy, so a per-IP rate "
        "limiter that ignores X-Forwarded-For sees one shared bucket (or can be evaded by "
        "spoofing the header if it is trusted blindly).",
        "no X-Forwarded-For/X-Real-IP handling", rel, 1,
        "Read the left-most untrusted hop of X-Forwarded-For with a trusted-proxy count.",
    )]


def check_request_param_parsing(repo: Repo, profile: dict, suppressions: list[dict]) -> list[Finding]:
    """Raw int()/float() on request values without validation (TIF-PARSE-01)."""
    out: list[Finding] = []
    cast_re = re.compile(r"\b(int|float)\(\s*(request|data|body|payload)[\w.]*\.(get|\[)")
    for path in repo.files(("**/*.py",)):
        rel = repo.rel(path)
        if repo.is_test(rel) or suppressed("PARSE-UNVALIDATED", rel, suppressions):
            continue
        text = repo.text(path)
        if not text:
            continue
        doc = repo.docstring_lines(path)
        lines = text.splitlines()
        for i, line in enumerate(lines, 1):
            if i in doc or not cast_re.search(line):
                continue
            window = "\n".join(lines[max(0, i - 6):i + 1])
            if "try:" in window or "except" in window:
                continue
            out.append(_finding(
                "PARSE-UNVALIDATED", "Integer cast on a request value without error handling",
                Severity.LOW, "A03:2021", "web", "DESIGN", "CANDIDATE",
                "int()/float() on an unvalidated request value raises on any non-numeric "
                "input, turning a malformed parameter into a 500 and a noisy traceback.",
                line.strip()[:160], rel, i,
                "Validate with a typed parser (or wrap in try/except) and return 400 on bad input.",
            ))
            if len(out) >= 5:
                return out
    return out


# Rule ids implemented by the semantic checks above (not by RegexRule tables).
# build_coverage() uses this so the profile can only reference rules that exist.
SEMANTIC_RULE_IDS = (
    "AC-WEBHOOK-FAILOPEN", "AC-CORS-WILDCARD", "AC-DEBUG-ENDPOINT", "AC-ADMIN-GUARD",
    "AC-ROLE-FROM-PAYLOAD", "AC-USERID-FROM-PAYLOAD", "AC-IDOR-JOB",
    "DES-RATE-LIMIT", "DES-NO-TIMEOUT", "DES-BODY-LIMIT", "DES-URL-LENGTH",
    "BOT-FLOOD-WAIT", "SSRF-VALIDATOR-WEAK", "CFG-TRUSTED-HOST", "AUTH-SESSION-FILE",
    "AUTH-QUERY-TOKEN", "CRYPTO-CMP", "SANITIZE-FILENAME", "UPLOAD-MIME-CHECK",
    "IP-PROXY-HEADER", "PARSE-UNVALIDATED",
    "INFRA-ENV-TRACKED", "INFRA-GITIGNORE", "INFRA-DOCKERIGNORE", "INFRA-DOCKER-ROOT",
    "INFRA-DOCKER-SECRET", "INFRA-COMPOSE-PORT", "INFRA-DEBUG-LAUNCH",
    "INT-CI-UNPINNED", "DEP-NO-HASHES",
)

SEMANTIC_CHECKS = (
    check_fail_open_auth,
    check_cors,
    check_routes_auth,
    check_admin_guard,
    check_rate_limit,
    check_dependencies,
    check_ssrf_validator,
    check_repo_hygiene,
    check_supply_chain_pins,
    check_outbound_timeouts,
    check_flood_wait,
    check_url_length,
    check_trusted_host,
    check_session_file_hygiene,
    check_payload_identity_fields,
    check_job_idor,
    check_body_limit,
    check_filename_sanitizer,
    check_upload_validation,
    check_proxy_ip,
    check_request_param_parsing,
)


# ─────────────────────────────────────────────────────────────────────────────
# Real CVE ingestion (pip-audit + OSV.dev)
# ─────────────────────────────────────────────────────────────────────────────

def run_pip_audit(repo: Repo) -> tuple[dict | None, str | None]:
    cmd = [sys.executable, "-m", "pip_audit", "--requirement", "requirements.txt", "--format", "json"]
    try:
        proc = subprocess.run(cmd, cwd=repo.root, capture_output=True, text=True, timeout=420,
                              encoding="utf-8", errors="replace")
    except (OSError, subprocess.SubprocessError) as exc:
        return None, f"pip-audit could not run: {exc}"
    if not proc.stdout.strip():
        return None, f"pip-audit produced no JSON (exit {proc.returncode}): {proc.stderr[-200:]}"
    try:
        return json.loads(proc.stdout), None
    except json.JSONDecodeError as exc:
        return None, f"pip-audit JSON unparsable: {exc}"


def parse_requirements_fallback(repo: Repo, profile: dict) -> dict:
    """Resolved versions without pip-audit: take the highest allowed version literal."""
    deps = []
    for rel in profile["cve"]["requirements"]:
        text = repo.read_rel(rel) or ""
        for line in text.splitlines():
            raw = line.split("#")[0].strip()
            if not raw or raw.startswith("-") or "@" in raw:
                continue
            m = re.match(r"([A-Za-z0-9_.\-]+)\s*(\[[^\]]*\])?\s*(.*)", raw)
            if not m:
                continue
            name, _, spec = m.group(1), m.group(2), m.group(3)
            versions = re.findall(r"==\s*([0-9][0-9A-Za-z.\-]*)", spec)
            if not versions:
                versions = re.findall(r"<=\s*([0-9][0-9A-Za-z.\-]*)", spec)
            if versions:
                deps.append({"name": name, "version": versions[-1], "source": rel})
    return {"dependencies": deps, "source": "requirements-parse"}


def osv_query_batch(deps: list[dict], cfg: dict) -> tuple[dict[str, list[str]], str | None]:
    """OSV.dev batch lookup → {package: [osv ids]}."""
    if not deps:
        return {}, None
    queries = [{"package": {"name": d["name"], "ecosystem": cfg["ecosystem"]},
                "version": d["version"]} for d in deps]
    body = json.dumps({"queries": queries}).encode()
    req = urllib.request.Request(  # noqa: S310 - URL comes from the scanner profile, not user input
        cfg["osv_batch_url"], data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=cfg["timeout_seconds"]) as resp:  # noqa: S310 - fixed OSV API URL
            data = json.load(resp)
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        return {}, f"OSV batch query failed: {type(exc).__name__}: {exc}"
    out: dict[str, list[str]] = {}
    for dep, result in zip(deps, data.get("results", []), strict=False):
        ids = [v.get("id") for v in (result.get("vulns") or []) if v.get("id")]
        if ids:
            out.setdefault(dep["name"], []).extend(ids)
    return out, None


def _cvss_base_score(vector: str) -> float:
    """Approximate the CVSS v3.x base score from a vector string."""
    try:
        import math
        metrics = dict(p.split(":") for p in vector.split("/") if ":" in p)
        av = {"N": 0.85, "A": 0.62, "L": 0.55, "P": 0.2}.get(metrics.get("AV", "N"), 0.85)
        ac = {"L": 0.77, "H": 0.44}.get(metrics.get("AC", "L"), 0.77)
        pr = {"N": 0.85, "L": 0.62, "H": 0.27}.get(metrics.get("PR", "N"), 0.85)
        ui = {"N": 0.85, "R": 0.62}.get(metrics.get("UI", "N"), 0.85)
        scope = metrics.get("S", "U")
        c = {"H": 0.56, "L": 0.22, "N": 0.0}.get(metrics.get("C", "N"), 0.0)
        i = {"H": 0.56, "L": 0.22, "N": 0.0}.get(metrics.get("I", "N"), 0.0)
        a = {"H": 0.56, "L": 0.22, "N": 0.0}.get(metrics.get("A", "N"), 0.0)
        iss = 1 - (1 - c) * (1 - i) * (1 - a)
        impact = 7.52 * (iss - 0.029) - 3.25 * (iss - 0.02) ** 15 if scope == "C" else 6.42 * iss
        exploit = 8.22 * av * ac * pr * ui
        if impact <= 0:
            return 0.0
        raw = min(1.08 * (impact + exploit), 10) if scope == "C" else min(impact + exploit, 10)
        return math.ceil(raw * 10) / 10
    except Exception:
        return 0.0


def osv_fetch_detail(osv_id: str, cfg: dict) -> dict | None:
    url = cfg["osv_detail_url"].rstrip("/") + "/" + osv_id
    if not osv_id.startswith(("OSV-", "GHSA-", "CVE-", "PYSEC-")):
        return None
    try:
        with urllib.request.urlopen(url, timeout=cfg["timeout_seconds"]) as resp:  # noqa: S310 - fixed OSV API URL
            return json.load(resp)
    except (urllib.error.URLError, OSError, json.JSONDecodeError):
        return None


def summarize_osv(osv_id: str, detail: dict) -> dict:
    aliases = [a for a in (detail.get("aliases") or []) if a.startswith("CVE-")]
    sev_label = ((detail.get("database_specific") or {}).get("severity") or "")
    cvss = 0.0
    for entry in detail.get("severity") or []:
        score = entry.get("score") or ""
        if score.startswith("CVSS:") or "/" in score:
            cvss = max(cvss, _cvss_base_score(score) if score.startswith("CVSS:") else 0.0)
        else:
            with suppress(ValueError):
                cvss = max(cvss, float(score))
    fixed = sorted({
        ev["fixed"]
        for aff in detail.get("affected") or []
        for rng in aff.get("ranges") or []
        for ev in rng.get("events") or []
        if ev.get("fixed")
    })
    # GitHub advisories put the label in database_specific.severity (MODERATE/HIGH/...)
    if not sev_label and cvss:
        sev_label = ("CRITICAL" if cvss >= 9 else "HIGH" if cvss >= 7
                     else "MEDIUM" if cvss >= 4 else "LOW")
    return {
        "osv_id": osv_id,
        "cve": aliases[0] if aliases else "",
        "aliases": aliases,
        "summary": (detail.get("summary") or "").strip()[:300],
        "severity": sev_label.upper() or "MEDIUM",
        "cvss": cvss,
        "fixed_versions": fixed,
        "published": detail.get("published") or "",
        "url": f"https://osv.dev/vulnerability/{osv_id}",
    }


def ingest_cves(repo: Repo, profile: dict, *, offline: bool, refresh: bool,
                errors: list[str]) -> dict:
    cfg = profile["cve"]
    cache_path = repo.root / cfg["cache_path"]

    if cache_path.is_file() and not refresh:
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            age_h = (time.time() - cached.get("generated_at_epoch", 0)) / 3600
            if age_h <= cfg["cache_max_age_hours"] or offline:
                cached["from_cache"] = True
                return cached
        except (OSError, json.JSONDecodeError):
            pass

    audit, audit_err = (None, "offline mode") if offline else run_pip_audit(repo)
    if audit_err:
        errors.append(audit_err)

    deps: list[dict] = []
    if audit:
        for d in audit.get("dependencies", []):
            deps.append({
                "name": d.get("name", ""),
                "version": d.get("version") or "",
                "source": "pip-audit",
                "pip_audit_vulns": [
                    {
                        "id": v.get("id"),
                        "fix_versions": v.get("fix_versions") or [],
                        "description": (v.get("description") or "")[:300],
                        "aliases": v.get("aliases") or [],
                    }
                    for v in (d.get("vulns") or [])
                ],
            })
    else:
        fallback = parse_requirements_fallback(repo, profile)
        deps = [{**d, "pip_audit_vulns": []} for d in fallback["dependencies"]]
        if not offline:
            errors.append("pip-audit unavailable — fell back to requirement-specifier versions")

    vulns: list[dict] = []
    if not offline and deps:
        ids_by_pkg, osv_err = osv_query_batch(deps, cfg)
        if osv_err:
            errors.append(osv_err)
        budget = cfg["max_detail_fetches"]
        seen: set[str] = set()
        for pkg, ids in ids_by_pkg.items():
            version = next((d["version"] for d in deps if d["name"] == pkg), "")
            for osv_id in ids:
                if osv_id in seen or budget <= 0:
                    continue
                seen.add(osv_id)
                budget -= 1
                detail = osv_fetch_detail(osv_id, cfg)
                if not detail:
                    vulns.append({"osv_id": osv_id, "package": pkg, "installed": version,
                                  "severity": "MEDIUM", "cvss": 0.0, "cve": "", "aliases": [],
                                  "summary": "(detail fetch failed)", "fixed_versions": [],
                                  "url": f"https://osv.dev/vulnerability/{osv_id}"})
                    continue
                entry = summarize_osv(osv_id, detail)
                entry.update({"package": pkg, "installed": version})
                vulns.append(entry)

    # pip-audit's own verdict is authoritative when it ran
    for dep in deps:
        for pv in dep.get("pip_audit_vulns") or []:
            vulns.append({
                "osv_id": pv.get("id") or "",
                "cve": next((a for a in (pv.get("aliases") or []) if a.startswith("CVE-")), ""),
                "aliases": pv.get("aliases") or [],
                "summary": pv.get("description") or "",
                "severity": "HIGH", "cvss": 0.0,
                "fixed_versions": pv.get("fix_versions") or [],
                "package": dep["name"], "installed": dep["version"],
                "url": f"https://osv.dev/vulnerability/{pv.get('id')}",
                "source": "pip-audit",
            })

    kb = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "generated_at_epoch": time.time(),
        "generator": f"scan_mediabot.py v{SCANNER_VERSION}",
        "sources": ["pip-audit", "osv.dev"],
        "ecosystem": cfg["ecosystem"],
        "from_cache": False,
        "dependency_count": len(deps),
        "vulnerability_count": len(vulns),
        "dependencies": deps,
        "vulnerabilities": sorted(vulns, key=lambda v: (-v.get("cvss", 0.0), v.get("package", ""))),
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(kb, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError as exc:
        errors.append(f"could not write CVE cache: {exc}")
    return kb


def cve_findings(kb: dict, profile: dict, suppressions: list[dict]) -> list[Finding]:
    out: list[Finding] = []
    for v in kb.get("vulnerabilities", []):
        label = v.get("cve") or v.get("osv_id") or "OSV entry"
        layer = "deps"
        if suppressed("DEP-CVE", f"requirements.txt#{v.get('package','')}", suppressions):
            continue
        out.append(Finding(
            rule="DEP-CVE", title=f"Vulnerable dependency: {v.get('package')} {v.get('installed')}",
            severity=sev(v.get("severity")), owasp="A06:2021", layer=layer,
            category="DEPENDENCIES", verdict="CONFIRMED",
            description=v.get("summary") or f"{label} affects {v.get('package')} {v.get('installed')}.",
            evidence=f"{label} · fixed in: {', '.join(v.get('fixed_versions') or []) or 'n/a'}",
            file="requirements.txt", line=1, fix="Upgrade to a fixed version and re-run the scan.",
            cve=label, package=v.get("package", ""), installed=v.get("installed", ""),
            fixed=", ".join(v.get("fixed_versions") or []), cvss=float(v.get("cvss") or 0.0),
        ))
    return out


def write_vulnclaw_kb(repo: Repo, kb: dict) -> list[str]:
    """Mirror the ingested CVEs into the VulnClaw instance knowledge base."""
    written: list[str] = []
    kb_dir = repo.root / ".vulnclaw-local" / "kb"
    cve_dir = kb_dir / "cve"
    try:
        cve_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return written
    for v in kb.get("vulnerabilities", []):
        entry_id = (v.get("cve") or v.get("osv_id") or "unknown").replace("/", "-")
        tags = ["cve", "python", v.get("package", ""), sev(v.get("severity")).value.lower()]
        entry = {
            "id": entry_id,
            "title": f"{entry_id} — {v.get('package')} {v.get('installed')}",
            "tags": [t for t in tags if t],
            "severity": v.get("severity"),
            "cvss": v.get("cvss", 0.0),
            "package": v.get("package"),
            "installed_version": v.get("installed"),
            "fixed_versions": v.get("fixed_versions") or [],
            "summary": v.get("summary"),
            "url": v.get("url"),
            "source": "scan_mediabot.py CVE ingestion",
            "target": "media_conversion_bot",
        }
        try:
            (cve_dir / f"{entry_id}.json").write_text(
                json.dumps(entry, indent=2, ensure_ascii=False), encoding="utf-8")
            written.append(str(cve_dir / f"{entry_id}.json"))
        except OSError:
            continue
    # Rebuild the index across EVERY category (mirrors KnowledgeStore._build_index):
    # hand-written technique/reference entries must survive a CVE refresh.
    try:
        index: dict[str, list[dict]] = {}
        for cat in ("cve", "techniques", "protocols", "tools", "payloads"):
            (kb_dir / cat).mkdir(parents=True, exist_ok=True)
            entries: list[dict] = []
            for f in sorted((kb_dir / cat).glob("*.json")):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                entries.append({"id": data.get("id", f.stem),
                                "title": data.get("title", f.stem),
                                "tags": data.get("tags", []),
                                "file": str(f)})
            index[cat] = entries
        (kb_dir / "index.json").write_text(json.dumps(index, indent=2, ensure_ascii=False),
                                           encoding="utf-8")
    except OSError:
        pass
    return written


# ─────────────────────────────────────────────────────────────────────────────
# Coverage matrix
# ─────────────────────────────────────────────────────────────────────────────

def build_coverage(profile: dict, findings: list[Finding]) -> list[dict]:
    defined = {
        *(r.id for r in (*SECRET_RULES, *INJECTION_RULES, *CONFIG_RULES,
                         *SSRF_RULES, *LOG_RULES, *AUTH_RULES)),
        *DEP_RULE_IDS,
        *SEMANTIC_RULE_IDS,
    }
    by_rule: dict[str, list[Finding]] = {}
    for f in findings:
        by_rule.setdefault(f.rule, []).append(f)
    rows: list[dict] = []
    for entry in profile["skill_catalog"]:
        rules = entry.get("rules", [])
        unknown = [r for r in rules if r not in defined]
        if not rules:
            status = "not-applicable"
        elif unknown:
            status = "catalog-drift"
        else:
            hits = [f for r in rules for f in by_rule.get(r, [])]
            if hits:
                worst = min((f.severity for f in hits), key=lambda s: SEV_RANK[s])
                status = f"finding:{worst.value}"
            else:
                status = "checked-clean"
        rows.append({
            "id": entry["id"], "source": entry["source"], "title": entry["title"],
            "owasp": entry["owasp"], "rules": rules, "unknown_rules": unknown,
            "status": status,
            "findings": sum(len(by_rule.get(r, [])) for r in rules),
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def sarif_doc(report: Report) -> dict:
    rules: dict[str, dict] = {}
    for f in report.findings:
        rules.setdefault(f.rule, {
            "id": f.rule,
            "name": f.title,
            "shortDescription": {"text": f.title},
            "fullDescription": {"text": f.description},
            "help": {"text": f.fix},
            "properties": {"owasp": f.owasp, "layer": f.layer, "security-severity":
                           str(f.cvss or {"CRITICAL": 9.5, "HIGH": 8.0, "MEDIUM": 5.5,
                                          "LOW": 2.5, "INFO": 0.0}[f.severity.value])},
        })
    results = [{
        "ruleId": f.rule,
        "level": {"CRITICAL": "error", "HIGH": "error", "MEDIUM": "warning",
                  "LOW": "note", "INFO": "note"}[f.severity.value],
        "message": {"text": f"{f.title} — {f.evidence or f.description}"},
        "locations": [{
            "physicalLocation": {
                "artifactLocation": {"uri": f.file or "."},
                "region": {"startLine": max(1, f.line)},
            }
        }],
        "properties": {"verdict": f.verdict, "owasp": f.owasp, "layer": f.layer},
    } for f in report.findings]
    return {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [{
            "tool": {"driver": {
                "name": "scan_mediabot",
                "version": SCANNER_VERSION,
                "informationUri": "https://osv.dev",
                "rules": list(rules.values()),
            }},
            "results": results,
        }],
    }


def render_markdown(report: Report) -> str:
    sev_counts = report.by_severity()
    verdicts = report.by_verdict()
    L: list[str] = []
    worst = report.worst()
    L.append(f"# media_conversion_bot — security audit ({report.instance})")
    L.append("")
    L.append(f"**Target:** `{report.target}`  ")
    L.append(f"**Generated:** {report.timestamp} · **Scanner:** scan_mediabot v{report.scanner_version}  ")
    L.append(f"**Files scanned:** {report.files_scanned} · **Duration:** {report.scan_seconds:.1f}s · "
             f"**Score:** {report.score}/100  ")
    L.append(f"**Worst finding:** {worst.value if worst else 'none'}  ")
    L.append(f"**Verdicts:** {verdicts['CONFIRMED']} CONFIRMED · {verdicts['CANDIDATE']} CANDIDATE")
    L.append("")
    L.append("## Severity summary")
    L.append("")
    L.append("| Severity | Count |")
    L.append("|----------|------:|")
    for s in Severity:
        L.append(f"| {s.value} | {sev_counts[s.value]} |")
    L.append("")
    L.append("## Findings by layer")
    L.append("")
    L.append("| Layer | Count |")
    L.append("|-------|------:|")
    for layer, count in sorted(report.by_layer().items()):
        L.append(f"| {layer} | {count} |")
    L.append("")

    conf = [f for f in report.findings if f.verdict == "CONFIRMED"]
    cand = [f for f in report.findings if f.verdict == "CANDIDATE"]
    for title, items in (("Confirmed findings", conf), ("Candidates (need adjudication)", cand)):
        L.append(f"## {title}")
        L.append("")
        if not items:
            L.append("_None._")
            L.append("")
            continue
        ordered = sorted(items, key=lambda f: (SEV_RANK[f.severity], f.file, f.line))
        for i, f in enumerate(ordered, 1):
            L.append(f"### {i}. [{f.severity.value}] {f.title}")
            L.append("")
            L.append(f"- **Rule:** `{f.rule}` · **OWASP:** {f.owasp} · **Layer:** {f.layer} · "
                     f"**Category:** {f.category} · **Verdict:** {f.verdict}")
            L.append(f"- **Location:** `{f.location}`")
            if f.cve:
                L.append(f"- **Advisory:** {f.cve} · package `{f.package}` {f.installed} "
                         f"→ fixed in `{f.fixed or 'n/a'}` · CVSS {f.cvss or '—'}")
            L.append("")
            L.append(f"**Why it matters:** {f.description}")
            L.append("")
            if f.evidence:
                L.append("**Evidence:**")
                L.append("```")
                L.append(f.evidence)
                L.append("```")
                L.append("")
            if f.fix:
                L.append(f"**Fix:** {f.fix}")
                L.append("")

    L.append("## Coverage vs the flaw catalog")
    L.append("")
    L.append("| Catalog entry | Source | OWASP | Status | Findings |")
    L.append("|---------------|--------|-------|--------|---------:|")
    for row in report.coverage:
        L.append(f"| {row['id']} — {row['title'][:70]} | {row['source']} | {row['owasp']} | "
                 f"{row['status']} | {row['findings']} |")
    L.append("")
    drift = [r for r in report.coverage if r["status"] == "catalog-drift"]
    if drift:
        L.append("> **Catalog drift:** these entries reference rule ids that do not exist — "
                 "fix the profile so the coverage claim stays honest:")
        for row in drift:
            L.append(f"> - `{row['id']}` → {', '.join(row['unknown_rules'])}")
        L.append("")
    gaps = [r for r in report.coverage if r["status"] == "not-applicable"]
    if gaps:
        L.append("> **Out of scope / no applicable rule:** " +
                 ", ".join(f"`{r['id']}`" for r in gaps))
        L.append("")

    kb = report.cve_kb
    L.append("## Dependency CVE ingestion")
    L.append("")
    L.append(f"- Source: {', '.join(kb.get('sources', []))} "
             f"({'cached' if kb.get('from_cache') else 'fresh'})")
    L.append(f"- Dependencies resolved: **{kb.get('dependency_count', 0)}** · "
             f"advisories matched: **{kb.get('vulnerability_count', 0)}**")
    if kb.get("vulnerabilities"):
        L.append("")
        L.append("| Advisory | Package | Installed | Severity | CVSS | Fixed in |")
        L.append("|----------|---------|-----------|----------|-----:|----------|")
        for v in kb["vulnerabilities"][:40]:
            L.append(f"| {v.get('cve') or v.get('osv_id')} | {v.get('package')} | "
                     f"{v.get('installed')} | {v.get('severity')} | {v.get('cvss') or '—'} | "
                     f"{', '.join(v.get('fixed_versions') or []) or '—'} |")
    else:
        L.append("")
        L.append("_No known-vulnerable dependency was found for the resolved version set._")
    L.append("")

    if report.suppressions:
        L.append("## Accepted deviations / scoped-out areas")
        L.append("")
        for s in report.suppressions:
            L.append(f"- `{s.get('rule')}` on `{s.get('glob')}` — {s.get('reason')}")
        L.append("")
    if report.tool_errors:
        L.append("## Tool notes")
        L.append("")
        for e in report.tool_errors:
            L.append(f"- {e}")
        L.append("")
    L.append("---")
    L.append("")
    L.append("Generated by `security/scan_mediabot.py` (instance "
             "`security/mediabot-profile.json`). Findings marked **CANDIDATE** are heuristics: "
             "they must be adjudicated by hand before being treated as real. Findings marked "
             "**CONFIRMED** carry a file:line excerpt from the scanned revision.")
    L.append("")
    return "\n".join(L)


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    with suppress(Exception):
        sys.stdout.reconfigure(errors="replace")

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--target", default=".", help="repo root (default: current directory)")
    ap.add_argument("--profile", default=None, help=f"profile JSON (default: {PROFILE_FILENAME})")
    ap.add_argument("--offline", action="store_true", help="no network: use the cached CVE KB")
    ap.add_argument("--refresh-cve", action="store_true", help="force a fresh pip-audit + OSV pull")
    ap.add_argument("--no-cve", action="store_true", help="skip dependency CVE ingestion")
    ap.add_argument("--include-tests", action="store_true", help="report test-file hits too")
    ap.add_argument("--no-write", action="store_true", help="print only, write nothing")
    ap.add_argument("--quiet", action="store_true", help="suppress the console summary")
    args = ap.parse_args()

    root = Path(args.target).expanduser().resolve()
    if not root.is_dir():
        print(f"[scan_mediabot] ERROR: not a directory: {root}", file=sys.stderr)
        return 1

    profile_path = Path(args.profile) if args.profile else Path(__file__).with_name(PROFILE_FILENAME)
    if not profile_path.is_file():
        print(f"[scan_mediabot] ERROR: profile not found: {profile_path}", file=sys.stderr)
        return 1
    profile = json.loads(profile_path.read_text(encoding="utf-8"))

    def log(msg: str) -> None:
        if not args.quiet:
            print(f"[scan_mediabot] {msg}", flush=True)

    started = time.time()
    repo = Repo(root, profile)
    errors: list[str] = []
    suppressions = profile.get("suppressions", [])
    overrides = {k: sev(v) for k, v in profile.get("severity_overrides", {}).items()}

    rules: list[RegexRule] = [*SECRET_RULES, *INJECTION_RULES, *CONFIG_RULES,
                              *SSRF_RULES, *LOG_RULES, *AUTH_RULES]
    findings: list[Finding] = []
    log(f"instance={profile['instance']['name']} root={root}")
    for rule in rules:
        hits = run_regex_rule(rule, repo, args.include_tests, suppressions, errors,
                              overrides.get(rule.id))
        findings.extend(hits)
        log(f"  rule {rule.id:<22} {len(hits):>3} finding(s)")

    # semantic / structural checks
    for check in SEMANTIC_CHECKS:
        hits = check(repo, profile, suppressions)
        findings.extend(hits)
        log(f"  check {check.__name__:<24} {len(hits):>3} finding(s)")

    # CVE ingestion
    kb: dict = {"sources": ["pip-audit", "osv.dev"], "dependencies": [], "vulnerabilities": [],
                "dependency_count": 0, "vulnerability_count": 0}
    if profile["cve"]["enabled"] and not args.no_cve:
        log("ingesting real CVEs (pip-audit + osv.dev)…")
        kb = ingest_cves(repo, profile, offline=args.offline, refresh=args.refresh_cve,
                         errors=errors)
        findings.extend(cve_findings(kb, profile, suppressions))
        log(f"  resolved {kb.get('dependency_count', 0)} dependencies, "
            f"{kb.get('vulnerability_count', 0)} advisories matched")
        if not args.no_write:
            written = write_vulnclaw_kb(repo, kb)
            if written:
                log(f"  mirrored {len(written)} CVE entries into .vulnclaw-local/kb/cve/")

    report = Report(
        instance=profile["instance"]["name"],
        target=str(root),
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
        findings=sorted(findings, key=lambda f: (SEV_RANK[f.severity], f.file, f.line)),
        cve_kb=kb,
        coverage=build_coverage(profile, findings),
        suppressions=suppressions,
        files_scanned=sum(1 for p in repo.files(("**/*.py", "**/*.yml", "**/*.yaml",
                                                 "**/*.json", "**/*.txt", "**/*.sh"))
                          if repo.text(p) is not None),
        scan_seconds=time.time() - started,
        tool_errors=errors,
    )

    outputs = profile["outputs"]
    if not args.no_write:
        reports_dir = root / "security" / "reports"
        try:
            reports_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                **{k: v for k, v in asdict(report).items() if k != "findings"},
                "score": report.score,
                "by_severity": report.by_severity(),
                "by_layer": report.by_layer(),
                "by_verdict": report.by_verdict(),
                "findings": [{**asdict(f), "severity": f.severity.value} for f in report.findings],
            }
            (root / outputs["json"]).write_text(
                json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            (root / outputs["markdown"]).write_text(render_markdown(report), encoding="utf-8")
            (root / outputs["sarif"]).write_text(
                json.dumps(sarif_doc(report), indent=2, ensure_ascii=False), encoding="utf-8")
            log(f"reports written: {outputs['json']}, {outputs['markdown']}, {outputs['sarif']}")
        except OSError as exc:
            print(f"[scan_mediabot] ERROR: could not write reports: {exc}", file=sys.stderr)
            return 1

    if not args.quiet:
        print()
        print(render_markdown(report).split("## Findings by layer")[0])
        print(f"score={report.score}/100  findings={len(report.findings)}  "
              f"confirmed={report.by_verdict()['CONFIRMED']}  "
              f"candidates={report.by_verdict()['CANDIDATE']}")

    fail_on = {sev(s) for s in profile["verdict_policy"]["fail_on"]}
    confirmed_worst = min((f.severity for f in report.findings
                           if f.verdict == "CONFIRMED" and f.severity in fail_on),
                          key=lambda s: SEV_RANK[s], default=None)
    if confirmed_worst is not None:
        return 2
    if report.by_severity()["MEDIUM"] or report.by_verdict()["CANDIDATE"]:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
