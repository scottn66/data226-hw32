"""Check files for Snowflake secrets before they go into git. Never prints a secret value.

Usage:
  python3 tools/check_secrets.py            # files git would commit (tracked + staged + untracked, not ignored)
  python3 tools/check_secrets.py FILE ...   # just these files

Secret values come from airflow/.env, airflow/include/profiles.yml and airflow/include/rsa_key.p8
(all gitignored): account, user, passwords and key text. Key file paths are not treated as secrets. Each hit prints as path:line and the kind of secret. Exit code 1 if anything is found.
Images, PDFs and zips can't be scanned this way and are listed separately, so look at them yourself.
"""
import json, os, re, subprocess, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENV = os.path.join(ROOT, "airflow", ".env")
PROFILES = os.path.join(ROOT, "airflow", "include", "profiles.yml")
KEY = os.path.join(ROOT, "airflow", "include", "rsa_key.p8")
SECRET_FILES = {ENV, PROFILES, KEY}
BINARY = (".png", ".jpg", ".jpeg", ".gif", ".pdf", ".zip", ".gz", ".parquet", ".pyc")

secrets = {}  # value -> kind


def add(val, kind):
    val = str(val).strip().strip("'\"")
    if len(val) >= 6 and not val.isdigit():
        secrets[val] = kind


try:
    for line in open(ENV):
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        name, val = line.split("=", 1)
        val = val.strip().strip("'\"")
        add(val, f".env {name.strip()}")
        try:
            obj = json.loads(val)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            for k in ("login", "password", "host"):
                if obj.get(k):
                    add(obj[k], f".env conn {k}")
            for k, v in (obj.get("extra") or {}).items():
                if k in ("account", "user", "private_key_content", "password", "token",
                         "private_key_passphrase") and v:
                    add(v, f".env conn extra {k}")
except FileNotFoundError:
    pass

try:
    for line in open(PROFILES):
        m = re.match(r"\s*(account|user|password|private_key_passphrase|token)\s*:\s*(\S.*)$", line)
        if m and "env_var" not in m.group(2):
            add(m.group(2), f"profiles.yml {m.group(1)}")
except FileNotFoundError:
    pass

try:
    body = [l.strip() for l in open(KEY) if l.strip() and not l.startswith("-----")]
    for l in body[:3] + body[-3:]:
        if len(l) >= 40:
            secrets[l] = "rsa_key.p8 key body"
except FileNotFoundError:
    pass

PATTERNS = [
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "private key block"),
    (re.compile(r"snowflake://[^\s'\"]*:[^\s'\"]*@"), "snowflake URI with credentials"),
    (re.compile(r"^\s*AIRFLOW_CONN_[A-Z_]+\s*=\s*\S"), "AIRFLOW_CONN_ assignment"),
    (re.compile(r"[A-Za-z0-9._%+-]+@(?!anthropic\.com)[A-Za-z0-9.-]+\.(com|edu|org|net|io)\b"), "email address"),
]


def candidate_files():
    out = subprocess.run(["git", "-C", ROOT, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                         capture_output=True, text=True, check=True).stdout
    return [os.path.join(ROOT, p) for p in out.split("\0") if p]


files = [os.path.abspath(f) for f in sys.argv[1:]] or candidate_files()
hits, unscanned = [], []
for path in files:
    rel = os.path.relpath(path, ROOT)
    if path in SECRET_FILES:
        hits.append(f"{rel}: this is a secret file itself; it must stay gitignored")
        continue
    if rel.lower().endswith(BINARY):
        unscanned.append(rel)
        continue
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except (IsADirectoryError, FileNotFoundError):
        continue
    for n, line in enumerate(text.split("\n"), 1):
        low = line.lower()
        for val, kind in secrets.items():
            if val.lower() in low:
                hits.append(f"{rel}:{n}: {kind}")
        for rx, kind in PATTERNS:
            if kind == "AIRFLOW_CONN_ assignment" and rel.endswith(".example"):
                continue  # templates are allowed to show the variable, with placeholder values
            if rx.search(line):
                hits.append(f"{rel}:{n}: {kind}")

print(f"checked {len(files) - len(unscanned)} text files against {len(secrets)} known secret values")
if unscanned:
    print(f"not scanned (binary, check by eye): {len(unscanned)} files")
    for u in unscanned:
        print(f"  {u}")
if hits:
    print(f"FOUND {len(hits)}:")
    for h in hits:
        print(f"  {h}")
    sys.exit(1)
print("no secrets found")
