"""Redact secrets from text on stdin. Values come from airflow/.env and are never printed."""
import json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ENV = os.path.join(HERE, "..", "airflow", ".env")

secrets = set()
try:
    for line in open(ENV):
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        _, val = line.split("=", 1)
        val = val.strip().strip("'\"")
        try:
            obj = json.loads(val)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            for k in ("login", "password", "host"):
                if obj.get(k):
                    secrets.add(str(obj[k]))
            for k, v in (obj.get("extra") or {}).items():
                if k in ("account", "private_key_file", "private_key_content", "password", "token") and v:
                    secrets.add(str(v))
        elif len(val) > 12 and not val.isdigit():
            secrets.add(val)
except FileNotFoundError:
    pass

text = sys.stdin.read()
text = re.sub(r"\x1b\[[0-9;]*m", "", text)
text = "\n".join(l for l in text.split("\n") if "[warning  ]" not in l)
for s in sorted(secrets, key=len, reverse=True):
    text = re.sub(re.escape(s), "[REDACTED]", text, flags=re.I)
text = re.sub(r"-----BEGIN[^-]*-----.*?-----END[^-]*-----", "[REDACTED KEY]", text, flags=re.S)
text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[REDACTED EMAIL]", text)
text = re.sub(r"snowflake://\S+", "[REDACTED URI]", text)
text = re.sub(r"(/[\w./-]*rsa_key[\w.]*)", "[REDACTED KEY PATH]", text)
sys.stdout.write(text)
