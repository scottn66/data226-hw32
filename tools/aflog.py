"""Read-only Airflow 3 REST reader (local Astro, http://localhost:8080). Output is redacted.

  aflog.py states <dag> <run_id>                       task ids, states, tries, operators
  aflog.py log <dag> <run_id> <task> <try> [regex]     task log lines (filtered by regex if given)
  aflog.py runs <dag>                                  recent runs with Pacific start/end times
"""
import json, os, re, subprocess, sys, urllib.parse, urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = "http://localhost:8080/api/v2"
PT = ZoneInfo("America/Los_Angeles")
REDACT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redact.py")


def get(path):
    with urllib.request.urlopen(BASE + path) as r:
        return json.load(r)


def q(s):
    return urllib.parse.quote(s, safe="")


def pt(ts):
    if not ts:
        return "-"
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(PT).strftime("%Y-%m-%d %H:%M:%S %Z")


def main():
    cmd, dag = sys.argv[1], sys.argv[2]
    out = []
    if cmd == "runs":
        for r in get(f"/dags/{dag}/dagRuns?order_by=-start_date&limit=20")["dag_runs"]:
            out.append(f"{r['dag_run_id']}  {r['run_type']:<10} {r['state']:<8} start {pt(r.get('start_date'))}  end {pt(r.get('end_date'))}")
    elif cmd == "states":
        run = sys.argv[3]
        r = get(f"/dags/{dag}/dagRuns/{q(run)}")
        out.append(f"dag={dag} run={run} state={r['state']} start={pt(r.get('start_date'))} end={pt(r.get('end_date'))}")
        tis = get(f"/dags/{dag}/dagRuns/{q(run)}/taskInstances?limit=100")["task_instances"]
        for t in sorted(tis, key=lambda t: (t.get("start_date") or "9", t["task_id"])):
            out.append(f"  {t['task_id']:<48} {str(t['state']):<10} try={t['try_number']} {t.get('operator') or ''}")
    elif cmd == "log":
        run, task, tr = sys.argv[3], sys.argv[4], sys.argv[5]
        pat = re.compile(sys.argv[6]) if len(sys.argv) > 6 else None
        d = get(f"/dags/{dag}/dagRuns/{q(run)}/taskInstances/{q(task)}/logs/{tr}?full_content=true")
        out.append(f"--- log dag={dag} run={run} task={task} try={tr}" + (f" filter=/{pat.pattern}/" if pat else ""))
        for e in d.get("content", []):
            ev = str(e.get("event", ""))
            if ev.startswith("::group::") or ev.startswith("::endgroup::"):
                continue
            line = f"{e.get('timestamp', '')} {e.get('level', '')} {ev}".strip()
            for x in e.get("error_detail") or []:
                if isinstance(x, dict):
                    line += f" | {x.get('exc_type', '')}: {x.get('exc_value', '')}"
            if pat is None or pat.search(line):
                out.append(line)
    text = "\n".join(out) + "\n"
    sys.stdout.write(subprocess.run([sys.executable, REDACT], input=text, capture_output=True, text=True).stdout)


if __name__ == "__main__":
    main()
