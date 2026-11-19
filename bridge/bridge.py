# -*- coding: utf-8 -*-
"""Мост синка: mantledb + дельты устройств (textdb.dev) → mantledb + snapshot.json.

Запуск в GitHub Actions:  python bridge/bridge.py
Запуск локально (без коммита):  python bridge/bridge.py --no-git

Что делает:
  1. Читает облачные документы mantledb (progress + answers).
  2. Читает реестр устройств и дельты каждого устройства из textdb.dev
     (ключи mlq58f716457a-out-<device>, куски правок mlq58f716457a-outa-<device>-<i>).
  3. Мержит дельты в облачные документы (флаги/правки — по времени t,
     просмотры — максимум по устройству, дни — объединение).
  4. Кладёт объединённые документы обратно в mantledb и пишет ack в дельту устройства.
  5. Пишет site/snapshot.json — сайт отдаёт его как свежий снимок с mismo origin.
"""
import json
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent  # site/
NS = "mlq58f716457a"
MANTLE_BASE = "https://mantledb.sh/v2/mlq-sync-58f716457a"
MANTLE_KEY = "1162a83b616bbf943548722ffb965e6b2b9d33c783b3f9e7e2138f2487cba245"
TEXTDB = "https://textdb.dev/api/data/"
UA = "Mozilla/5.0 (compatible; mllm-sync-bridge/1.0)"
STALE_DEVICE_MS = 45 * 24 * 3600 * 1000


def _curl_http(url, method="GET", headers=None, data=None, timeout=40):
    """curl.exe надёжнее urllib до mantledb с некоторых сетей (Windows)."""
    with tempfile.NamedTemporaryFile(suffix=".out", delete=False) as tf:
        out = tf.name
    args = ["curl.exe", "-s", "--compressed", "-m", str(timeout), "-X", method, url,
            "-H", f"User-Agent: {UA}", "-o", out, "-w", "%{http_code}"]
    for k, v in (headers or {}).items():
        args += ["-H", f"{k}: {v}"]
    if data is not None:
        with tempfile.NamedTemporaryFile(suffix=".body", delete=False) as bf:
            bf.write(data)
            args += ["--data-binary", "@" + bf.name]
    try:
        r = subprocess.run(args, capture_output=True, timeout=timeout + 20)
        code = int(r.stdout.decode("ascii", "replace").strip() or 0)
        body = Path(out).read_text(encoding="utf-8", errors="replace")
        return code, body
    finally:
        Path(out).unlink(missing_ok=True)


def http(url, method="GET", headers=None, data=None, timeout=40):
    import os
    if os.name == "nt":
        return _curl_http(url, method=method, headers=headers, data=data, timeout=timeout)
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h, data=data, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def get_json(url, key=None, attempts=3):
    headers = {"X-Mantle-Key": key} if key else None
    last = None
    for i in range(attempts):
        try:
            st, body = http(url, headers=headers)
            if st == 200:
                return json.loads(body)
            if st == 404:
                return {}
            last = f"HTTP {st}"
        except Exception as e:
            last = str(e)
        time.sleep(2)
    print(f"  ! GET не удался ({attempts} попытки): {last}")
    return None


def post_json(url, doc, key=None, attempts=3):
    body = json.dumps(doc, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-Mantle-Key"] = key
    last = None
    for i in range(attempts):
        try:
            st, _ = http(url, method="POST", headers=headers, data=body)
            if st in (200, 201):
                return st
            last = f"HTTP {st}"
        except Exception as e:
            last = str(e)
        time.sleep(2)
    print(f"  ! POST не удался ({attempts} попытки): {last}")
    return 0


def textdb_get(key, attempts=2):
    for i in range(attempts):
        try:
            st, body = http(TEXTDB + key)
            if st == 200:
                return body
            if st == 404:
                return None
        except Exception as e:
            print(f"  ! textdb GET {key}: {e}")
        time.sleep(1)
    return None


def textdb_post(key, text, attempts=2):
    for i in range(attempts):
        try:
            st, _ = http(TEXTDB + key, method="POST",
                         headers={"Content-Type": "text/plain"},
                         data=text.encode("utf-8"))
            if st == 200:
                return True
        except Exception as e:
            print(f"  ! textdb POST {key}: {e}")
        time.sleep(1)
    return False


def merge_flags(dst, src):
    """dst, src: {id: {v,t} | число}. Победа по t; ничья — v=1, если хоть где-то."""
    changed = False
    src = src or {}
    ids = set(dst.keys()) | set(src.keys())
    for i in ids:
        s = src.get(i)
        if isinstance(s, (int, float)):
            s = {"v": 1 if s else 0, "t": 0}
        d = dst.get(i)
        if isinstance(d, (int, float)):
            d = {"v": 1 if d else 0, "t": 0}
        dv = int((d or {}).get("v") or 0)
        dt = int((d or {}).get("t") or 0)
        sv = int((s or {}).get("v") or 0)
        st = int((s or {}).get("t") or 0)
        if st > dt:
            nv, nt = sv, st
        elif dt > st:
            nv, nt = dv, dt
        else:
            nv, nt = (1 if (dv or sv) else 0), max(dt, st)
        if nv != dv or nt != dt or (i not in dst):
            dst[i] = {"v": nv, "t": nt}
            changed = True
    return changed


def merge_answers(dst, src):
    """dst, src: {id: {short?, long?, q?, del?, t}}. Победа по t."""
    changed = False
    for i, rec in (src or {}).items():
        if not isinstance(rec, dict):
            continue
        cur = dst.get(i)
        if not isinstance(cur, dict) or int(rec.get("t") or 0) > int(cur.get("t") or 0):
            dst[i] = rec
            changed = True
    return changed


def merge_views(dst_dev, src_map):
    """Просмотры устройства: поэлементный максимум (документы — копии одного состояния)."""
    changed = False
    for i, n in (src_map or {}).items():
        n = int(n or 0)
        if n <= 0:
            continue
        if n > int(dst_dev.get(i) or 0):
            dst_dev[i] = n
            changed = True
    return changed


def merge_daily(dst, src):
    changed = False
    for day, m in (src or {}).items():
        if not isinstance(m, dict):
            continue
        dday = dst.setdefault(day, {})
        for i, v in m.items():
            if v and dday.get(i) != 1:
                dday[i] = 1
                changed = True
    return changed


def main(no_git: bool) -> int:
    global NS
    for i, arg in enumerate(sys.argv):
        if arg == "--ns" and i + 1 < len(sys.argv):
            NS = sys.argv[i + 1]
    now = int(time.time() * 1000)
    print(f"[мост] запуск {now} ns={NS}")

    pdoc = get_json(f"{MANTLE_BASE}/progress", key=MANTLE_KEY)
    adoc = get_json(f"{MANTLE_BASE}/answers", key=MANTLE_KEY)
    if pdoc is None or adoc is None:
        print("[мост] mantledb недоступен — выходим")
        return 1
    pdoc.setdefault("views", {})
    pdoc.setdefault("favs", {})
    pdoc.setdefault("done", {})
    pdoc.setdefault("hidden", {})
    pdoc.setdefault("daily", {})
    adoc.setdefault("answers", {})

    reg_raw = textdb_get(f"{NS}-devices")
    devices = {}
    if reg_raw:
        try:
            devices = json.loads(reg_raw)
            if not isinstance(devices, dict):
                devices = {}
        except Exception:
            devices = {}
    devices = {k: v for k, v in devices.items()
               if isinstance(v, (int, float)) and now - int(v) < STALE_DEVICE_MS}
    print(f"[мост] устройств в реестре: {len(devices)}")

    for dev, last in sorted(devices.items()):
        if not dev or not str(dev).replace('-', '').replace('_', '').isalnum():
            continue
        ob_raw = textdb_get(f"{NS}-out-{dev}")
        if not ob_raw:
            continue
        try:
            ob = json.loads(ob_raw)
        except Exception:
            print(f"[мост] {dev}: кривая дельта, пропускаю")
            continue
        if not isinstance(ob, dict):
            continue
        for kind in ("favs", "done", "hidden"):
            if merge_flags(pdoc[kind], ob.get(kind)):
                print(f"[мост] {dev}: флаги {kind}")
        if isinstance(ob.get("views"), dict):
            merge_views(pdoc["views"].setdefault(dev, {}), ob["views"])
        if isinstance(ob.get("daily"), dict):
            merge_daily(pdoc["daily"], ob["daily"])
        try:
            nparts = int(ob.get("aParts") or 0)
        except Exception:
            nparts = 0
        for i in range(min(nparts, 40)):
            part_raw = textdb_get(f"{NS}-outa-{dev}-{i}")
            if not part_raw:
                continue
            try:
                part = json.loads(part_raw)
            except Exception:
                continue
            if not isinstance(part, list):
                continue
            recs = {}
            for pair in part:
                if isinstance(pair, list) and len(pair) == 2 and isinstance(pair[1], dict):
                    recs[str(pair[0])] = pair[1]
            if recs and merge_answers(adoc["answers"], recs):
                print(f"[мост] {dev}: правки ответов, кусок {i}")
        # ack не пишем: подтверждение после переноса в mantledb даёт браузер-«мост» (десктоп)

    saved_at = max(int(pdoc.get("savedAt") or 0), int(adoc.get("savedAt") or 0)) or now
    snap = {"schema": 3, "savedAt": saved_at, "p": pdoc, "a": adoc}
    snap_path = BASE_DIR / "snapshot.json"
    snap_path.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    print(f"[мост] snapshot.json: {snap_path.stat().st_size} Б, savedAt {saved_at}")

    if no_git:
        print("[мост] --no-git: коммит не выполняется")
        return 0

    import subprocess
    def git(*args):
        return subprocess.run(["git", "-C", str(BASE_DIR), *args],
                              capture_output=True, text=True)
    git("add", "snapshot.json")
    diff = git("diff", "--cached", "--quiet")
    if diff.returncode == 0:
        print("[мост] snapshot без изменений — коммита нет")
        return 0
    git("config", "user.name", "sync-bridge")
    git("config", "user.email", "actions@users.noreply.github.com")
    c = git("commit", "-m", "snapshot refresh")
    if c.returncode != 0:
        print(f"[мост] commit failed: {c.stderr.strip()[:200]}")
        return 1
    p = git("push", "origin", "main")
    if p.returncode != 0:
        print(f"[мост] push failed: {p.stderr.strip()[:200]}")
        return 1
    print("[мост] snapshot закоммичен и отправлен")
    return 0


if __name__ == "__main__":
    sys.exit(main("--no-git" in sys.argv))
