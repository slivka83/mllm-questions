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
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent.parent  # site/
NS = "mlq58f716457a"
MANTLE_BASE = "https://mantledb.sh/v2/mlq-sync-58f716457a"
MANTLE_KEY = "1162a83b616bbf943548722ffb965e6b2b9d33c783b3f9e7e2138f2487cba245"
TEXTDB = "https://textdb.dev/api/data/"
UA = "Mozilla/5.0 (compatible; mllm-sync-bridge/1.0)"
STALE_DEVICE_MS = 45 * 24 * 3600 * 1000
# Второй канал записи: retained-сообщения публичных MQTT-брокеров.
# (host, port, tls) — на Actions обычно доступен TCP 1883, WSS оставлен запасным.
MQTT_BROKERS = [
    ("broker.hivemq.com", 1883, False),
    ("broker.emqx.io", 1883, False),
    ("broker.hivemq.com", 8884, True),
    ("broker.emqx.io", 8084, True),
]
MQTT_WAIT = 5.0


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


def norm_p(doc):
    if not isinstance(doc, dict):
        doc = {}
    for k in ("views", "favs", "done", "hidden", "daily"):
        if not isinstance(doc.get(k), dict):
            doc[k] = {}
    return doc


def norm_a(doc):
    if not isinstance(doc, dict):
        doc = {}
    if not isinstance(doc.get("answers"), dict):
        doc["answers"] = {}
    return doc


def merge_cloud_into(base_p, base_a, pdoc, adoc):
    """Текущее облако → накопительная база снимка (победа по t)."""
    changed = False
    for kind in ("favs", "done", "hidden"):
        if merge_flags(base_p[kind], pdoc.get(kind)):
            changed = True
    for dev, m in (pdoc.get("views") or {}).items():
        if isinstance(m, dict) and merge_views(base_p["views"].setdefault(dev, {}), m):
            changed = True
    if merge_daily(base_p["daily"], pdoc.get("daily")):
        changed = True
    if merge_answers(base_a["answers"], adoc.get("answers")):
        changed = True
    for k in ("schema", "savedAt"):
        if pdoc.get(k) and (k not in base_p or int(pdoc.get(k) or 0) > int(base_p.get(k) or 0)):
            base_p[k] = pdoc[k]
    for k in ("schema", "savedAt"):
        if adoc.get(k) and (k not in base_a or int(adoc.get(k) or 0) > int(base_a.get(k) or 0)):
            base_a[k] = adoc[k]
    return changed


def apply_outbox(pdoc, adoc, dev, ob):
    """Дельта устройства (textdb-документ) → документы снимка."""
    if not isinstance(ob, dict):
        return False
    changed = False
    for kind in ("favs", "done", "hidden"):
        if merge_flags(pdoc[kind], ob.get(kind)):
            changed = True
    if isinstance(ob.get("views"), dict):
        if merge_views(pdoc["views"].setdefault(dev, {}), ob["views"]):
            changed = True
    if isinstance(ob.get("daily"), dict):
        if merge_daily(pdoc["daily"], ob["daily"]):
            changed = True
    pairs = []
    if isinstance(ob.get("a"), list):
        pairs.extend(ob["a"])
    if isinstance(ob.get("_answerPairs"), list):
        pairs.extend(ob["_answerPairs"])
    recs = {}
    for pair in pairs:
        if isinstance(pair, list) and len(pair) == 2 and isinstance(pair[1], dict):
            recs[str(pair[0])] = pair[1]
    if recs and merge_answers(adoc["answers"], recs):
        changed = True
    return changed


def read_textdb_device(dev):
    raw = textdb_get(f"{NS}-out-{dev}")
    if not raw:
        return dev, None
    try:
        ob = json.loads(raw)
    except Exception:
        print(f"  ! textdb {dev}: кривая дельта")
        return dev, None
    if not isinstance(ob, dict):
        return dev, None
    try:
        nparts = int(ob.get("aParts") or 0)
    except Exception:
        nparts = 0
    pairs = []
    for i in range(min(nparts, 40)):
        part_raw = textdb_get(f"{NS}-outa-{dev}-{i}")
        if not part_raw:
            continue
        try:
            part = json.loads(part_raw)
        except Exception:
            continue
        if isinstance(part, list):
            pairs.extend(part)
    if pairs:
        ob["_answerPairs"] = pairs
    return dev, ob


def snapshot_extra(base_p, base_a, pdoc, adoc):
    """Есть ли в накопительном снимке записи новее облачных (дельты, ещё не в mantledb)."""
    for kind in ("favs", "done", "hidden"):
        cloud = pdoc.get(kind) or {}
        for i, e in (base_p.get(kind) or {}).items():
            if not isinstance(e, dict):
                continue
            ce = cloud.get(i)
            ct = int(ce.get("t") or 0) if isinstance(ce, dict) else 0
            cv = int(ce.get("v") or 0) if isinstance(ce, dict) else 0
            bt = int(e.get("t") or 0)
            bv = int(e.get("v") or 0)
            if bt > ct or (bt == ct and bv != cv):
                return True
    cloud_a = adoc.get("answers") or {}
    for i, r in (base_a.get("answers") or {}).items():
        if not isinstance(r, dict):
            continue
        cr = cloud_a.get(i)
        if not isinstance(cr, dict) or int(r.get("t") or 0) > int(cr.get("t") or 0):
            return True
    cloud_v = pdoc.get("views") or {}
    for dev, m in (base_p.get("views") or {}).items():
        cm = cloud_v.get(dev) or {}
        for i, n in (m or {}).items():
            if int(n or 0) > int(cm.get(i) or 0):
                return True
    cloud_d = pdoc.get("daily") or {}
    for day, m in (base_p.get("daily") or {}).items():
        cm = cloud_d.get(day) or {}
        for i, v in (m or {}).items():
            if v and not cm.get(i):
                return True
    return False


def collect_mqtt():
    """Retained-сообщения со всех публичных брокеров: куски дельт устройств."""
    try:
        import paho.mqtt.client as mqtt
    except Exception as e:
        print(f"  ! paho-mqtt нет: {str(e)[:80]}")
        return []
    try:
        from paho.mqtt.enums import CallbackAPIVersion
        def mk():
            return mqtt.Client(CallbackAPIVersion.VERSION2,
                               client_id="mlqbridge" + str(int(time.time() * 1000))[-9:])
    except Exception:
        def mk():
            return mqtt.Client(client_id="mlqbridge" + str(int(time.time() * 1000))[-9:])
    out = []
    topic = NS + "/out/#"
    for host, port, tls in MQTT_BROKERS:
        got = []
        def on_message(cl, userdata, msg, _got=got):
            _got.append((msg.topic, bytes(msg.payload)))
        c = None
        try:
            c = mk()
            if tls:
                c.tls_set()
            c.on_message = on_message
            c.connect(host, port, 8)
            c.subscribe(topic, qos=1)
            c.loop_start()
            time.sleep(MQTT_WAIT)
        except Exception as e:
            print(f"  ! MQTT {host}:{port}: {str(e)[:90]}")
        finally:
            try:
                if c is not None:
                    c.loop_stop()
                    c.disconnect()
            except Exception:
                pass
        if got:
            print(f"  MQTT {host}:{port}: {len(got)} сообщ.")
        out.extend(got)
    return out


def dev_from_topic(topic):
    prefix = NS + "/out/"
    if not topic.startswith(prefix):
        return ""
    rest = topic[len(prefix):]
    return rest.split("/")[0]


def main(no_git: bool) -> int:
    global NS
    out_path = None
    reset = False
    for i, arg in enumerate(sys.argv):
        if arg == "--ns" and i + 1 < len(sys.argv):
            NS = sys.argv[i + 1]
        if arg == "--out" and i + 1 < len(sys.argv):
            out_path = Path(sys.argv[i + 1])
        if arg == "--reset":
            reset = True
    now = int(time.time() * 1000)
    print(f"[мост] запуск {now} ns={NS}")

    snap_path = out_path or (BASE_DIR / "snapshot.json")
    prev = None
    if not reset:
        try:
            loaded = json.loads(snap_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict) and isinstance(loaded.get("p"), dict):
                prev = loaded
                print(f"[мост] накопительная база: savedAt {loaded.get('savedAt')}")
        except Exception:
            prev = None

    if prev:
        base_p = json.loads(json.dumps(prev.get("p") or {}))
        base_a = json.loads(json.dumps(prev.get("a") or {}))
    else:
        base_p = {}
        base_a = {}
    base_p = norm_p(base_p)
    base_a = norm_a(base_a)

    changed = False
    pdoc = get_json(f"{MANTLE_BASE}/progress", key=MANTLE_KEY)
    adoc = get_json(f"{MANTLE_BASE}/answers", key=MANTLE_KEY)
    if pdoc is None and adoc is None and not prev:
        print("[мост] mantledb недоступен и снимка нет — выходим")
        return 1
    if pdoc is None:
        print("[мост] mantledb progress недоступен — продолжаю по снимку")
        pdoc = {}
    if adoc is None:
        print("[мост] mantledb answers недоступен — продолжаю по снимку")
        adoc = {}
    pdoc = norm_p(pdoc)
    adoc = norm_a(adoc)
    if merge_cloud_into(base_p, base_a, pdoc, adoc):
        changed = True
        print("[мост] облако → база: есть новые записи")

    # textdb: реестр + все известные устройства (реестр, просмотры облака/снимка)
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
    candidates = set(devices.keys())
    candidates |= set((base_p.get("views") or {}).keys())
    candidates |= set((pdoc.get("views") or {}).keys())
    candidates = {d for d in candidates
                  if d and str(d).replace("-", "").replace("_", "").isalnum()}
    print(f"[мост] textdb: реестр {len(devices)}, кандидатов {len(candidates)}")
    if candidates:
        with ThreadPoolExecutor(max_workers=6) as ex:
            results = list(ex.map(read_textdb_device, sorted(candidates)))
        for dev, ob in results:
            if ob and apply_outbox(base_p, base_a, dev, ob):
                changed = True
                print(f"[мост] textdb {dev}: дельта слита")

    # MQTT: retained-куски всех устройств; реестр не нужен (wildcard)
    for topic, payload in collect_mqtt():
        try:
            piece = json.loads(payload.decode("utf-8", "replace"))
        except Exception:
            continue
        if not isinstance(piece, dict):
            continue
        dev = str(piece.get("dev") or dev_from_topic(topic) or "")
        if not dev:
            continue
        if apply_outbox(base_p, base_a, dev, piece):
            changed = True
            print(f"[мост] mqtt {dev}/{topic.rsplit('/', 1)[-1]}: дельта слита")

    prev_saved = int(prev.get("savedAt") or 0) if prev else 0
    cloud_saved = max(int(pdoc.get("savedAt") or 0), int(adoc.get("savedAt") or 0))
    extra = snapshot_extra(base_p, base_a, pdoc, adoc)
    if extra:
        # в снимке есть дельты, которых ещё нет в облаке: он обязан быть новее
        saved_at = max(now, prev_saved + 1, cloud_saved + 1)
    else:
        saved_at = max(prev_saved, cloud_saved)
    snap = {"schema": 3, "savedAt": saved_at, "p": base_p, "a": base_a}
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap_path.write_text(json.dumps(snap, ensure_ascii=False), encoding="utf-8")
    print(f"[мост] snapshot: {snap_path} ({snap_path.stat().st_size} Б), savedAt {saved_at}, "
          f"hidden {len(base_p['hidden'])}, answers {len(base_a['answers'])}")

    if no_git or out_path:
        print("[мост] без коммита")
        return 0

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
    pushed = git("push", "origin", "main")
    if pushed.returncode != 0:
        print(f"[мост] push failed: {pushed.stderr.strip()[:200]}")
        return 1
    print("[мост] snapshot закоммичен и отправлен")
    return 0


if __name__ == "__main__":
    sys.exit(main("--no-git" in sys.argv))
