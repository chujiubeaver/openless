#!/usr/bin/env python3
"""主链路延时报告：对比「实时译文预览开 / 关」两类会话的主听写链路耗时。

用途：验证译文预览有没有拖累主体功能（麦克风启动、编码上传、端到端插入）。
数据全部来自应用日志，不需要额外埋点：

    ~/Library/Logs/OpenLess/openless.log

每个会话的归类依据（按优先级）：
  1. `[live-translation] preview attached session_id=...` / `preview unavailable session_id=...`
     —— 精确到会话，说明这次会话确实建了译文通道（ON）。
  2. 老日志没有该标记时，退化为「本次应用运行内出现过 qwen-livetranslate /
     live-translation 警告」→ 整个 run 记为 ON（近似，会在输出里标注）。

指标（中位数）：
  mic_ms   打开设备 → 第一帧输出 PCM（`[audio-startup] phase=first_output_pcm`）
  build_ms cpal 建流耗时（`phase=stream_built duration_us`）
  enc_s    录音停止 → omni POST（本地 WAV 编码 + 请求建立）
  srv_s    omni POST → stream done（服务端 + 网络，作为环境噪声的对照组）
  e2e_s    录音停止 → 插入完成（用户感知的总时长）

用法：
    python3 scripts/main-path-perf-report.py [日志路径]
"""

from __future__ import annotations

import collections
import datetime as dt
import re
import sys
from pathlib import Path

DEFAULT_LOG = Path.home() / "Library/Logs/OpenLess/openless.log"
LINE = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+)Z\s+\[\w+\]\s+(.*)$")


def parse_ts(raw: str) -> dt.datetime:
    head, _, frac = raw.partition(".")
    return dt.datetime.fromisoformat(f"{head}.{(frac + '000000')[:6]}")


def median(values: list[float]) -> float | None:
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return round(values[len(values) // 2], 3)


def main() -> int:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_LOG
    if not path.exists():
        print(f"找不到日志：{path}")
        return 1

    runs: list[dict] = []
    current_run = -1
    sessions: dict[str, dict] = {}
    last_sid: str | None = None
    attached_sids: set[str] = set()

    for line in path.read_text(errors="replace").splitlines():
        match = LINE.match(line.strip())
        if not match:
            continue
        stamp = parse_ts(match.group(1))
        msg = match.group(2)

        if "=== OpenLess 启动 ===" in msg:
            current_run = len(runs)
            runs.append({"start": stamp, "marked": False})
        if "qwen-livetranslate" in msg or "live-translation" in msg:
            if current_run >= 0:
                runs[current_run]["marked"] = True

        sid_match = re.search(r"session_id=(\S+)", msg)
        if msg.startswith("[dictation] start session_id=") and sid_match:
            last_sid = sid_match.group(1)
            sessions[last_sid] = {"run": current_run, "start": stamp, "on": None}
        elif "[live-translation] preview" in msg and sid_match:
            attached_sids.add(sid_match.group(1))
        elif "[audio-startup]" in msg and last_sid and last_sid in sessions:
            since = re.search(r"since_open_us=(\d+)", msg)
            dur = re.search(r"duration_us=(\d+)", msg)
            if "first_output_pcm" in msg and since:
                sessions[last_sid]["mic_ms"] = int(since.group(1)) / 1000
            if "phase=stream_built" in msg and dur:
                sessions[last_sid]["build_ms"] = int(dur.group(1)) / 1000
        elif "cpal Stream dropped" in msg and last_sid and last_sid in sessions:
            sessions[last_sid]["stop"] = stamp
        elif msg.startswith("[omni] POST") and last_sid and last_sid in sessions:
            sessions[last_sid]["post"] = stamp
        elif msg.startswith("[omni] stream done") and last_sid and last_sid in sessions:
            sessions[last_sid]["done"] = stamp
        elif "phase=completed outcome=" in msg and sid_match:
            sid = sid_match.group(1)
            if sid in sessions:
                sessions[sid]["completed"] = stamp

    groups: dict[str, list[dict]] = collections.defaultdict(list)
    for sid, s in sessions.items():
        if "stop" not in s or "completed" not in s:
            continue
        on = sid in attached_sids
        if not on:
            run = s.get("run", -1)
            # Only fall back to the coarse run-level heuristic when this run has
            # NO exact markers at all; otherwise a run mixing preview-on and
            # preview-off sessions would mislabel every off session as on.
            if 0 <= run < len(runs):
                marked_ids_in_run = any(
                    ss.get("run") == run and ss_id in attached_sids
                    for ss_id, ss in sessions.items()
                )
                on = False if marked_ids_in_run else bool(runs[run]["marked"])
        row = {
            "sid": sid[:8],
            "start": s["start"],
            "mic_ms": s.get("mic_ms"),
            "build_ms": s.get("build_ms"),
            "enc_s": round((s["post"] - s["stop"]).total_seconds(), 3) if "post" in s else None,
            "srv_s": (
                round((s["done"] - s["post"]).total_seconds(), 3)
                if "done" in s and "post" in s
                else None
            ),
            "e2e_s": round((s["completed"] - s["stop"]).total_seconds(), 3),
        }
        groups["ON " if on else "OFF"].append(row)

    exact = len(attached_sids)
    print(f"日志：{path}")
    print(f"会话总数（有完整 stop→completed）：{sum(len(v) for v in groups.values())}")
    print(f"其中带精确 preview attached 标记的会话：{exact}")
    if exact == 0:
        print("⚠ 没有精确标记，ON/OFF 按「该次运行内出现过译文警告」近似归类")
    print()
    header = f"{'preview':>7} {'n':>3} | {'mic_ms':>7} {'build_ms':>8} {'enc_s':>6} {'srv_s':>6} {'e2e_s':>6}"
    print(header)
    print("-" * len(header))
    for key in ("OFF", "ON "):
        rows = groups.get(key, [])
        if not rows:
            print(f"{key:>7} {0:>3} |  （无样本）")
            continue
        print(
            f"{key:>7} {len(rows):>3} | "
            f"{str(median([r['mic_ms'] for r in rows])):>7} "
            f"{str(median([r['build_ms'] for r in rows])):>8} "
            f"{str(median([r['enc_s'] for r in rows])):>6} "
            f"{str(median([r['srv_s'] for r in rows])):>6} "
            f"{str(median([r['e2e_s'] for r in rows])):>6}"
        )
    print()
    print("最近 12 个会话：")
    recent = sorted(
        (dict(r, on=k) for k, v in groups.items() for r in v),
        key=lambda r: r["start"],
    )[-12:]
    for r in recent:
        print(
            f"  {r['on'].strip():>3} {r['start'].strftime('%m-%d %H:%M:%S')} {r['sid']} "
            f"mic={r['mic_ms']}ms build={r['build_ms']}ms enc={r['enc_s']}s "
            f"srv={r['srv_s']}s e2e={r['e2e_s']}s"
        )
    print()
    print("判读：srv_s 是服务端耗时，作为环境噪声对照；若 ON 组只有 srv_s 偏高而")
    print("      mic_ms/build_ms/enc_s 与 OFF 组接近，说明差异来自网络而非本地链路。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
