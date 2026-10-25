#!/usr/bin/env bash
# 本地开发一键部署：构建 → 安装 → 启动 → 权限自检（并在缺权限时直接打开对应设置面板）。
#
# 和 scripts/build-mac.sh 的区别：
#   1. 用**稳定的本地签名身份**签名，而不是每次构建都不同的 ad-hoc 签名。macOS 的
#      TCC 授权是按「bundle id + 签名要求」记的：ad-hoc 每次 cdhash 都变，所以每次
#      重装后「麦克风 / 辅助功能」都要重新勾；用同一张证书签名则授权可以跨构建保留。
#   2. 装完自动启动，读应用日志判断权限是否齐全，缺哪个就帮你打开哪个设置面板。
#
# 用法（在 app/ 目录下）：
#     ./scripts/dev-deploy-mac.sh
#     OPENLESS_DEV_SIGN_IDENTITY="Developer ID Application: Name (TEAM)" ./scripts/dev-deploy-mac.sh
#     SKIP_BUILD=1 ./scripts/dev-deploy-mac.sh    # 跳过构建，只安装现有产物 + 启动 + 自检
#
# 第一次用稳定身份签名时仍需要授权一次（签名要求变了）；之后的构建不再需要。

set -euo pipefail

cd "$(dirname "$0")/.."

BUNDLE_ID="com.openless.app"
INSTALLED="/Applications/OpenLess.app"
LOG="$HOME/Library/Logs/OpenLess/openless.log"

# ── 选签名身份 ────────────────────────────────────────────────────────────
# 优先本地自签名开发证书（不需要安全时间戳，离线可签），其次 Developer ID /
# Apple Development；都没有则退回 ad-hoc（此时每次构建都要重新授权）。
pick_identity() {
  if [ -n "${OPENLESS_DEV_SIGN_IDENTITY:-}" ]; then
    printf '%s' "$OPENLESS_DEV_SIGN_IDENTITY"
    return
  fi
  local list pattern name
  list="$(security find-identity -v -p codesigning 2>/dev/null || true)"
  for pattern in 'Dev Signing' 'Developer ID Application' 'Apple Development'; do
    name="$(printf '%s\n' "$list" | grep -o "\"[^\"]*${pattern}[^\"]*\"" | head -1 | tr -d '"' || true)"
    if [ -n "$name" ]; then
      printf '%s' "$name"
      return
    fi
  done
  printf ''
}

IDENTITY="$(pick_identity)"
if [ -n "$IDENTITY" ]; then
  export APPLE_SIGNING_IDENTITY="$IDENTITY"
  echo "▶ 签名身份：$IDENTITY"
  echo "  （稳定身份 → 麦克风/辅助功能授权可跨构建保留，不用每次重勾）"
else
  export APPLE_SIGNING_IDENTITY="-"
  echo "▶ 没找到可用的本地签名证书，退回 ad-hoc 签名：每次重装都要重新授权。"
  echo "  一劳永逸的做法：钥匙串访问 → 证书助手 → 创建证书（自签名根、代码签名），"
  echo "  然后重跑本脚本；或设置 OPENLESS_DEV_SIGN_IDENTITY 指定已有证书。"
fi

# ── 构建 + 安装 ──────────────────────────────────────────────────────────
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  echo "▶ 构建 native-capsule sidecar"
  node scripts/build-native-capsule.mjs
  echo "▶ 构建并安装到 /Applications"
  ./scripts/build-mac.sh
else
  echo "▶ SKIP_BUILD=1：跳过构建"
fi

if [ ! -x "$INSTALLED/Contents/MacOS/openless" ]; then
  echo "✗ $INSTALLED 不存在，安装失败"
  exit 1
fi

echo "▶ 已安装签名："
codesign -dvv "$INSTALLED" 2>&1 | grep -E "^(Identifier|Authority|Signature)" | sed 's/^/  /' || true

# ── 启动 + 权限自检 ──────────────────────────────────────────────────────
LOG_OFFSET=0
if [ -f "$LOG" ]; then
  LOG_OFFSET="$(wc -c < "$LOG" | tr -d ' ')"
fi

echo "▶ 启动应用"
open "$INSTALLED"

new_log() {
  [ -f "$LOG" ] && tail -c +"$((LOG_OFFSET + 1))" "$LOG" 2>/dev/null || true
}

# 启动阶段有两道可能需要用户点一下的关卡，脚本会把它们明确报出来：
#   1. 钥匙串 ACL：换了签名身份后，读取凭据会弹「OpenLess 想使用你保存在钥匙串中的
#      机密信息」，应用会**阻塞**在启动早期（SecurityAgent 在跑、启动 banner 不出现）。
#   2. TCC：辅助功能 / 麦克风。
echo "▶ 等待启动 banner（最多 30s）…"
STARTED=0
for _ in $(seq 1 15); do
  sleep 2
  if new_log | grep -q "OpenLess 启动"; then
    STARTED=1
    break
  fi
done

KEYCHAIN_BLOCKED=0
if [ "$STARTED" != "1" ] && pgrep -x SecurityAgent > /dev/null 2>&1; then
  KEYCHAIN_BLOCKED=1
fi

if [ "$STARTED" = "1" ]; then
  echo "▶ 应用已启动，再等 10s 收集权限相关日志…"
  sleep 10
fi
NEW="$(new_log)"

MISSING_AX=0
MISSING_MIC=0
MIC_STATE=""

if printf '%s' "$NEW" | grep -q "Accessibility 权限未授予"; then
  MISSING_AX=1
fi
if printf '%s' "$NEW" | grep -q "hotkey listener installed"; then
  MISSING_AX=0
fi
MIC_STATE="$(printf '%s' "$NEW" \
  | grep -oE 'recordPermission raw=[^ ]+ \([0-9]+\) → [A-Za-z]+' \
  | tail -1 | grep -oE '[A-Za-z]+$' || true)"
case "$MIC_STATE" in
  Denied) MISSING_MIC=1 ;;
esac

echo
echo "── 部署自检 ─────────────────────────────────────────────"
if [ "$KEYCHAIN_BLOCKED" = "1" ]; then
  echo "⚠ 钥匙串授权弹窗拦住了启动（SecurityAgent 正在运行，日志里没有启动 banner）"
  echo "  屏幕上应该有「OpenLess 想使用你保存在钥匙串中的机密信息」——请点「始终允许」。"
  echo "  换签名身份后每个钥匙串项只会问一次；点完之后应用会自己继续启动。"
  echo "  全部点完可以再跑一次做权限自检：SKIP_BUILD=1 ./scripts/dev-deploy-mac.sh"
elif [ "$STARTED" != "1" ]; then
  echo "⚠ 30s 内没看到启动 banner，应用可能还在初始化（或弹窗被挡住了）"
fi

if [ "$STARTED" = "1" ]; then
  if [ "$MISSING_AX" = "1" ]; then
    echo "✗ 辅助功能：未授权 —— Fn+Esc / Ctrl+Esc 全局快捷键、录音中 Esc 取消都依赖它"
    echo "  已为你打开设置面板，请把 OpenLess 打开（应用每 3s 自动重试，不用重启）"
    open "x-apple.systempreferences:com.apple.preference.security?Privacy_Accessibility"
  elif [ -n "$NEW" ]; then
    echo "✓ 辅助功能：已授权（全局快捷键可用）"
  else
    echo "? 辅助功能：日志里没有相关记录，无法判断"
  fi

  if [ "$MISSING_MIC" = "1" ]; then
    echo "✗ 麦克风：被拒绝"
    echo "  已为你打开设置面板，请把 OpenLess 的开关打开"
    open "x-apple.systempreferences:com.apple.preference.security?Privacy_Microphone"
  elif [ "$MIC_STATE" = "NotDetermined" ]; then
    echo "… 麦克风：应用正在请求授权，请在弹窗里点「允许」"
  elif [ "$MIC_STATE" = "Granted" ]; then
    echo "✓ 麦克风：已授权"
  else
    echo "? 麦克风：日志里还没看到授权状态；录音时若没声音请检查"
    echo "  系统设置 → 隐私与安全性 → 麦克风 → OpenLess"
  fi
fi
echo "────────────────────────────────────────────────────────"
echo "✓ 部署完成：${INSTALLED}（bundle id ${BUNDLE_ID}）"
