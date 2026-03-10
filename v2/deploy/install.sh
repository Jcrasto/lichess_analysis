#!/usr/bin/env bash
# install.sh — register lichess_analysis as a user-level service (no sudo)
#
# Usage:
#   bash deploy/install.sh             # install
#   bash deploy/install.sh --uninstall # remove
#   bash deploy/install.sh --enable    # also auto-start on login

set -euo pipefail

REPO=/Users/joshcrasto/Projects/lichess_analysis/v2
LOGS=$REPO/logs
DEPLOY=$REPO/deploy
CONF=$DEPLOY/supervisord.conf
LABEL="com.joshcrasto.lichess_analysis"
PLIST=$DEPLOY/"${LABEL}.plist"

info()    { echo "[install] $*"; }
success() { echo "[install] ✓ $*"; }
warn()    { echo "[install] ⚠ $*"; }
die()     { echo "[install] ✗ $*" >&2; exit 1; }

OS=$(uname -s)

# ── macOS ─────────────────────────────────────────────────────────────────────

DOMAIN="gui/$(id -u)"

install_macos() {
    which supervisord >/dev/null 2>&1 || die "supervisord not found — run: brew install supervisor"
    SUPERVISORD=$(which supervisord)

    # Patch the plist to use the actual supervisord path (in case it differs)
    sed -i '' "s|/usr/local/bin/supervisord|$SUPERVISORD|g" "$PLIST"

    mkdir -p "$LOGS" ~/Library/LaunchAgents
    [ -f "$PLIST" ] || die "Plist not found: $PLIST"

    dest=~/Library/LaunchAgents/"${LABEL}.plist"
    launchctl bootout "$DOMAIN" "$dest" 2>/dev/null || true
    cp "$PLIST" "$dest"
    launchctl bootstrap "$DOMAIN" "$dest"
    success "Registered $LABEL"

    echo ""
    echo "  Start:   launchctl kickstart $DOMAIN/$LABEL"
    echo "  Stop:    launchctl kill SIGTERM $DOMAIN/$LABEL"
    echo "  Status:  supervisorctl -c $CONF status"
    echo "  Logs:    $LOGS/"
    echo ""
    echo "  Or add to ~/.zshrc:"
    echo "    alias lichess-start='launchctl kickstart $DOMAIN/$LABEL'"
    echo "    alias lichess-stop='launchctl kill SIGTERM $DOMAIN/$LABEL'"
    echo ""
    warn "Service will NOT auto-start on login. To enable: bash deploy/install.sh --enable"
}

uninstall_macos() {
    dest=~/Library/LaunchAgents/"${LABEL}.plist"
    launchctl kill SIGTERM "$DOMAIN/$LABEL" 2>/dev/null || true
    launchctl bootout "$DOMAIN" "$dest" 2>/dev/null || true
    rm -f "$dest"
    success "Removed $LABEL"
}

enable_macos() {
    dest=~/Library/LaunchAgents/"${LABEL}.plist"
    [ -f "$dest" ] || die "$dest not found — run install.sh first"
    launchctl enable "$DOMAIN/$LABEL"
    success "Auto-start on login enabled for $LABEL"
}

# ── Linux ─────────────────────────────────────────────────────────────────────

SVC_FILE=$DEPLOY/lichess_analysis.service
SYSTEMD_USER_DIR=~/.config/systemd/user

install_linux() {
    which supervisord >/dev/null 2>&1 || die "supervisord not found — run: pip install supervisor  or  sudo apt install supervisor"
    SUPERVISORD=$(which supervisord)
    SUPERVISORCTL=$(which supervisorctl)

    # Patch service file to use actual binary paths
    sed -i "s|/usr/local/bin/supervisord|$SUPERVISORD|g; s|/usr/local/bin/supervisorctl|$SUPERVISORCTL|g" "$SVC_FILE"

    mkdir -p "$LOGS" "$SYSTEMD_USER_DIR"
    cp "$SVC_FILE" "$SYSTEMD_USER_DIR/lichess_analysis.service"
    systemctl --user daemon-reload
    success "Installed lichess_analysis.service"

    echo ""
    echo "  Start:   systemctl --user start lichess_analysis"
    echo "  Stop:    systemctl --user stop lichess_analysis"
    echo "  Status:  systemctl --user status lichess_analysis"
    echo "           supervisorctl -c $CONF status"
    echo "  Logs:    journalctl --user -u lichess_analysis -f"
    echo "           $LOGS/"
    echo ""
    warn "Service will NOT auto-start on login. To enable: bash deploy/install.sh --enable"
}

uninstall_linux() {
    systemctl --user stop lichess_analysis 2>/dev/null || true
    systemctl --user disable lichess_analysis 2>/dev/null || true
    rm -f "$SYSTEMD_USER_DIR/lichess_analysis.service"
    systemctl --user daemon-reload
    success "Removed lichess_analysis.service"
}

enable_linux() {
    systemctl --user enable lichess_analysis
    success "Auto-start on login enabled"
    warn "Also run: sudo loginctl enable-linger \$(whoami)  (to survive after logout)"
}

# ── main ──────────────────────────────────────────────────────────────────────

UNINSTALL=false
ENABLE=false
for arg in "$@"; do
    case "$arg" in
        --uninstall) UNINSTALL=true ;;
        --enable)    ENABLE=true ;;
    esac
done

if $UNINSTALL; then
    case "$OS" in
        Darwin) uninstall_macos ;;
        Linux)  uninstall_linux ;;
        *) die "Unsupported OS: $OS" ;;
    esac
    exit 0
fi

if $ENABLE; then
    case "$OS" in
        Darwin) enable_macos ;;
        Linux)  enable_linux ;;
        *) die "Unsupported OS: $OS" ;;
    esac
    exit 0
fi

case "$OS" in
    Darwin) install_macos ;;
    Linux)  install_linux ;;
    *) die "Unsupported OS: $OS" ;;
esac

info "Logs → $LOGS/"
