#!/bin/bash
# E-Fence Edge Agent — Raspberry Pi Installer
# Run as root: sudo bash install.sh
# ============================================================

set -e

INSTALL_DIR="/opt/efence"
CONFIG_DIR="/etc/efence"
DATA_DIR="/var/lib/efence"
SERVICE_FILE="/etc/systemd/system/efence-agent.service"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ---- Require root ----
[ "$EUID" -eq 0 ] || error "Please run as root: sudo bash install.sh"

info "=== E-Fence Edge Agent Installer ==="

# ---- System packages ----
info "Installing system packages..."
apt-get update -qq
apt-get install -y \
    wireless-tools \
    iw \
    bluetooth \
    bluez \
    python3-pip \
    python3-dbus \
    libglib2.0-dev \
    --no-install-recommends

# ---- Python packages ----
info "Installing Python packages..."
pip3 install --quiet \
    bleak==0.14.3 \
    pyyaml \
    requests

# ---- Verify bleak installed ----
python3 -c "import bleak; print('bleak OK:', bleak.__version__)" || \
    error "bleak installation failed"

# ---- Directories ----
info "Creating directories..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"
chmod 700 "$CONFIG_DIR"    # token lives here — restrict access
chmod 755 "$DATA_DIR"

# ---- Copy agent code ----
info "Installing agent code to $INSTALL_DIR..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cp -r "$SCRIPT_DIR/efence" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"

# ---- Config file ----
if [ ! -f "$CONFIG_DIR/config.yaml" ]; then
    info "Installing config template to $CONFIG_DIR/config.yaml"
    cp "$SCRIPT_DIR/config.yaml.template" "$CONFIG_DIR/config.yaml"
    warn "Edit $CONFIG_DIR/config.yaml before starting the agent"
else
    info "Config already exists at $CONFIG_DIR/config.yaml — skipping"
fi

# ---- Environment file (Databricks token) ----
if [ ! -f "$CONFIG_DIR/environment" ]; then
    info "Creating $CONFIG_DIR/environment for secrets"
    cat > "$CONFIG_DIR/environment" << 'EOF'
# Databricks Personal Access Token
# Replace the placeholder with your actual token.
# This file is chmod 600 — only root can read it.
EFENCE_DATABRICKS_TOKEN=dapi_replace_me
EOF
    chmod 600 "$CONFIG_DIR/environment"
    warn "Set your Databricks PAT in $CONFIG_DIR/environment"
else
    info "Environment file already exists — skipping"
fi

# ---- Bluetooth: allow non-root BLE scan (optional, agent runs as root anyway) ----
info "Enabling Bluetooth service..."
systemctl enable bluetooth
systemctl start bluetooth || warn "Bluetooth service start failed — check with: systemctl status bluetooth"

# ---- systemd service ----
info "Installing systemd service..."
cp "$SCRIPT_DIR/efence-agent.service" "$SERVICE_FILE"
systemctl daemon-reload
systemctl enable efence-agent

# ---- Done ----
echo ""
info "=== Installation complete ==="
echo ""
echo "  Next steps:"
echo "  1. Set your Databricks workspace URL in $CONFIG_DIR/config.yaml"
echo "  2. Set your Databricks PAT in $CONFIG_DIR/environment"
echo "     (replace 'dapi_replace_me' with your actual token)"
echo "  3. Set your sensor coordinates in $CONFIG_DIR/config.yaml"
echo "     (if this is a fixed sensor)"
echo "  4. Start the agent:"
echo "       sudo systemctl start efence-agent"
echo "  5. Watch the logs:"
echo "       journalctl -u efence-agent -f"
echo ""
echo "  To run manually for testing:"
echo "    export EFENCE_DATABRICKS_TOKEN='dapi...'"
echo "    cd $INSTALL_DIR"
echo "    python3 -m efence.main --config $CONFIG_DIR/config.yaml --log-level DEBUG"
echo ""
