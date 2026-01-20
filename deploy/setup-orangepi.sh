#!/bin/bash
set -euo pipefail

# Initial setup script for Orange Pi 5
# Run this on the Orange Pi after installing Docker
#
# Usage: curl -sSL <url>/setup-orangepi.sh | sudo bash
# Or copy to device and run: sudo ./setup-orangepi.sh

INSTALL_DIR="/opt/weather-station-db"
REGISTRY="${REGISTRY:-registry.local}"

echo "==> Creating installation directory..."
mkdir -p "${INSTALL_DIR}"
cd "${INSTALL_DIR}"

echo "==> Downloading configuration files..."
# If running from repo, copy files instead
if [[ -f "docker-compose.prod.yml" ]]; then
    cp docker-compose.prod.yml "${INSTALL_DIR}/"
else
    echo "ERROR: docker-compose.prod.yml not found"
    echo "Copy docker-compose.prod.yml to ${INSTALL_DIR}/ manually"
    exit 1
fi

echo "==> Creating .env file (edit as needed)..."
if [[ ! -f "${INSTALL_DIR}/.env" ]]; then
    cat > "${INSTALL_DIR}/.env" << 'EOF'
# Docker registry
REGISTRY=registry.local
VERSION=latest

# Producers to run (comma-separated: ndbc,openmeteo,nws,isd,oscar)
PRODUCERS=ndbc,openmeteo

# Kafka (optional - leave empty to disable)
KAFKA_BOOTSTRAP_SERVERS=

# InfluxDB (optional - leave empty to disable)
INFLUXDB_URL=
INFLUXDB_TOKEN=
INFLUXDB_ORG=
INFLUXDB_BUCKET=

# Alerting (optional)
NTFY_URL=
NTFY_TOPIC=
EOF
    echo "Created ${INSTALL_DIR}/.env - edit with your settings"
else
    echo ".env already exists, skipping"
fi

echo "==> Installing systemd service..."
cp deploy/weather-station-db.service /etc/systemd/system/ 2>/dev/null || \
    echo "Copy deploy/weather-station-db.service to /etc/systemd/system/ manually"

systemctl daemon-reload
systemctl enable weather-station-db

echo "==> Logging in to registry (if needed)..."
if ! docker pull "${REGISTRY}/weather-station-db:latest" 2>/dev/null; then
    echo "NOTE: Could not pull image. You may need to:"
    echo "  1. docker login ${REGISTRY}"
    echo "  2. Build and push the image from your dev machine"
fi

echo ""
echo "==> Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Edit ${INSTALL_DIR}/.env with your configuration"
echo "  2. From your dev machine, run: ./deploy/deploy.sh"
echo "  3. Or manually: sudo systemctl start weather-station-db"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status weather-station-db"
echo "  sudo journalctl -u weather-station-db -f"
echo "  docker compose -f docker-compose.prod.yml logs -f"
