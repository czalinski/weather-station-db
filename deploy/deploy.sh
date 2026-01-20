#!/bin/bash
set -euo pipefail

# Deploy weather-station-db to Orange Pi 5
#
# Usage:
#   ./deploy.sh [VERSION] [HOST]
#
# Examples:
#   ./deploy.sh                    # Deploy 'latest' to default host
#   ./deploy.sh v1.2.0             # Deploy specific version
#   ./deploy.sh v1.2.0 pi@opi5     # Deploy to specific host

VERSION="${1:-latest}"
REMOTE_HOST="${2:-${DEPLOY_HOST:-orangepi}}"
REGISTRY="${REGISTRY:-registry.local}"
IMAGE_NAME="weather-station-db"

echo "==> Building image for linux/arm64..."
docker build --platform linux/arm64 -t "${REGISTRY}/${IMAGE_NAME}:${VERSION}" .

echo "==> Pushing to registry..."
docker push "${REGISTRY}/${IMAGE_NAME}:${VERSION}"

if [[ "${VERSION}" != "latest" ]]; then
    echo "==> Tagging as latest..."
    docker tag "${REGISTRY}/${IMAGE_NAME}:${VERSION}" "${REGISTRY}/${IMAGE_NAME}:latest"
    docker push "${REGISTRY}/${IMAGE_NAME}:latest"
fi

echo "==> Deploying to ${REMOTE_HOST}..."
ssh "${REMOTE_HOST}" "cd /opt/weather-station-db && sudo systemctl restart weather-station-db"

echo "==> Checking status..."
ssh "${REMOTE_HOST}" "sudo systemctl status weather-station-db --no-pager"

echo "==> Done! Deployed ${VERSION} to ${REMOTE_HOST}"
