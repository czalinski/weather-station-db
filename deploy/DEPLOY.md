# Deploying to Orange Pi 5

## Prerequisites

### Install Docker on Orange Pi 5

The Orange Pi 5 runs ARM64 Linux (typically Armbian or Ubuntu). Install Docker using the official convenience script:

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com | sudo sh

# Add your user to docker group (logout/login required)
sudo usermod -aG docker $USER

# Enable Docker to start on boot
sudo systemctl enable docker

# Verify installation
docker --version
docker run --rm hello-world
```

### Configure Private Registry Access

```bash
# Login to your private registry
docker login registry.local

# Or for insecure registries (not recommended for production)
# Add to /etc/docker/daemon.json:
# { "insecure-registries": ["registry.local:5000"] }
# Then: sudo systemctl restart docker
```

## Initial Setup on Orange Pi

1. Copy deployment files to the device:

```bash
# From your dev machine
scp docker-compose.prod.yml orangepi:/opt/weather-station-db/
scp deploy/weather-station-db.service orangepi:/tmp/
scp deploy/.env.example orangepi:/opt/weather-station-db/.env

# On the Orange Pi
sudo mv /tmp/weather-station-db.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable weather-station-db
```

2. Edit configuration:

```bash
# On the Orange Pi
sudo nano /opt/weather-station-db/.env
```

## Deploying Updates

From your dev machine:

```bash
# Deploy latest
./deploy/deploy.sh

# Deploy specific version
./deploy/deploy.sh v1.2.0

# Deploy to specific host
./deploy/deploy.sh v1.2.0 pi@my-orangepi.local
```

The deploy script will:
1. Build the image for ARM64
2. Push to your private registry
3. SSH to the Orange Pi and restart the service

## Manual Operations

```bash
# On the Orange Pi

# Check service status
sudo systemctl status weather-station-db

# View logs
sudo journalctl -u weather-station-db -f

# View container logs
cd /opt/weather-station-db
docker compose -f docker-compose.prod.yml logs -f

# Restart service
sudo systemctl restart weather-station-db

# Stop service
sudo systemctl stop weather-station-db

# Pull latest image manually
docker compose -f docker-compose.prod.yml pull
```

## Data Persistence

Weather data is stored in the `weather-data` Docker volume. To backup:

```bash
# Find volume location
docker volume inspect weather-station-db_weather-data

# Or copy from running container
docker cp weather-station-db:/app/data ./backup/
```

## Troubleshooting

### Image won't pull
- Verify registry login: `docker login registry.local`
- Check registry URL in `.env`
- Ensure image was pushed from dev machine

### Service won't start
- Check logs: `sudo journalctl -u weather-station-db -e`
- Verify Docker is running: `sudo systemctl status docker`
- Check compose file: `docker compose -f docker-compose.prod.yml config`

### Out of memory
- Adjust limits in `docker-compose.prod.yml` under `deploy.resources`
- Default is 512MB limit, 128MB reserved
