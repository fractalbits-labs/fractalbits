#!/bin/bash
set -euo pipefail

export DOCKER_S3_ENDPOINT="http://${rss_a_ip}:8080"
export DOCKER_S3_BUCKET="fractalbits-bootstrap"
export CLUSTER_ID="${cluster_id}"
export DEPLOY_TARGET="${deploy_target}"
export SERVICE_ROLE="${service_role}"
export INSTANCE_ROLE="${instance_role}"

LOG_FILE="/var/log/fractalbits-bootstrap.log"

echo "$(date): Starting fractalbits bootstrap for $SERVICE_ROLE ($INSTANCE_ROLE)" | tee -a "$LOG_FILE"

# For RSS leader, Docker S3 is started by the xtask deploy command before
# other instances boot. Skip the Docker S3 wait.
if [ "$SERVICE_ROLE" = "root_server" ] && [ "$INSTANCE_ROLE" = "leader" ]; then
    echo "$(date): RSS leader - waiting for xtask deploy to set up Docker S3" | tee -a "$LOG_FILE"
    exit 0
fi

# Wait for Docker S3 to be available (RSS-A starts it during xtask deploy)
echo "$(date): Waiting for Docker S3 at $DOCKER_S3_ENDPOINT ..." | tee -a "$LOG_FILE"
for i in $(seq 1 120); do
    if curl -sf "$DOCKER_S3_ENDPOINT" > /dev/null 2>&1; then
        echo "$(date): Docker S3 is available" | tee -a "$LOG_FILE"
        break
    fi
    if [ "$i" -eq 120 ]; then
        echo "$(date): ERROR: Docker S3 not available after 4 minutes" | tee -a "$LOG_FILE"
        exit 1
    fi
    sleep 2
done

# Download and run bootstrap
echo "$(date): Downloading bootstrap script..." | tee -a "$LOG_FILE"
curl -sf "$DOCKER_S3_ENDPOINT/$DOCKER_S3_BUCKET/bootstrap.sh" -o /tmp/bootstrap.sh
chmod +x /tmp/bootstrap.sh

echo "$(date): Running bootstrap script..." | tee -a "$LOG_FILE"
nohup /tmp/bootstrap.sh >> "$LOG_FILE" 2>&1 &
