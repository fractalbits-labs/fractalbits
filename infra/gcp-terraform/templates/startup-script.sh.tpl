#!/bin/bash
set -euo pipefail

LOG_FILE="/var/log/fractalbits-bootstrap.log"

mkdir -p /opt/fractalbits/bin
ARCH=$(arch)
# Retry download to handle IAM propagation delay after service account creation
for attempt in 1 2 3 4 5; do
  gcloud storage cp "gs://${gcs_bucket}/$ARCH/fractalbits-bootstrap" /opt/fractalbits/bin/fractalbits-bootstrap && break
  echo "Download attempt $attempt failed, retrying in 30s..." | tee -a "$LOG_FILE"
  sleep 30
done
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap "gs://${gcs_bucket}" ${role_args} 2>&1 | tee -a "$LOG_FILE"
