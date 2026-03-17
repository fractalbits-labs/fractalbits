#!/bin/bash
set -euo pipefail

LOG_FILE="/var/log/fractalbits-bootstrap.log"

mkdir -p /opt/fractalbits/bin
ARCH=$(arch)
gcloud storage cp "gs://${gcs_bucket}/$ARCH/fractalbits-bootstrap" /opt/fractalbits/bin/fractalbits-bootstrap
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap "gs://${gcs_bucket}" 2>&1 | tee -a "$LOG_FILE"
