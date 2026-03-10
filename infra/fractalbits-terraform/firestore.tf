# Firestore "fractalbits" database is created outside Terraform because:
# 1. It persists across terraform destroy (deletion_policy=ABANDON)
# 2. Re-creating it after destroy causes "already exists" errors
#
# Create it manually if needed:
#   gcloud firestore databases create --project=<project-id> --location=<region> --type=firestore-native --database=fractalbits
