#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u

# --- Configuration Variables ---
# Replace these placeholders with your actual values or set them as environment variables.
# Required:
PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
REGION="${REGION:-YOUR_GCP_REGION}" # e.g., us-central1
BUCKET_NAME="${BUCKET_NAME:-YOUR_GCS_BUCKET_NAME}"
YOUR_EMAIL="${YOUR_EMAIL:-YOUR_EMAIL_ADDRESS}" # For GCS admin access (optional)

# Resource Naming:
SERVICE_NAME="${SERVICE_NAME:-YOUR_SERVICE_NAME}" # e.g., my-data-fetcher
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-YOUR_SERVICE_ACCOUNT_NAME}" # e.g., my-data-fetcher-sa
SCHEDULER_JOB_NAME="${SCHEDULER_JOB_NAME:-YOUR_SCHEDULER_JOB_NAME}" # e.g., my-data-fetcher-job
REPO_NAME="${REPO_NAME:-YOUR_ARTIFACT_REGISTRY_REPO_NAME}" # e.g., my-docker-repo

# Calculated Variables:
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
AR_HOSTNAME="${REGION}-docker.pkg.dev"
IMAGE_TAG="${AR_HOSTNAME}/${PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:latest"

# --- Optional Environment Variables for Cloud Run ---
# Add any other environment variables your service needs, comma-separated
# Example: OTHER_VARS="KEY1=VALUE1,KEY2=VALUE2"
# Ensure GCS_BUCKET_NAME is included if your app uses it directly
RUN_ENV_VARS="GCS_BUCKET_NAME=${BUCKET_NAME},LOCAL_MODE=false" # Add other vars like ,KALSHI_ENV=PROD if needed

echo "--- Configuration ---"
echo "PROJECT_ID: ${PROJECT_ID}"
echo "REGION: ${REGION}"
echo "BUCKET_NAME: ${BUCKET_NAME}"
echo "SERVICE_NAME: ${SERVICE_NAME}"
echo "SERVICE_ACCOUNT_EMAIL: ${SERVICE_ACCOUNT_EMAIL}"
echo "SCHEDULER_JOB_NAME: ${SCHEDULER_JOB_NAME}"
echo "IMAGE_TAG: ${IMAGE_TAG}"
echo "---------------------"
echo

# --- Prerequisites ---
echo "STEP 0: Ensuring gcloud is configured for project ${PROJECT_ID}..."
gcloud config set project ${PROJECT_ID}
echo "DONE."
echo

echo "STEP 1: Enabling required Google Cloud APIs..."
# Ensure all necessary APIs for your specific application are listed here
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  --project=${PROJECT_ID}
echo "DONE."
echo

# --- Setup ---
echo "STEP 2: Creating service account ${SERVICE_ACCOUNT_NAME}..."
# Check if SA exists, create if not
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT_EMAIL} --project=${PROJECT_ID} > /dev/null 2>&1; then
  gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
    --display-name="${SERVICE_NAME} Service Account" \
    --project=${PROJECT_ID}
else
  echo "Service account ${SERVICE_ACCOUNT_EMAIL} already exists."
fi
echo "DONE."
echo

echo "STEP 3: Granting roles to service account ${SERVICE_ACCOUNT_EMAIL}..."
# Role for Cloud Run to access secrets (if needed)
echo " - Granting roles/secretmanager.secretAccessor (if needed)..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" \
  --condition=None --quiet
# Role for Cloud Run to write to GCS bucket (Project level - refine if needed)
echo " - Granting roles/storage.objectCreator..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/storage.objectCreator" \
  --condition=None --quiet
# Add any other roles your service account needs here
echo "DONE."
echo

echo "STEP 4: Creating GCS bucket gs://${BUCKET_NAME} and granting access..."
# Create bucket if it doesn't exist
if ! gsutil ls gs://${BUCKET_NAME} > /dev/null 2>&1; then
  gsutil mb -l ${REGION} gs://${BUCKET_NAME}
else
  echo "Bucket gs://${BUCKET_NAME} already exists."
fi
# Grant service account permissions to write objects in the bucket
echo " - Granting SA objectAdmin on bucket..."
gsutil iam ch \
  serviceAccount:${SERVICE_ACCOUNT_EMAIL}:objectAdmin \
  gs://${BUCKET_NAME}
# Grant your user account admin permissions (optional)
if [ -n "${YOUR_EMAIL}" ]; then
  echo " - Granting User (${YOUR_EMAIL}) objectAdmin on bucket..."
  gsutil iam ch \
    user:${YOUR_EMAIL}:objectAdmin \
    gs://${BUCKET_NAME}
fi
echo "DONE."
echo

echo "STEP 5: Creating Artifact Registry repository ${REPO_NAME}..."
# Create repo if it doesn't exist
if ! gcloud artifacts repositories describe ${REPO_NAME} --location=${REGION} --project=${PROJECT_ID} > /dev/null 2>&1; then
  gcloud artifacts repositories create ${REPO_NAME} \
      --repository-format=docker \
      --location=${REGION} \
      --description="Docker repository for ${SERVICE_NAME}" \
      --project=${PROJECT_ID}
else
    echo "Artifact Registry repository ${REPO_NAME} already exists."
fi
echo "DONE."
echo

# --- Build & Deploy ---
echo "STEP 6: Building and pushing Docker image using Cloud Build..."
# Assumes script is run from the project root directory containing the Dockerfile
gcloud builds submit . --tag ${IMAGE_TAG} --project=${PROJECT_ID} --quiet
echo "DONE."
echo

echo "STEP 7: Deploying Cloud Run service ${SERVICE_NAME}..."
gcloud run deploy ${SERVICE_NAME} \
  --image=${IMAGE_TAG} \
  --platform=managed \
  --region=${REGION} \
  --service-account=${SERVICE_ACCOUNT_EMAIL} \
  --ingress=all `# Or internal / internal-and-cloud-load-balancing` \
  --no-allow-unauthenticated `# Require authentication` \
  --port=8080 `# Port exposed in Dockerfile` \
  --set-env-vars="${RUN_ENV_VARS}" \
  --project=${PROJECT_ID} --quiet

# Get the service URL
CLOUD_RUN_SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} --platform=managed --region=${REGION} --format='value(status.url)' --project=${PROJECT_ID})
echo "Cloud Run Service URL: ${CLOUD_RUN_SERVICE_URL}"
echo "DONE."
echo

# --- IAM Permissions for Invocation ---
echo "STEP 8: Granting invocation permissions..."
# Grant Cloud Scheduler's default SA permission to invoke the Cloud Run service
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
SCHEDULER_SA_EMAIL="service-${PROJECT_NUMBER}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
echo " - Granting Cloud Scheduler SA (${SCHEDULER_SA_EMAIL}) roles/run.invoker..."
gcloud run services add-iam-policy-binding ${SERVICE_NAME} \
    --member="serviceAccount:${SCHEDULER_SA_EMAIL}" \
    --role="roles/run.invoker" \
    --region=${REGION} \
    --platform=managed \
    --project=${PROJECT_ID} --quiet

# Grant the Cloud Run service account permission to invoke itself (Required for OIDC auth from Scheduler)
echo " - Granting Cloud Run SA (${SERVICE_ACCOUNT_EMAIL}) roles/run.invoker on itself..."
gcloud run services add-iam-policy-binding ${SERVICE_NAME} \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/run.invoker" \
    --region=${REGION} \
    --platform=managed \
    --project=${PROJECT_ID} --quiet
echo "DONE."
echo

# --- Create Scheduler Job ---
echo "STEP 9: Creating Cloud Scheduler job ${SCHEDULER_JOB_NAME}..."
# Check if job exists, create if not
if ! gcloud scheduler jobs describe ${SCHEDULER_JOB_NAME} --location=${REGION} --project=${PROJECT_ID} > /dev/null 2>&1; then
  gcloud scheduler jobs create http ${SCHEDULER_JOB_NAME} \
    --schedule="0 */2 * * *" `# Example: Every 2 hours. Adjust as needed.` \
    --time-zone="America/New_York" `# Adjust timezone if necessary` \
    --uri="${CLOUD_RUN_SERVICE_URL}/run" `# Ensure '/run' or your target endpoint is correct` \
    --http-method=POST \
    --oidc-service-account-email=${SERVICE_ACCOUNT_EMAIL} \
    --oidc-token-audience=${CLOUD_RUN_SERVICE_URL} \
    --location=${REGION} \
    --description="Triggers ${SERVICE_NAME} Cloud Run service" \
    --max-retry-attempts=3 \
    --min-backoff=30s \
    --max-backoff=300s \
    --max-doublings=5 \
    --attempt-deadline=1800s `# 30 minutes` \
    --project=${PROJECT_ID} --quiet
else
    echo "Cloud Scheduler job ${SCHEDULER_JOB_NAME} already exists."
fi
echo "DONE."
echo

echo "--- Deployment Script Finished ---"
echo "Verify the deployment manually (check logs, GCS, manually run job)."
