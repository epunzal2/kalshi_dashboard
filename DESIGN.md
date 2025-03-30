Overall Project Goal: To create an automated system that periodically fetches market data from the Kalshi API for a predefined list of tickers and stores this data in Google Cloud Storage (GCS) for later analysis and visualization.
Core Components Implemented:
Data Fetcher (src/data_fetcher.py): A Python Flask application using a custom Kalshi client (src/clients.py) to interact with the Kalshi API (handling authentication via keys stored in Secret Manager). It reads tickers from tickers.txt and saves fetched market data as JSON files structured by ticker into a GCS bucket.
Containerization (Dockerfile): Packages the data fetcher application for deployment.
Cloud Run Service (kalshi-data-fetcher): Hosts the containerized data fetcher application on Google Cloud. It's configured to require authentication and runs using a dedicated service account (kalshi-data-fetcher-sa).
Cloud Storage Bucket (kalshi-market-data-storage): Stores the output JSON files under the market_data/ prefix.
Secret Manager: Securely stores the Kalshi API Key ID (prod-keyid) and Private Key (prod-keyfile).
Cloud Scheduler Job (kalshi-data-fetcher-job): Triggers the Cloud Run service's /run endpoint every 2 hours using an HTTP POST request, authenticated via OIDC using the Cloud Run service account's identity.
Deployment Artifacts: cloud/deploy-scheduler.md (manual steps), cloud/deploy-scheduler.sh (executable script), and cloud/deploy-scheduler-template.sh (anonymized template) were created to manage the deployment.
Recent Troubleshooting & Setup:
We successfully deployed the Cloud Run service and the Cloud Scheduler job.
Initial attempts by the scheduler to trigger the Cloud Run service resulted in 403 Permission Denied errors.
This was resolved by granting the Cloud Run service account (kalshi-data-fetcher-sa@...) the roles/run.invoker permission on the Cloud Run service itself. This allows the service to accept requests authenticated with its own OIDC token, which is how Cloud Scheduler is configured to call it.
We verified the fix by manually triggering the scheduler job and observing successful execution logs in Cloud Run, indicating data was fetched and saved.
Current Status: The automated data fetching pipeline is fully deployed and operational on Google Cloud Platform. Market data for tickers in tickers.txt is being fetched and stored in gs://kalshi-market-data-storage/market_data/ every two hours.