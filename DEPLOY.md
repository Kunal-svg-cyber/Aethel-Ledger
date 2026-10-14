# Deploying to Google Cloud Run

Requires a Google Cloud billing account (a card on file); the free tier covers demo traffic, but Cloud Run cannot be used without billing enabled. The container itself is built and started in CI on every push, so the image is verified even when the service is not hosted.

Runs the server as a container on Cloud Run, using Cloud Shell in the browser (nothing to install locally), with secrets in Secret Manager.

## Constraints that shape the deployment

- **Single instance only.** The ledger engine holds balances in memory and persists an event log. Two instances would each hold their own state and could both approve spending the same funds. Always deploy with `--max-instances 1`. Horizontal scaling is described in the README under "Scaling beyond a single node".
- **Cold starts replay the event log.** With scale-to-zero, the first request after idle loads all events from Postgres. Startup time grows with history.
- **TLS is terminated by Cloud Run.** The container serves plaintext HTTP/2 on `$PORT`; `TLS_CERT_FILE`/`TLS_KEY_FILE` are not set.
- **`/stats` is disabled** when `PORT` is set (Cloud Run exposes one port and `/stats` is unauthenticated). Set `STATS_ADDR` to re-enable it on a different address.
- **Region.** Pick a region close to your database; every durable acknowledgment is a database round trip.

## Configuration

| Variable | Source | Purpose |
| --- | --- | --- |
| `PORT` | set by Cloud Run | gRPC listen port |
| `DATABASE_URL` | Secret Manager | Postgres connection string (use the IPv4 session pooler) |
| `API_KEY` | Secret Manager | required `x-api-key` header on every RPC |

## Steps (Cloud Shell)

1. Create a Google Cloud project, attach a billing account, and add a budget alert (Billing → Budgets & alerts).
2. Open Cloud Shell (the terminal icon in the console) and set the project:
   ```bash
   gcloud config set project YOUR_PROJECT_ID
   gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com
   ```
3. Fetch the code:
   ```bash
   git clone https://github.com/Kunal-svg-cyber/Aethel-Ledger.git
   cd Aethel-Ledger
   ```
4. Store the secrets. The first prompt hides your input:
   ```bash
   read -rsp "DATABASE_URL: " DBURL; echo
   printf '%s' "$DBURL" | gcloud secrets create DATABASE_URL --data-file=-
   unset DBURL
   API_KEY=$(openssl rand -hex 24)
   printf '%s' "$API_KEY" | gcloud secrets create API_KEY --data-file=-
   echo "API key (save it now): $API_KEY"
   ```
5. Let the service read the secrets:
   ```bash
   PROJECT_NUMBER=$(gcloud projects describe $(gcloud config get-value project) --format='value(projectNumber)')
   gcloud projects add-iam-policy-binding $(gcloud config get-value project) \
     --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
     --role="roles/secretmanager.secretAccessor"
   ```
6. Deploy:
   ```bash
   gcloud run deploy aethel-ledger \
     --source . \
     --region asia-northeast3 \
     --use-http2 \
     --allow-unauthenticated \
     --min-instances 0 \
     --max-instances 1 \
     --memory 512Mi \
     --set-secrets DATABASE_URL=DATABASE_URL:latest,API_KEY=API_KEY:latest
   ```
   `--allow-unauthenticated` only opens the network path; every RPC, including reflection, still requires the API key.
7. Smoke test with grpcurl:
   ```bash
   curl -sSL https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_x86_64.tar.gz | tar -xz grpcurl
   HOST=$(gcloud run services describe aethel-ledger --region asia-northeast3 --format='value(status.url)' | sed 's#https://##')
   ./grpcurl $HOST:443 list                                   # expect Unauthenticated
   ./grpcurl -H "x-api-key: $API_KEY" $HOST:443 list          # expect the service list
   ./grpcurl -H "x-api-key: $API_KEY" -d '{"account_id":"cloud-alice","amount":1000}' $HOST:443 ledger.v1.LedgerService/Deposit
   ./grpcurl -H "x-api-key: $API_KEY" -d '{"account_id":"cloud-alice"}' $HOST:443 ledger.v1.LedgerService/GetBalance
   ```
8. Verify durability across a new instance by forcing a new revision, then reading the balance again:
   ```bash
   gcloud run services update aethel-ledger --region asia-northeast3 --update-env-vars REDEPLOY=$(date +%s)
   ./grpcurl -H "x-api-key: $API_KEY" -d '{"account_id":"cloud-alice"}' $HOST:443 ledger.v1.LedgerService/GetBalance
   ```
9. Read logs:
   ```bash
   gcloud run services logs read aethel-ledger --region asia-northeast3 --limit 30
   ```

## Troubleshooting

- **Build fails with a permission error:** grant the compute service account `roles/cloudbuild.builds.builder`, `roles/artifactregistry.writer` and `roles/storage.objectViewer`, then redeploy.
- **Container failed to start:** check the logs. A bad `DATABASE_URL` shows as `failed to connect to Postgres`. Use the session pooler string, percent-encode special characters in the password.
- **Unauthenticated on every call:** the `x-api-key` header must match the stored secret exactly.

## Clean up

```bash
gcloud run services delete aethel-ledger --region asia-northeast3
gcloud secrets delete DATABASE_URL
gcloud secrets delete API_KEY
```
