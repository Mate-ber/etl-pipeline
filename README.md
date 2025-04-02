# Coursera ETL Pipeline with Google Cloud

## Overview
This repository contains an ETL pipeline that extracts course data from Coursera's API, stores the extracted data in Google Cloud Storage (GCS), and loads it into BigQuery for further analysis. The pipeline is designed to be fully automated and serverless, leveraging Google Cloud services such as Cloud Run, Cloud Scheduler, and Cloud Functions.

## Architecture
The pipeline consists of three main components:

1. **Extractor (Cloud Run)**: Fetches course data from Coursera's API and stores it as a CSV file in GCS.
2. **Loader (Cloud Run)**: Reads the CSV file from GCS and loads it into BigQuery.
3. **Cloud Scheduler**: Triggers the Extractor service at scheduled intervals.

### Workflow
1. Cloud Scheduler triggers the Extractor service.
2. The Extractor fetches course data from Coursera's API and writes it to GCS.
3. The Loader picks up the CSV file from GCS and inserts the data into BigQuery.
4. The data is available for querying in BigQuery.

## Google Cloud Services Used
- **Cloud Run**: Runs the Extractor and Loader services in a serverless environment.
- **Cloud Scheduler**: Automates the execution of the Extractor.
- **Cloud Storage**: Stores the extracted data as CSV files.
- **BigQuery**: Stores and processes the extracted course data.

## Prerequisites
Before running the pipeline, ensure you have the following:

- A Google Cloud project with billing enabled.
- Cloud SDK installed and authenticated (`gcloud auth login`).
- Required IAM roles assigned:
  - Extractor: `Storage Object Creator`
  - Loader: `Storage Object Viewer`, `BigQuery Data Editor`

## Deployment

### Step 1: Clone the Repository
```sh
git clone https://github.com/yourusername/coursera-etl.git
cd coursera-etl
```

### Step 2: Set Up Environment Variables
Modify the `.env` file and set the required environment variables:
```env
COURSE_API_URL="https://api.coursera.org/api/courses.v1"
GCS_BUCKET_NAME="your-gcs-bucket"
BIGQUERY_DATASET="your_dataset"
BIGQUERY_TABLE="your_table"
```

### Step 3: Deploy the Extractor
```sh
gcloud run deploy coursera-extractor \
    --image gcr.io/YOUR_PROJECT_ID/coursera-extractor \
    --region YOUR_REGION \
    --allow-unauthenticated
```

### Step 4: Deploy the Loader
```sh
gcloud run deploy coursera-loader \
    --image gcr.io/YOUR_PROJECT_ID/coursera-loader \
    --region YOUR_REGION \
    --allow-unauthenticated
```

### Step 5: Schedule the Extractor
```sh
gcloud scheduler jobs create http coursera-extract-job \
    --schedule="0 2 * * *" \
    --uri=https://YOUR_CLOUD_RUN_URL \
    --http-method=GET
```

## Usage
Once deployed, the pipeline runs automatically as per the schedule set in Cloud Scheduler. You can also trigger it manually by making an HTTP request to the Extractor service.

### Querying Data in BigQuery
After the pipeline runs, you can query the data using:
```sql
SELECT * FROM `your_project_id.your_dataset.your_table` LIMIT 10;
```
