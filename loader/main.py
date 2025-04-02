from flask import Flask, request
from google.cloud import bigquery
import os

app = Flask(__name__)

# Get dataset and table IDs from environment variables
DATASET_ID = os.environ['DATASET_ID']
TABLE_ID = os.environ['TABLE_ID']


@app.route('/', methods=['POST'])
def load_to_bigquery():
    # Parse the Eventarc event data
    event = request.get_json()
    if 'data' not in event:
        return 'Invalid event data', 400

    bucket = event['data']['bucket']
    object_name = event['data']['name']

    # Process only CSV files
    if not object_name.endswith('.csv'):
        return 'Not a CSV file', 200

    # Set up BigQuery client and table reference
    client = bigquery.Client()
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)

    # Define the schema matching the CSV structure
    schema = [
        bigquery.SchemaField("collection_id", "STRING"),
        bigquery.SchemaField("collection_label", "STRING"),
        bigquery.SchemaField("entity_id", "STRING"),
        bigquery.SchemaField("entity_name", "STRING"),
        bigquery.SchemaField("entity_slug", "STRING"),
        bigquery.SchemaField("entity_url", "STRING"),
        bigquery.SchemaField("entity_type", "STRING"),
        bigquery.SchemaField("partner_names", "STRING"),
        bigquery.SchemaField("difficulty_level", "STRING"),
        bigquery.SchemaField("course_count", "INTEGER"),
        bigquery.SchemaField("is_part_of_coursera_plus", "BOOLEAN"),
        bigquery.SchemaField("is_cost_free", "BOOLEAN"),
    ]

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,  # Skip the header row
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing table
    )

    # Load the file from GCS into BigQuery
    uri = f"gs://{bucket}/{object_name}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    return 'Loaded successfully', 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)