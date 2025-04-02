from flask import Flask
import requests
import json
import csv
from google.cloud import storage
from datetime import datetime
import os

app = Flask(__name__)

# Get GCS bucket name from environment variable
GCS_BUCKET_NAME = os.environ['GCS_BUCKET_NAME']


@app.route('/', methods=['POST'])
def extract_and_upload():
    # Define the GraphQL endpoint and headers (reused from provided code)
    url = "https://www.coursera.org/graphql-gateway?opname=Search"
    headers = {
        "authority": "www.coursera.org",
        "method": "POST",
        "path": "/graphql-gateway?opname=Search",
        "scheme": "https",
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en",
        "Apollographql-Client-Name": "search-v2",
        "Apollographql-Client-Version": "480bd0d7bf858837154f13ee3e31b0bbb217c35",
        "Content-Type": "application/json",
        "Cookie": "CSRF3-Token=1743682356.vbKluxtoNi5AS7E9; __204u=YOUR_AUTH_COOKIE",
        "x-csrf3-token": "1743682356.vbKluxtoNi5AS7E9",
        "Operation-Name": "Search",
        "Origin": "https://www.coursera.org",
        "Referer": "https://www.coursera.org/search?query=Data%20Science",
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A_Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "macOS",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }
    payload = [{
        "operationName": "DiscoveryCollections",
        "variables": {
            "contextType": "PAGE",
            "contextId": "search-zero-state",
            "passThroughParameters": [{"name": "query", "value": "Data science"}]
        },
        "query": """
        query DiscoveryCollections($contextType: String!, $contextId: String!, $passThroughParameters: [DiscoveryCollections_PassThroughParameter!]) {
          DiscoveryCollections {
            queryCollections(
              input: {contextType: $contextType, contextId: $contextId, passThroughParameters: $passThroughParameters}
            ) {
              id
              label
              linkedCollectionPageMetadata {
                url
                __typename
              }
              entities {
                id
                slug
                name
                url
                partnerIds
                imageUrl
                partners {
                  id
                  name
                  logo
                  __typename
                }
                ... on DiscoveryCollections_specialization {
                  courseCount
                  difficultyLevel
                  isPartOfCourseraPlus
                  __typename
                }
                ... on DiscoveryCollections_course {
                  difficultyLevel
                  isPartOfCourseraPlus
                  isCostFree
                  __typename
                }
                ... on DiscoveryCollections_professionalCertificate {
                  difficultyLevel
                  isPartOfCourseraPlus
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
        }
        """
    }]

    # Send the POST request
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        data = response.json()
        if isinstance(data, list) and len(data) > 0:
            data = data[0]  # Unwrap the list if it’s a batch response
        if "errors" in data:
            return f"API Errors: {json.dumps(data['errors'])}", 500
        elif "data" not in data:
            return "Error: 'data' key not found in response.", 500
        else:
            collections = data["data"]["DiscoveryCollections"]["queryCollections"]
            csv_data = []
            for collection in collections:
                for entity in collection.get("entities", []):
                    partners = entity.get("partners", [])
                    row = {
                        "collection_id": collection["id"],
                        "collection_label": collection.get("label", "N/A"),
                        "entity_id": entity["id"],
                        "entity_name": entity["name"],
                        "entity_slug": entity["slug"],
                        "entity_url": entity["url"],
                        "entity_type": entity["__typename"],
                        "partner_names": ", ".join([p["name"] for p in partners]),
                        "difficulty_level": entity.get("difficultyLevel", "N/A"),
                        "course_count": entity.get("courseCount", "N/A") if entity[
                                                                                "__typename"] == "DiscoveryCollections_specialization" else "N/A",
                        "is_part_of_coursera_plus": str(entity.get("isPartOfCourseraPlus", "N/A")),
                        "is_cost_free": str(entity.get("isCostFree", "N/A")) if entity[
                                                                                    "__typename"] == "DiscoveryCollections_course" else "N/A",
                    }
                    csv_data.append(row)

            # Generate a unique timestamp for the file name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file_path = f"/tmp/coursera_search_results_{timestamp}.csv"
            csv_headers = [
                "collection_id", "collection_label", "entity_id", "entity_name", "entity_slug",
                "entity_url", "entity_type", "partner_names", "difficulty_level", "course_count",
                "is_part_of_coursera_plus", "is_cost_free"
            ]

            # Write data to CSV
            with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
                writer.writeheader()
                writer.writerows(csv_data)

            # Upload to GCS
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET_NAME)
            destination_csv = f"coursera_search_results_{timestamp}.csv"
            blob = bucket.blob(destination_csv)
            blob.upload_from_filename(csv_file_path)

            # Clean up temporary file
            os.remove(csv_file_path)
            return 'Success', 200
    else:
        return f"Failed to fetch data. Status code: {response.status_code}", 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)