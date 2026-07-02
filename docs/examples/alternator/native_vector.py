"""Minimal vector search example using ScyllaDB Alternator (DynamoDB API).

Stores embeddings with the native Vector type, which is more compact and
enables a configurable similarity function and similarity scores. Mirrors
../quick-start.cql and requires ScyllaDB 2026.3.0-dev or later.
"""

import time

from alternator import AlternatorConfig, AlternatorResource
from alternator.vector import Vector


def main():
    config = AlternatorConfig(seed_hosts=["127.0.0.1"], port=8000)
    with AlternatorResource(config) as resource:
        client = resource.meta.client
        table = resource.Table("comments")

        # Start from a clean table so the example can be re-run.
        try:
            table.delete()
            table.wait_until_not_exists()
        except client.exceptions.ResourceNotFoundException:
            pass

        resource.create_table(
            TableName="comments",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        # The native Vector type accepts plain floats directly.
        table.put_item(
            Item={
                "id": "1",
                "comment": "I like vector search!",
                "comment_vector": Vector([0.12, 0.34, 0.56, 0.78, 0.91]),
            }
        )
        table.put_item(
            Item={
                "id": "2",
                "comment": "ScyllaDB is great!",
                "comment_vector": Vector([0.11, 0.35, 0.55, 0.77, 0.92]),
            }
        )

        # Build the vector index, choosing the similarity function used to
        # compare vectors. Once the build reaches ACTIVE, every row that existed
        # when it started is indexed; rows inserted afterwards are picked up
        # asynchronously (eventual consistency). We insert first and wait for
        # ACTIVE so the query below deterministically sees all the data.
        client.update_table(
            TableName="comments",
            VectorIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "comment_ann_index",
                        "VectorAttribute": {
                            "AttributeName": "comment_vector",
                            "Dimensions": 5,
                        },
                        "SimilarityFunction": "COSINE",
                    }
                }
            ],
        )

        # Wait for the vector index to finish building.
        while True:
            indexes = client.describe_table(TableName="comments")["Table"].get(
                "VectorIndexes", []
            )
            if indexes and indexes[0]["IndexStatus"] == "ACTIVE":
                break
            time.sleep(1)

        # Query for the nearest comment and its similarity score. The index
        # projects only the key, so read the full item to get its text.
        response = table.query(
            IndexName="comment_ann_index",
            VectorSearch={
                "QueryVector": Vector([0.12, 0.34, 0.56, 0.78, 0.91]),
                "ReturnScores": "SIMILARITY",
            },
            Limit=1,
        )
        nearest_id = response["Items"][0]["id"]
        score = response["Scores"][0]
        item = table.get_item(Key={"id": nearest_id})["Item"]
        print(f"{item['comment']} (similarity: {score})")


if __name__ == "__main__":
    main()
