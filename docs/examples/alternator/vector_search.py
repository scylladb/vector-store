"""Minimal vector search example using ScyllaDB Alternator (DynamoDB API).

Stores embeddings as lists of numbers and finds the most similar one with
DynamoDB's SearchVectors operation. Mirrors ../quick-start.cql and requires
ScyllaDB 2026.4 or later.
"""

import time
from decimal import Decimal

import boto3


def main() -> None:
    # Alternator runs with authorization disabled in the example docker
    # compose stack, so any credentials work; boto3 still requires some.
    resource = boto3.resource(
        "dynamodb",
        endpoint_url="http://127.0.0.1:8000",
        region_name="us-east-1",
        aws_access_key_id="alternator",
        aws_secret_access_key="alternator",
    )
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

    # The DynamoDB resource API represents numbers as Decimal.
    first_vector = [Decimal(str(x)) for x in (0.12, 0.34, 0.56, 0.78, 0.91)]
    second_vector = [Decimal(str(x)) for x in (0.11, 0.35, 0.55, 0.77, 0.92)]
    table.put_item(
        Item={
            "id": "1",
            "comment": "I like vector search!",
            "comment_vector": first_vector,
        }
    )
    table.put_item(
        Item={
            "id": "2",
            "comment": "ScyllaDB is great!",
            "comment_vector": second_vector,
        }
    )

    # Build the vector index. Projecting `comment` into the index lets the
    # search return it directly, without reading the item from the table.
    # Once the build reaches ACTIVE, every row that existed when it started is
    # indexed; rows inserted afterwards are picked up asynchronously (eventual
    # consistency). We insert first and wait for ACTIVE so the search below
    # deterministically sees all the data.
    client.update_table(
        TableName="comments",
        VectorIndexUpdates=[
            {
                "Create": {
                    "IndexName": "comment_ann_index",
                    "VectorAttribute": {"AttributeName": "comment_vector"},
                    "Dimensions": 5,
                    "DistanceFunction": "COSINE",
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": ["comment"],
                    },
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

    # Find the nearest comment. With COSINE, Score is the cosine distance, so
    # lower means more similar.
    response = client.search_vectors(
        TableName="comments",
        IndexName="comment_ann_index",
        SearchVector=first_vector,
        TopK=1,
    )
    nearest = response["SearchResults"][0]
    print(f"{nearest['Item']['comment']} (cosine distance: {nearest['Score']})")


if __name__ == "__main__":
    main()
