import qdrant_client
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from qdrant_client.http.exceptions import UnexpectedResponse

endpoint = "localhost"

qdrant_client = QdrantClient(url=endpoint)

try:
    qdrant_client.create_collection(
        collection_name="test_collection",
        vectors_config=VectorParams(size=4, distance=Distance.COSINE),
    )
except UnexpectedResponse as e:
    if e.status_code == 409:
        print("Caught conflict error!")
        # handle the error
    else:
        raise  # re-raise the error if it's not a 409nt(f"result: {result}")
