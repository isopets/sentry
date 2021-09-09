from typing import Optional

from django.conf import settings

from sentry.utils.redis import redis_clusters

from .base import StringIndexer, UseCase


def get_client():
    return redis_clusters.get(settings.SENTRY_METRICS_INDEXER_REDIS_CLUSTER)


class MockIndexer(StringIndexer):
    """
    Mock string indexer
    """

    def record(self, org_id: str, use_case: UseCase, string: str) -> int:
        """
        If key already exists, grab that value, otherwise record both the
        string to int and int to string relationships.
        """
        client = get_client()

        string_key = f"temp-metrics-indexer:{org_id}:1:str:{string}"
        value = client.get(string_key)
        if value is None:
            value: int = abs(hash(string)) % (10 ** 8)
            client.set(string_key, value)

            # reverse record (int to string)
            int_key = f"temp-metrics-indexer:{org_id}:1:int:{value}"
            client.set(int_key, string)

        return int(value)

    def reverse_resolve(self, org_id: str, use_case: UseCase, id: int) -> Optional[str]:
        # NOTE: Ignores ``use_case`` for simplicity.

        client = get_client()
        key = f"temp-metrics-indexer:{org_id}:1:int:{id}"

        return client.get(key)
