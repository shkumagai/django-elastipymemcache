from pymemcache.client.hash import HashClient
from pymemcache.serde import (
    python_memcache_deserializer,
    python_memcache_serializer,
)


class Client(HashClient):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault('serializer', python_memcache_serializer)
        kwargs.setdefault('deserializer', python_memcache_deserializer)

        super(Client, self).__init__(
            *args,
            **kwargs
        )

    def get_many(self, keys, gets=False, *args, **kwargs):
        # pymemcache's HashClient may returns {'key': False}
        end = super().get_many(keys, gets, *args, **kwargs)

        return {key: end.get(key) for key in end if end.get(key)}

    get_multi = get_many

    def disconnect_all(self):
        for client in self.clients.values():
            client.close()
