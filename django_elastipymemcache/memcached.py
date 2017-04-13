"""
Backend for django cache
"""
import logging
import socket
from functools import wraps

from django.core.cache import InvalidCacheBackendError
from django.core.cache.backends.memcached import BaseMemcachedCache

from . import client as pyMemcache_client
from .cluster_utils import get_cluster_info


logger = logging.getLogger(__name__)


def invalidate_cache_after_error(f):
    """
    catch any exception and invalidate internal cache with list of nodes
    """
    @wraps(f)
    def wrapper(self, *args, **kwds):
        try:
            return f(self, *args, **kwds)
        except Exception:
            self.clear_cluster_nodes_cache()
            raise
    return wrapper


class ElastiPyMemCache(BaseMemcachedCache):
    """
    backend for Amazon ElastiCache (memcached) with auto discovery mode
    it used pyMemcache
    """
    def __init__(self, server, params):
        super(ElastiPyMemCache, self).__init__(
            server,
            params,
            library=pyMemcache_client,
            value_not_found_exception=ValueError)
        if len(self._servers) > 1:
            raise InvalidCacheBackendError(
                'ElastiCache should be configured with only one server '
                '(Configuration Endpoint)')
        if len(self._servers[0].split(':')) != 2:
            raise InvalidCacheBackendError(
                'Server configuration should be in format IP:port')

        # Patch for django<1.11
        self._options = self._options or dict()
        self._cluster_timeout = self._options.get(
                'cluster_timeout', socket._GLOBAL_DEFAULT_TIMEOUT)

    def clear_cluster_nodes_cache(self):
        """clear internal cache with list of nodes in cluster"""
        if hasattr(self, '_client'):
            del self._client

    def get_cluster_nodes(self):
        """
        return list with all nodes in cluster
        """
        server, port = self._servers[0].split(':')
        try:
            return get_cluster_info(
                server,
                port,
                self._cluster_timeout
            )['nodes']
        except (OSError, socket.gaierror, socket.timeout) as err:
            logger.debug(
                'Cannot connect to cluster %s, err: %s',
                self._servers[0],
                err
            )
            return []

    @property
    def _cache(self):

        if getattr(self, '_client', None) is None:

            options = self._options
            options.setdefault('ignore_exc', True)
            options.pop('cluster_timeout', None)

            self._client = self._lib.Client(
                self.get_cluster_nodes(), **options)

        return self._client

    @invalidate_cache_after_error
    def get(self, *args, **kwargs):
        return super(ElastiPyMemCache, self).get(*args, **kwargs)

    @invalidate_cache_after_error
    def get_many(self, *args, **kwargs):
        return super(ElastiPyMemCache, self).get_many(*args, **kwargs)

    @invalidate_cache_after_error
    def set(self, *args, **kwargs):
        return super(ElastiPyMemCache, self).set(*args, **kwargs)

    @invalidate_cache_after_error
    def set_many(self, *args, **kwargs):
        return super(ElastiPyMemCache, self).set_many(*args, **kwargs)

    @invalidate_cache_after_error
    def delete(self, *args, **kwargs):
        return super(ElastiPyMemCache, self).delete(*args, **kwargs)
