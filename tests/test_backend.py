from unittest.mock import Mock, patch

import django
import pytest
from django.core.cache import InvalidCacheBackendError

from django_elastipymemcache.client import ConfigurationEndpointClient


def test_multiple_servers():
    from django_elastipymemcache.backend import ElastiPymemcache
    with pytest.raises(InvalidCacheBackendError):
        ElastiPymemcache('h1:0,h2:0', {})


def test_wrong_server_format():
    from django_elastipymemcache.backend import ElastiPymemcache
    with pytest.raises(InvalidCacheBackendError):
        ElastiPymemcache('h', {})


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_split_servers(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache
    backend = ElastiPymemcache('h:0', {})
    servers = [('h1', 0), ('h2', 0)]
    get_cluster_info.return_value = {
        'nodes': servers
    }
    with patch.object(backend._lib, 'Client'):
        assert backend._cache
        get_cluster_info.assert_called()
        backend._lib.Client.assert_called_once_with(
            servers,
            ignore_exc=True,
        )


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_node_info_cache(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache
    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    with patch.object(backend._lib, 'Client'):
        backend.set('key1', 'val')
        backend.get('key1')
        backend.set('key2', 'val')
        backend.get('key2')
        backend._lib.Client.assert_called_once_with(
            servers,
            ignore_exc=True,
        )
        assert backend._cache.get.call_count == 2
        assert backend._cache.set.call_count == 2

    get_cluster_info.assert_called_once()


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_failed_to_connect_servers(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache
    backend = ElastiPymemcache('h:0', {})
    get_cluster_info.side_effect = OSError()
    assert backend.get_cluster_nodes() == []


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_invalidate_cache(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache
    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    with patch.object(backend._lib, 'Client'):
        assert backend._cache

        with patch.object(backend._cache, 'get', Mock()):
            backend._cache.get.side_effect = Exception()
            try:
                backend.get('key1', 'val')
            except Exception:
                pass
            #  invalidate cached client
            container = getattr(backend, '_local', backend)
            container._client = None
            try:
                backend.get('key1', 'val')
            except Exception:
                pass
            assert backend._cache.get.call_count == 2

    assert get_cluster_info.call_count == 3


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_add(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.add('key1', 'value1')
    assert ret is False


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_delete(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.delete('key1')
    if django.get_version() >= '3.1':
        assert ret is False
    else:
        assert ret is None


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_get_many(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.get_many(['key1'])
    assert ret == {}

    # When server does not found...
    with patch('pymemcache.client.hash.HashClient._get_client') as p:
        p.return_value = None
        ret = backend.get_many(['key2'])
        assert ret == {}

    with patch('pymemcache.client.hash.HashClient._safely_run_func') as p2:
        p2.return_value = {
            ':1:key3': 1509111630.048594
        }

        ret = backend.get_many(['key3'])
        assert ret == {'key3': 1509111630.048594}

    # If False value is included, ignore it.
    with patch('pymemcache.client.hash.HashClient.get_many') as p:
        p.return_value = {
            ':1:key1': 1509111630.048594,
            ':1:key2': False,
            ':1:key3': 1509111630.058594,
        }
        ret = backend.get_many(['key1', 'key2', 'key3'])
        assert \
            ret == \
            {
                'key1': 1509111630.048594,
                'key3': 1509111630.058594
            }

    with patch('pymemcache.client.hash.HashClient.get_many') as p:
        p.return_value = {
            ':1:key1': None,
            ':1:key2': 1509111630.048594,
            ':1:key3': False,
        }
        ret = backend.get_many(['key1', 'key2', 'key3'])
        assert \
            ret == \
            {
                'key2': 1509111630.048594,
            }


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_set_many(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.set_many({'key1': 'value1', 'key2': 'value2'})
    assert ret == ['key1', 'key2']


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_delete_many(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.delete_many(['key1', 'key2'])
    assert ret is None


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_incr(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.incr('key1', 1)
    assert ret is False


@patch.object(ConfigurationEndpointClient, 'get_cluster_info')
def test_client_decr(get_cluster_info):
    from django_elastipymemcache.backend import ElastiPymemcache

    servers = ['h1:0', 'h2:0']
    get_cluster_info.return_value = {
        'nodes': servers
    }

    backend = ElastiPymemcache('h:0', {})
    ret = backend.decr('key1', 1)
    assert ret is False
