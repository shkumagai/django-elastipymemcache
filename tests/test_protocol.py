import sys

from django_elastipymemcache.cluster_utils import (
    WrongProtocolData,
    get_cluster_info,
)
from nose.tools import (
    eq_,
    raises,
)

if sys.version < '3':
    from mock import patch, call, MagicMock
else:
    from unittest.mock import patch, call, MagicMock


# https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.AddingToYourClientLibrary.html
EXAMPLE_RESPONSE = (
    b'CONFIG cluster 0 147\r\n'
    b'12\n'
    b'myCluster.pc4ldq.0001.use1.cache.amazonaws.com|10.82.235.120|11211 '
    b'myCluster.pc4ldq.0002.use1.cache.amazonaws.com|10.80.249.27|11211\n\r\n'
    b'END\r\n'
)


@patch('django_elastipymemcache.cluster_utils.Telnet')
def test_get_cluster_info(Telnet):
    client = Telnet.return_value
    client.read_until.side_effect = [
        b'VERSION 1.4.14',
    ]
    client.expect.side_effect = [
        (0, None, EXAMPLE_RESPONSE),  # NOQA
    ]
    info = get_cluster_info('', 0)
    eq_(info['version'], 12)
    eq_(info['nodes'], [('10.82.235.120', 11211), ('10.80.249.27', 11211)])
    client.write.assert_has_calls([
        call(b'version\n'),
        call(b'config get cluster\n'),
    ])


@patch('django_elastipymemcache.cluster_utils.Telnet')
def test_get_cluster_info_before_1_4_13(Telnet):
    client = Telnet.return_value
    client.read_until.side_effect = [
        b'VERSION 1.4.13',
    ]
    client.expect.side_effect = [
        (0, None, EXAMPLE_RESPONSE),  # NOQA
    ]
    info = get_cluster_info('', 0)
    eq_(info['version'], 12)
    eq_(info['nodes'], [('10.82.235.120', 11211), ('10.80.249.27', 11211)])
    client.write.assert_has_calls([
        call(b'version\n'),
        call(b'get AmazonElastiCache:cluster\n'),
    ])


@raises(WrongProtocolData)
@patch('django_elastipymemcache.cluster_utils.Telnet', MagicMock())
def test_bad_protocol():
    get_cluster_info('', 0)


@patch('django_elastipymemcache.cluster_utils.Telnet')
def test_ubuntu_protocol(Telnet):
    client = Telnet.return_value
    client.read_until.side_effect = [
        b'VERSION 1.4.14 (Ubuntu)',
    ]
    client.expect.side_effect = [
        (0, None, EXAMPLE_RESPONSE),  # NOQA
    ]
    get_cluster_info('', 0)
    client.write.assert_has_calls([
        call(b'version\n'),
        call(b'config get cluster\n'),
    ])


@raises(WrongProtocolData)
@patch('django_elastipymemcache.cluster_utils.Telnet')
def test_no_configuration_protocol_support_with_errors(Telnet):
    client = Telnet.return_value
    client.read_until.side_effect = [
        b'VERSION 1.4.34',
    ]
    client.expect.side_effect = [
        (0, None, b'ERROR\r\n'),
    ]
    get_cluster_info('test', 0)


@raises(WrongProtocolData)
@patch('django_elastipymemcache.cluster_utils.Telnet')
def test_cannot_parse_version(Telnet):
    client = Telnet.return_value
    client.read_until.side_effect = [
        b'VERSION 1.4.34',
    ]
    client.expect.side_effect = [
        (0, None, b'CONFIG cluster 0 138\r\nfail\nhost|ip|11211 host||11211\n\r\nEND\r\n'),  # NOQA
    ]
    get_cluster_info('test', 0)


@raises(WrongProtocolData)
@patch('django_elastipymemcache.cluster_utils.Telnet')
def test_cannot_parse_nodes(Telnet):
    client = Telnet.return_value
    client.read_until.side_effect = [
        b'VERSION 1.4.34',
    ]
    client.expect.side_effect = [
        (0, None, b'CONFIG cluster 0 138\r\n1\nfail\n\r\nEND\r\n'),  # NOQA
    ]
    get_cluster_info('test', 0)
