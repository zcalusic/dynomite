#!/usr/bin/env python3
"""The datastore the functional tests run against.

$DYN_TEST_DATASTORE picks the protocol: "redis" (the default, for Valkey and
Redis) or "memcache". $DYN_TEST_SERVER picks the server binary; without it,
the first of test/_binaries/redis-server, valkey-server and redis-server for
redis, memcached for memcache.

Run as a script, prints the datastore and the server's version.
"""
import os

from plumbum import local

KIND = os.environ.get('DYN_TEST_DATASTORE', 'redis')
assert KIND in ('redis', 'memcache'), \
    'DYN_TEST_DATASTORE must be redis or memcache, not ' + KIND
MEMCACHE = KIND == 'memcache'

# dynomite's data_store setting
DATA_STORE = 1 if MEMCACHE else 0


def server_bin():
    """The datastore server binary, as a plumbum command."""
    if os.environ.get('DYN_TEST_SERVER'):
        return local.get(os.environ['DYN_TEST_SERVER'])
    if MEMCACHE:
        return local.get('memcached')
    return local.get('./test/_binaries/redis-server', 'valkey-server',
                     'redis-server')


def server_version():
    if MEMCACHE:
        return server_bin()('-V').strip()
    return server_bin()('--version').strip()


if __name__ == '__main__':
    print('Datastore: {}, server: {} ({})'.format(KIND, server_bin(),
                                                  server_version()))
