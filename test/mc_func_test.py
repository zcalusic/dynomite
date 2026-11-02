#!/usr/bin/env python3
"""Functional tests for memcache: the same raw requests to a standalone
memcached and through the Dynomite cluster, with the replies compared byte
for byte."""
import re
import time


class McMismatchError(Exception):
    def __init__(self, request, want, got):
        self.request, self.want, self.got = request, want, got

    def __str__(self):
        return ('\n\t======Result Mismatch=======\n\tQuery: {!r}'
                '\n\t===========================\n\tmemcached: {!r}'
                '\n\t===========================\n\tDyno: {!r}').format(
                    self.request[:200], self.want[:500], self.got[:500])


# The cas unique in a VALUE line: each memcached numbers its writes itself.
_CAS = re.compile(rb'^(VALUE \S+ \d+ \d+) \d+\r$', re.M)


def without_cas(reply):
    return _CAS.sub(rb'\1 <cas>\r', reply)


class McDualRun(object):
    def __init__(self, standalone, dyno_cluster):
        self.standalone = standalone
        self.dyno_cluster = dyno_cluster
        self.mc_conn = standalone.get_connection()
        self.dyno_conn = dyno_cluster.get_connection()

    def run_dynomite_only(self, data, replies=1):
        """Sends data through Dynomite. Retries once the cluster's peers
        aren't connected yet."""
        for _ in range(3):
            got = self.dyno_conn.request(data, replies)
            if b'Peer Node is not connected' not in got:
                return got
            time.sleep(1)
        return got

    def run_memcached_only(self, data, replies=1):
        return self.mc_conn.request(data, replies)

    def run_verify(self, data, replies=1, cas=True):
        """Sends data to both and checks the replies are the same; cas=False
        ignores the cas uniques of VALUE lines."""
        want = self.run_memcached_only(data, replies)
        got = self.run_dynomite_only(data, replies)
        w, g = (want, got) if cas else (without_cas(want), without_cas(got))
        if w != g:
            raise McMismatchError(data, want, got)
        return got


def run_storage_test(c):
    print('Running memcache storage tests')
    c.run_verify(b'set k1 0 0 5\r\nhello\r\n')
    c.run_verify(b'get k1\r\n')
    c.run_verify(b'set f 12345 0 3\r\nabc\r\n')
    c.run_verify(b'get f\r\n')
    c.run_verify(b'add k1 0 0 1\r\na\r\n')
    c.run_verify(b'add k2 0 0 2\r\nbb\r\n')
    c.run_verify(b'replace k1 0 0 3\r\nnew\r\n')
    c.run_verify(b'replace nokey 0 0 1\r\na\r\n')
    c.run_verify(b'append k1 0 0 2\r\n++\r\n')
    c.run_verify(b'prepend k1 0 0 2\r\n--\r\n')
    c.run_verify(b'append nokey 0 0 1\r\na\r\n')
    c.run_verify(b'get k1 k2 f nokey\r\n')
    c.run_verify(b'delete k1\r\n')
    c.run_verify(b'delete k1\r\n')
    c.run_verify(b'get k1\r\n')


def run_get_every_node_test(c):
    """get and gets through every node, with keys on every node: each
    node splits them by owner with its own view of the ring."""
    print('Running memcache get through every node test')
    keys = [b'every_%d' % i for i in range(50)]
    for k in keys:
        c.run_verify(b'set ' + k + b' 0 0 2\r\nev\r\n')
    want = c.run_memcached_only(b'get ' + b' '.join(keys) + b'\r\n')
    want_gets = without_cas(c.run_memcached_only(
        b'gets ' + b' '.join(keys) + b'\r\n'))
    for node in c.dyno_cluster.nodes:
        conn = node.get_connection()
        for k in keys[:3]:
            got = conn.request(b'get ' + k + b'\r\n')
            assert got == b'VALUE ' + k + b' 0 2\r\nev\r\nEND\r\n', \
                '{}: get {!r}: {!r}'.format(node.name, k, got)
        got = conn.request(b'get ' + b' '.join(keys) + b'\r\n')
        assert got == want, '{}: multi-key get: {!r}'.format(node.name, got[:300])
        got = without_cas(conn.request(b'gets ' + b' '.join(keys) + b'\r\n'))
        assert got == want_gets, '{}: multi-key gets: {!r}'.format(node.name, got[:300])
        conn.close()


def comparison_test(standalone, dynomite):
    c = McDualRun(standalone, dynomite)
    run_storage_test(c)
    run_get_every_node_test(c)
    print('All memcache tests ran fine')
