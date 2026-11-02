#!/usr/bin/env python3
"""Functional tests for memcache: the same raw requests to a standalone
memcached and through the Dynomite cluster, with the replies compared byte
for byte."""
import random
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


def run_noreply_test(c):
    """noreply on every command that takes it: no reply, the write done,
    the next request answered."""
    print('Running memcache noreply test')
    for req in [b'set nr 0 0 1 noreply\r\na\r\n',
                b'add nr2 0 0 1 noreply\r\nb\r\n',
                b'replace nr 0 0 1 noreply\r\nc\r\n',
                b'append nr 0 0 1 noreply\r\nd\r\n',
                b'prepend nr 0 0 1 noreply\r\ne\r\n',
                b'set nrn 0 0 1\r\n5\r\nincr nrn 3 noreply\r\n',
                b'decr nrn 1 noreply\r\n',
                b'touch nr 100 noreply\r\n',
                b'delete nr2 noreply\r\n']:
        c.run_verify(req + b'get nr nr2 nrn\r\n',
                     replies=2 if req.startswith(b'set nrn') else 1)
    # A pipeline of them, then a reply.
    c.run_verify(b''.join(b'set nrp%d 0 0 1 noreply\r\n%d\r\n' % (i, i % 10)
                          for i in range(50)) +
                 b'get ' + b' '.join(b'nrp%d' % i for i in range(50)) + b'\r\n')


def run_key_length_test(c):
    """Keys up to memcached's 250 bytes work; longer ones get Dynomite's
    own error, and the connection goes on."""
    print('Running memcache key length test')
    k250 = b'k' * 250
    c.run_verify(b'set ' + k250 + b' 0 0 1\r\na\r\n')
    c.run_verify(b'get ' + k250 + b'\r\n')
    c.run_verify(b'get a ' + k250 + b' b\r\n')
    too_long = (b'CLIENT_ERROR Dynomite: Key is too long, the limit is 250 '
                b'bytes\r\n')
    for req in [b'set ' + b'k' * 251 + b' 0 0 1\r\na\r\n',
                b'get ' + b'k' * 251 + b'\r\n',
                b'get ' + b'k' * 1000 + b'\r\n']:
        got = c.run_dynomite_only(req)
        assert got == too_long, got
        c.run_verify(b'get ' + k250 + b'\r\n')


def run_value_test(c):
    """Values: empty, with CRLF inside, larger than an mbuf, the flags'
    range, and over memcached's item size limit."""
    print('Running memcache value test')
    for value in [b'', b'a\r\nb', b'\x00\xff' * 10, b'v' * (100 * 1024),
                  bytes(range(256)) * 2048]:
        c.run_verify(b'set val 0 0 %d\r\n' % len(value) + value + b'\r\n')
        c.run_verify(b'get val\r\n')
    c.run_verify(b'set fl 4294967295 0 1\r\na\r\n')
    c.run_verify(b'get fl\r\n')
    big = b'x' * (2 * 1024 * 1024)
    c.run_verify(b'set huge 0 0 %d\r\n' % len(big) + big + b'\r\n')
    c.run_verify(b'get huge\r\n')


def run_arithmetic_test(c):
    print('Running memcache arithmetic test')
    for req in [b'set ar 0 0 1\r\n0\r\n', b'decr ar 1\r\n',
                b'set ar 0 0 20\r\n18446744073709551615\r\n',
                b'incr ar 1\r\n', b'incr ar 18446744073709551615\r\n',
                b'set ar 0 0 3\r\nabc\r\n', b'incr ar 1\r\n',
                b'set ar 0 0 2\r\n 1\r\n', b'incr ar 1\r\n',
                b'incr nosuch 1\r\n', b'decr nosuch 1\r\n',
                b'set ar 0 0 3\r\n100\r\n', b'decr ar 1\r\n']:
        c.run_verify(req)
        c.run_verify(b'get ar\r\n')


def run_expiry_test(c):
    print('Running memcache expiry test')
    c.run_verify(b'set ex1 0 1 1\r\na\r\nset ex2 0 1 1\r\nb\r\n'
                 b'set ex3 0 100 1\r\nc\r\n', replies=3)
    c.run_verify(b'touch ex2 100\r\n')
    c.run_verify(b'touch ex3 1\r\n')
    time.sleep(2.5)
    c.run_verify(b'get ex1 ex2 ex3\r\n')


def run_hash_tag_test(c):
    """Keys with the same hash tag go to the same node."""
    print('Running memcache hash tag test')
    for i in range(10):
        c.run_verify(b'set {tag}%d 0 0 1\r\n%d\r\n' % (i, i))
    c.run_verify(b'get ' + b' '.join(b'{tag}%d' % i for i in range(10)) +
                 b'\r\n')
    holders = set()
    for node in c.dyno_cluster.nodes:
        ds = node.get_data_store_connection()
        if ds.request(b'get {tag}0 {tag}9\r\n').count(b'VALUE') == 2:
            holders.add((node.spec.dc, node.spec.rack))
        ds.close()
    racks = sum(len(r) for r in c.dyno_cluster.counts_by_rack.values())
    assert len(holders) == racks, holders


def run_pipeline_test(c):
    """Requests in one write get their replies in order."""
    print('Running memcache pipeline test')
    reqs = []
    for i in range(30):
        reqs.append(b'set pl%d 0 0 2\r\n%02d\r\n' % (i, i))
        reqs.append(b'get pl%d pl%d\r\n' % (i, (i * 7) % 30))
        reqs.append(b'incr pl%d 1\r\n' % i)
    c.run_verify(b''.join(reqs), replies=len(reqs))


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


def run_unknown_command_test(c):
    """Unknown commands get ERROR, as from memcached, and the connection
    stays open; so do the memcached commands Dynomite doesn't support."""
    print('Running memcache unknown command test')
    c.run_verify(b'set uc 0 0 2\r\nuc\r\n')
    for req in [b'foo\r\n', b'foo bar baz\r\n', b'GET uc\r\n', b'\r\n',
                b'get\r\n', b'gets\r\n', b'bogus_cmd k\r\n', b'set uc\r\n',
                b'incr uc\r\n', b'touch uc\r\n']:
        c.run_verify(req)
        c.run_verify(b'get uc\r\n')
    # Pipelined: the requests around it get their replies.
    c.run_verify(b'get uc\r\nfoo\r\nget uc\r\n', replies=3)
    for req in [b'gat 0 uc\r\n', b'stats\r\n', b'flush_all\r\n',
                b'version\r\n', b'mg uc v\r\n']:
        got = c.run_dynomite_only(req)
        assert got == b'ERROR\r\n', '{!r}: {!r}'.format(req, got)
        c.run_verify(b'get uc\r\n')


def run_syntax_test(c):
    """Requests memcached takes or answers with an error itself: a negative
    expiry time, a time after delete's key, a delta that's not a number."""
    print('Running memcache syntax test')
    c.run_verify(b'set neg 0 -1 1\r\na\r\n')
    c.run_verify(b'get neg\r\n')
    c.run_verify(b'set tn 0 0 1\r\na\r\n')
    c.run_verify(b'touch tn -1\r\n')
    c.run_verify(b'get tn\r\n')
    c.run_verify(b'set d0 0 0 1\r\na\r\n')
    c.run_verify(b'delete d0 0\r\n')
    c.run_verify(b'get d0\r\n')
    c.run_verify(b'set d1 0 0 1\r\na\r\n')
    c.run_verify(b'delete d1 5\r\n')
    c.run_verify(b'delete d1 0 noreply\r\nget d1\r\n')
    c.run_verify(b'set num 0 0 2\r\n10\r\n')
    for req in [b'incr num abc\r\n', b'decr num -1\r\n', b'incr num 1x\r\n',
                b'incr num 99999999999999999999999\r\n']:
        c.run_verify(req)
        c.run_verify(b'get num\r\n')


def run_quorum_majority_test(c, num_keys=5):
    """Under DC_SAFE_QUORUM and DC_EACH_SAFE_QUORUM, values that disagree
    written straight into the racks' memcached, the same in every DC: with
    more than half the racks of each DC holding one, a get through one node
    returns it; without, it's refused, and the connection stays usable."""
    dyno_cluster = c.dyno_cluster
    consistency = dyno_cluster.request['conf']['read_consistency']
    if consistency not in ('DC_SAFE_QUORUM', 'DC_EACH_SAFE_QUORUM'):
        return
    print('Running memcache quorum majority test')
    rng = random.Random(1)
    conn = dyno_cluster.nodes[0].get_connection()
    for majority in (True, False):
        for i in range(num_keys):
            key = b'mc_quorum_%d_%d' % (majority, i)
            for dc, racks in dyno_cluster.counts_by_rack.items():
                n = len(racks)
                quorum = n // 2 + 1
                if majority:
                    values = [b'A'] * quorum + [b'B'] * (n - quorum)
                elif n % 2 == 0:
                    values = [b'A'] * (n // 2) + [b'B'] * (n // 2)
                else:
                    values = [b'ABCDEFGHIJ'[j:j + 1] for j in range(n)]
                rng.shuffle(values)
                by_rack = dict(zip(sorted(racks), values))
                for node in dyno_cluster.nodes:
                    if node.spec.dc == dc:
                        ds = node.get_data_store_connection()
                        r = ds.request(b'set ' + key + b' 0 0 1\r\n' +
                                       by_rack[node.spec.rack] + b'\r\n')
                        assert r == b'STORED\r\n', r
                        ds.close()
            got = conn.request(b'get ' + key + b'\r\n')
            if majority:
                assert got == b'VALUE ' + key + b' 0 1\r\nA\r\nEND\r\n', \
                    (key, got)
            else:
                assert got == b'SERVER_ERROR Dynomite: Failed to achieve ' \
                    b'Quorum\r\n', (key, got)
            # The connection goes on.
            got = conn.request(b'get mc_quorum_1_0\r\n')
            assert got == b'VALUE mc_quorum_1_0 0 1\r\nA\r\nEND\r\n', \
                (key, got)
    conn.close()
    print('\t-%d keys with a majority of the racks answered with its value, '
          '%d without one refused' % (num_keys, num_keys))


CAS_REFUSED = (b'SERVER_ERROR cas is not supported with more than one rack, '
               b'since each replica numbers its items itself\r\n')


def run_cas_test(c):
    """cas works with one rack, as on memcached; with more, each replica
    has its own cas uniques, and a cas would change only the replicas
    whose unique matched: refused."""
    print('Running memcache cas test')
    racks = sum(len(r) for r in c.dyno_cluster.counts_by_rack.values())
    c.run_verify(b'set cs 0 0 1\r\na\r\n')
    c.run_verify(b'gets cs\r\n', cas=False)
    if racks == 1:
        c.run_verify(b'cas nocs 0 0 1 1\r\nz\r\n')
    unique = c.run_dynomite_only(b'gets cs\r\n').split(b'\r\n')[0].split()[-1]
    got = c.run_dynomite_only(b'cas cs 0 0 1 ' + unique + b'\r\nb\r\n')
    if racks == 1:
        assert got == b'STORED\r\n', got
        return
    assert got == CAS_REFUSED, got
    # noreply: no reply, nothing written, the next request answered
    got = c.run_dynomite_only(b'cas cs 0 0 1 ' + unique +
                              b' noreply\r\nc\r\nget cs\r\n')
    assert got == b'VALUE cs 0 1\r\na\r\nEND\r\n', got
    print('\t-refused with %d racks' % racks)


def run_quit_test(c):
    """quit closes the connection without a reply."""
    print('Running memcache quit test')
    want = c.mc_conn.closed_after(b'quit\r\n')
    got = c.dyno_conn.closed_after(b'quit\r\n')
    assert want == b'', 'memcached: quit: {!r}'.format(want)
    if got != want:
        raise McMismatchError(b'quit\r\n', want, got)


def comparison_test(standalone, dynomite):
    c = McDualRun(standalone, dynomite)
    run_storage_test(c)
    run_noreply_test(c)
    run_key_length_test(c)
    run_value_test(c)
    run_arithmetic_test(c)
    run_expiry_test(c)
    run_hash_tag_test(c)
    run_pipeline_test(c)
    run_get_every_node_test(c)
    run_unknown_command_test(c)
    run_syntax_test(c)
    run_quorum_majority_test(c)
    run_cas_test(c)
    run_quit_test(c)
    print('All memcache tests ran fine')
