#!/usr/bin/env python3
"""Peer TLS checks on a two-node cluster (one DC, two racks, so every write
crosses the peer link): what the peer port accepts and rejects, the
counters, the handshake timeout, and the ways a node can be misconfigured.
test/run_tests.sh runs it after the cluster runs; certificates are made on
the fly with openssl."""
from contextlib import ExitStack
import json
import os
import socket
import sys
import time
from urllib.request import urlopen

import yaml
from plumbum import local

from dyno_cluster import DynoCluster
from tls_util import make_ca, make_node_cert
from utils import generate_ips, setup_temp_dir, wait_for

openssl = local['openssl']

HANDSHAKE_TIMEOUT = 10  # TLS_HANDSHAKE_TIMEOUT_MS in src/dyn_tls.h
REQUEST = dict(
    cluster_desc=[dict(name='dc1', racks=[['rack1', 1], ['rack2', 1]])],
    conf=dict())

def info(node):
    url = 'http://{}:{}/info'.format(node.ip, node.spec.stats_port)
    return json.loads(urlopen(url).read().decode('ascii'))

def counter(node, name):
    """A pool counter, or a header field, from /info."""
    data = info(node)
    if name in data:
        return data[name]
    return data['dyn_o_mite'][name]

def log_size(node):
    return os.path.getsize(node.logfile)

def log_since(node, offset):
    with open(node.logfile, 'rb') as fh:
        fh.seek(offset)
        return fh.read().decode('utf-8', 'replace')

def s_client(node, ca, cert=None, key=None):
    """Connects to the node's peer port with openssl s_client, sends a line
    and hangs up; returns the output. Whether the node accepted the client
    is checked on the node: s_client's own verdict is about the node's cert,
    and the node's rejection may arrive after s_client has gone."""
    args = ['s_client', '-connect', '{}:{}'.format(node.ip, node.dnode_port),
            '-CAfile', ca]
    if cert is not None:
        args += ['-cert', cert, '-key', key]
    rc, out, err = (openssl[args] << 'x\r\n').run(retcode=None, timeout=20)
    return out + err

def send_plaintext(node):
    """A plaintext dnode SET to the peer port; returns what came back before
    the node closed the connection."""
    msg = (b'   $2014$ 4242 3 0 1 1 *1 d *31\r\n'
           b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
    with socket.create_connection((node.ip, node.dnode_port), timeout=10) as s:
        s.sendall(msg)
        received = b''
        try:
            while True:
                data = s.recv(4096)
                if not data:
                    break
                received += data
        except ConnectionResetError:
            pass
    return received

def stalled_handshake(node):
    """Connects and sends nothing; returns how long the node took to close."""
    start = time.time()
    with socket.create_connection((node.ip, node.dnode_port), timeout=10) as s:
        s.settimeout(HANDSHAKE_TIMEOUT + 10)
        data = s.recv(16)
    assert data == b'', data
    return time.time() - start

def stats_up(node):
    try:
        info(node)
        return True
    except OSError:
        return False

def restart(nodes, tls_files):
    """Restarts the nodes, the second one with the given TLS files (None for
    plaintext); the first keeps its own. Both restart so that the first
    one's reconnect backoff starts over. Returns once their stats ports
    answer."""
    for node in nodes:
        node.teardown()
    nodes[1].spec.tls_files = tls_files
    for node in nodes:
        node.launch()
    for node in nodes:
        wait_for('the stats port of ' + node.ip, lambda: stats_up(node), 10)

def wait_connected(n1, n2, handshakes_before):
    """Waits for the two nodes to shake hands and replicate a write."""
    wait_for('handshakes', lambda: counter(n1, 'peer_tls_handshakes') >
             handshakes_before, 20)
    # The outgoing connection of a node started first was refused and waits
    # out its reconnect backoff (up to 10 s); a write before it is up isn't
    # replicated.
    wait_for('outgoing peer connections',
             lambda: counter(n1, 'peer_connections') >= 1 and
             counter(n2, 'peer_connections') >= 1, 30)
    key = 'tls_test_{}'.format(int(time.time() * 1000))
    n1.get_connection().set(key, 'value')
    wait_for('replication', lambda: n2.get_data_store_connection().get(key)
             == b'value', 10)

def main():
    temp = setup_temp_dir()
    ips = generate_ips()
    request_file = str(temp / 'tls_request.yaml')
    with open(request_file, 'w') as fh:
        yaml.dump(REQUEST, fh)
    cluster = DynoCluster(request_file, ips, peer_tls=True)

    with ExitStack() as stack:
        stack.enter_context(local.cwd(temp))
        stack.enter_context(cluster)
        cluster.wait_until_ready()
        n1, n2 = cluster.nodes
        good = dict(n2.spec.tls_files)
        ca = good['ca']

        print("Checking replication over TLS and the counters")
        wait_connected(n1, n2, 0)
        for node in (n1, n2):
            assert counter(node, 'peer_tls_handshakes') >= 1
            assert counter(node, 'peer_tls_handshake_errors') == 0
            assert counter(node, 'peer_tls_plaintext_rejected') == 0
            assert counter(node, 'peer_tls_cert_days_left') >= 0
        print("\t-Writes replicate, handshakes counted, no errors")

        print("Checking the peer port with openssl s_client")
        handshakes = counter(n1, 'peer_tls_handshakes')
        errors = counter(n1, 'peer_tls_handshake_errors')
        offset = log_size(n1)
        out = s_client(n1, ca, good['cert'], good['key'])
        assert 'Verify return code: 0 (ok)' in out and 'TLSv1.3' in out, out
        wait_for('handshake counter', lambda: counter(n1, 'peer_tls_handshakes')
                 == handshakes + 1, 5)
        out = s_client(n1, ca)
        wait_for('a client without a cert rejected',
                 lambda: 'peer did not return a certificate' in
                 log_since(n1, offset), 5)
        tls_dir = os.path.abspath('tls')
        other_ca_key, other_ca = make_ca(tls_dir, 'other-ca')
        other_key, other_cert = make_node_cert(tls_dir, 'other-node', n2.ip,
                                               other_ca_key, other_ca)
        out = s_client(n1, ca, other_cert, other_key)
        wait_for('a cert from another CA rejected',
                 lambda: 'certificate verify failed' in log_since(n1, offset),
                 5)
        wait_for('handshake error counter',
                 lambda: counter(n1, 'peer_tls_handshake_errors') == errors + 2,
                 5)
        assert counter(n1, 'peer_tls_handshakes') == handshakes + 1
        print("\t-Accepted with a node cert, refused without one and with "
              "one from another CA")

        print("Checking a plaintext peer and a stalled handshake")
        errors = counter(n1, 'peer_tls_handshake_errors')
        assert send_plaintext(n1) == b''
        assert not n1.get_data_store_connection().exists('key')
        wait_for('plaintext counter',
                 lambda: counter(n1, 'peer_tls_plaintext_rejected') == 1, 5)
        took = stalled_handshake(n1)
        assert HANDSHAKE_TIMEOUT - 1 <= took <= HANDSHAKE_TIMEOUT + 5, took
        wait_for('handshake error counter',
                 lambda: counter(n1, 'peer_tls_handshake_errors') == errors + 1,
                 5)
        print("\t-Plaintext closed and counted, stalled handshake closed "
              "after %.1f s" % took)

        print("Checking a node whose key doesn't match its cert")
        n2.teardown()
        n2.spec.tls_files = dict(good, key=n1.spec.tls_files['key'])
        offset = log_size(n2) if os.path.exists(n2.logfile) else 0
        n2.launch()
        rc = n2.proc_future.proc.wait(timeout=10)
        assert rc == 1, rc
        assert 'key values mismatch' in log_since(n2, offset)
        n2.data_store_node.teardown()
        print("\t-Refused to start with exit status 1")

        print("Checking a node with a cert from another CA")
        handshakes = counter(n1, 'peer_tls_handshakes')
        offset1 = log_size(n1)
        n1.teardown()
        n2.spec.tls_files = dict(good, cert=other_cert, key=other_key)
        offset2 = log_size(n2)
        n1.launch()
        n2.launch()
        wait_for('the stats port', lambda: stats_up(n1), 10)
        wait_for('self-check warning',
                 lambda: 'does not verify with the CAs' in log_since(n2, offset2),
                 10)
        wait_for('rejection by the peer',
                 lambda: 'certificate verify failed' in log_since(n1, offset1),
                 20)
        wait_for('handshake error counter',
                 lambda: counter(n1, 'peer_tls_handshake_errors') >= 1, 5)
        assert counter(n1, 'peer_tls_handshakes') == 0
        print("\t-Warned at startup, rejected by its peer")

        print("Checking a TLS node facing a plaintext node")
        offset1 = log_size(n1)
        restart((n1, n2), None)
        wait_for('the hint',
                 lambda: 'no TLS reply: is peer_tls on there?' in
                 log_since(n1, offset1), HANDSHAKE_TIMEOUT + 20)
        assert counter(n1, 'peer_tls_handshakes') == 0
        print("\t-Handshake timed out with the hint")

        print("Checking the plaintext node switched to TLS")
        restart((n1, n2), good)
        wait_connected(n1, n2, 0)
        print("\t-Connected and replicating again")

    print("All peer TLS tests ran fine")

if __name__ == '__main__':
    sys.exit(main())
