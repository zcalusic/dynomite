#!/usr/bin/env python3
"""Seeds given by name on a two-node cluster (one DC, two racks, so every
write crosses the peer link): a node starts while its seed's name doesn't
resolve yet, connects once it does, and follows the name to another
address. The name is made to resolve, and moved, through /etc/hosts, so
this needs to run as root, as in the test containers; elsewhere it's
skipped. test/run_tests.sh runs it after the cluster runs."""
from contextlib import ExitStack
import json
import os
import sys
import time
from urllib.request import urlopen

import yaml
from plumbum import local

from dyno_cluster import DynoCluster, DynoSpec
from dyno_node import DynoNode
from utils import generate_ips, setup_temp_dir

HOSTS = '/etc/hosts'
# .invalid never resolves through DNS (RFC 6761), so only /etc/hosts does
NAME = 'dynomite-seed-test.invalid'
REQUEST = dict(
    cluster_desc=[dict(name='dc1', racks=[['rack1', 1], ['rack2', 1]])],
    conf=dict())

def wait_for(what, cond, timeout):
    """Polls cond() until it is true; fails after timeout seconds."""
    deadline = time.time() + timeout
    while True:
        if cond():
            return
        assert time.time() < deadline, "timed out waiting for " + what
        time.sleep(0.2)

def stats_up(node):
    try:
        url = 'http://{}:{}/info'.format(node.ip, node.spec.stats_port)
        json.loads(urlopen(url).read().decode('ascii'))
        return True
    except OSError:
        return False

def log_text(node):
    with open(node.logfile, 'rb') as fh:
        return fh.read().decode('utf-8', 'replace')

def set_hosts(original, ip):
    """/etc/hosts as it was, plus NAME at ip unless ip is None. Written in
    place: in a container the file is a bind mount, which can't be
    replaced."""
    with open(HOSTS, 'w') as fh:
        fh.write(original)
        if ip is not None:
            fh.write('{} {}\n'.format(ip, NAME))

def replicates(n1, n2):
    """Writes a new key through n1 and checks it reached n2's datastore.
    Until n1 is connected to n2, the write isn't replicated."""
    key = 'seed_test_{}'.format(int(time.time() * 1000))
    try:
        n1.get_connection().set(key, 'value')
    except Exception:
        return False
    deadline = time.time() + 1
    while time.time() < deadline:
        if n2.get_data_store_connection().get(key) == b'value':
            return True
        time.sleep(0.1)
    return False

def spec_at(spec, ip):
    """The same node at another address."""
    return DynoSpec(ip, spec.dnode_port, spec.client_port, spec.rack, spec.dc,
                    spec.token, spec.local_connections,
                    spec.remote_connections, spec.req_conf)

def main():
    if not os.access(HOSTS, os.W_OK):
        print("Seed resolution tests skipped: {} isn't writable".format(HOSTS))
        return 0
    with open(HOSTS) as fh:
        original = fh.read()

    temp = setup_temp_dir()
    ips = generate_ips()
    request_file = str(temp / 'seed_request.yaml')
    with open(request_file, 'w') as fh:
        yaml.dump(REQUEST, fh)
    spec1, spec2 = DynoCluster(request_file, ips).specs
    by_name = '{}:{}:{}:{}:{}'.format(NAME, spec2.dnode_port, spec2.rack,
                                      spec2.dc, spec2.token)
    n1 = DynoNode(spec1, [by_name])
    n2 = DynoNode(spec2, [spec1.seed_string])

    with ExitStack() as stack:
        stack.callback(set_hosts, original, None)
        stack.enter_context(local.cwd(temp))
        set_hosts(original, None)

        print("Checking a node whose seed doesn't resolve yet")
        stack.enter_context(n1)
        stack.enter_context(n2)
        for node in (n1, n2):
            wait_for('the stats port of ' + node.ip, lambda: stats_up(node),
                     10)
        assert "doesn't resolve yet" in log_text(n1)
        assert not replicates(n1, n2)
        print("\t-Started, and writes don't reach the seed")

        print("Checking the seed once its name resolves")
        set_hosts(original, spec2.ip)
        # the reconnect backoff is up to 10 s
        wait_for('replication', lambda: replicates(n1, n2), 30)
        print("\t-Connected and replicating")

        print("Checking the seed at another address")
        n2.teardown()
        moved = DynoNode(spec_at(spec2, next(ips)), [spec1.seed_string])
        set_hosts(original, moved.ip)
        stack.enter_context(moved)
        wait_for('the stats port of ' + moved.ip, lambda: stats_up(moved), 10)
        wait_for('replication', lambda: replicates(n1, moved), 30)
        assert "resolves to {}:".format(moved.ip) in log_text(n1)
        print("\t-Followed the name to {}".format(moved.ip))

    print("All seed resolution tests ran fine")
    return 0

if __name__ == '__main__':
    sys.exit(main())
