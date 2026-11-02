#!/usr/bin/env python3
from plumbum import BG

import datastore
from mc_client import McConnection
from node import Node


class MemcachedNode(Node):
    def __init__(self, ip, port):
        super(MemcachedNode, self).__init__(ip, port)
        self.name = "Memcached" + self.name
        self.logfile = 'logs/memcached_{}.log'.format(self.ip)
        self.proc_future = None

    def get_connection(self):
        return McConnection(self.ip, self.port)

    def is_up(self):
        try:
            c = self.get_connection()
        except OSError:
            return False
        try:
            return c.request(b'version\r\n').startswith(b'VERSION ')
        finally:
            c.close()

    def get_pid(self):
        return self.proc_future.proc.pid

    def launch(self):
        # -U 0: no UDP; -u root: memcached refuses to run as root without it
        # (the test containers run as root); -vv: commands and replies in
        # the log, which memcached writes to stderr.
        self.proc_future = \
            (datastore.server_bin()['-l', self.ip, '-p', self.port, '-U', '0',
                                    '-u', 'root', '-vv'] >= self.logfile) & BG(-9)

    def teardown(self):
        self.proc_future.proc.kill()
        self.proc_future.wait()

    def __enter__(self):
        self.launch()

    def __exit__(self, type_, value, traceback):
        self.teardown()
