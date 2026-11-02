#!/usr/bin/env python3
"""A raw client for memcached's text protocol.

The tests compare replies byte for byte, so this client sends exactly the
bytes it's given and returns exactly what came back, reading until the
expected number of complete replies has arrived.
"""
import socket

# The first line of a reply that ends it. Retrieval replies (VALUE ...)
# end with END; numbers answer incr and decr.
_FINAL = (b'STORED', b'NOT_STORED', b'EXISTS', b'NOT_FOUND', b'DELETED',
          b'TOUCHED', b'OK', b'END', b'ERROR', b'CLIENT_ERROR',
          b'SERVER_ERROR', b'VERSION')

CLOSED = b'<closed>'


class McConnection(object):
    def __init__(self, ip, port, timeout=10):
        self.addr = (ip, port)
        self.timeout = timeout
        self.sock = None
        self.buf = b''
        self.connect()

    def connect(self):
        self.close()
        self.sock = socket.create_connection(self.addr, timeout=self.timeout)
        self.buf = b''

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def _fill(self):
        """Reads more bytes into the buffer; False once the peer closed."""
        data = self.sock.recv(1 << 16)
        if not data:
            return False
        self.buf += data
        return True

    def _line(self):
        while b'\r\n' not in self.buf:
            if not self._fill():
                return None
        line, self.buf = self.buf.split(b'\r\n', 1)
        return line + b'\r\n'

    def _block(self, n):
        while len(self.buf) < n:
            if not self._fill():
                return None
        block, self.buf = self.buf[:n], self.buf[n:]
        return block

    def _reply(self):
        """One complete reply, or what came of it before the peer closed,
        ending in CLOSED."""
        out = b''
        while True:
            line = self._line()
            if line is None:
                return out + self.buf + CLOSED
            out += line
            if line.startswith(b'VALUE '):
                size = int(line.split()[3])
                block = self._block(size + 2)
                if block is None:
                    return out + self.buf + CLOSED
                out += block
                continue
            if line.split(b' ', 1)[0].rstrip(b'\r\n') in _FINAL or \
                    line.rstrip(b'\r\n').isdigit():
                return out

    def request(self, data, replies=1):
        """Sends data and returns the next 'replies' replies, concatenated.
        A closed connection shows as CLOSED at the end; the next request
        reconnects."""
        if self.sock is None:
            self.connect()
        try:
            self.sock.sendall(data)
            out = b''
            for _ in range(replies):
                r = self._reply()
                out += r
                if r.endswith(CLOSED):
                    self.close()
                    break
            return out
        except (BrokenPipeError, ConnectionResetError):
            self.close()
            return CLOSED

    def closed_after(self, data, wait=2):
        """Sends data and returns everything until the peer closes the
        connection, or None if it stays open for 'wait' seconds."""
        if self.sock is None:
            self.connect()
        self.sock.sendall(data)
        self.sock.settimeout(wait)
        out = self.buf
        try:
            while True:
                data = self.sock.recv(1 << 16)
                if not data:
                    self.close()
                    return out
                out += data
        except socket.timeout:
            self.sock.settimeout(self.timeout)
            self.buf = out
            return None
