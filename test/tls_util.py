#!/usr/bin/env python3
"""Certificates for the peer TLS tests, made with the openssl command line:
a CA and node certificates signed by it, Ed25519, written under a directory.
Nothing here is committed; every test run makes its own."""
import os
from plumbum import local

openssl = local['openssl']

def make_ca(directory, name, days=2):
    """Makes a self-signed CA; returns the paths of its key and cert."""
    key = os.path.join(directory, name + '.key')
    cert = os.path.join(directory, name + '.pem')
    openssl('req', '-x509', '-newkey', 'ed25519', '-nodes', '-keyout', key,
            '-out', cert, '-days', str(days), '-subj', '/CN=' + name)
    return key, cert

def make_node_cert(directory, name, ip, ca_key, ca_cert, days=1):
    """Makes a node certificate signed by the CA, with the node's IP as a
    SAN and both the serverAuth and clientAuth key usages; returns the
    paths of the key and the cert."""
    key = os.path.join(directory, name + '.key')
    csr = os.path.join(directory, name + '.csr')
    ext = os.path.join(directory, name + '.ext')
    cert = os.path.join(directory, name + '.pem')
    openssl('req', '-newkey', 'ed25519', '-nodes', '-keyout', key, '-out', csr,
            '-subj', '/CN=' + name)
    with open(ext, 'w') as fh:
        fh.write('subjectAltName=IP:{}\n'.format(ip))
        fh.write('extendedKeyUsage=serverAuth,clientAuth\n')
    openssl('x509', '-req', '-in', csr, '-CA', ca_cert, '-CAkey', ca_key,
            '-CAcreateserial', '-out', cert, '-days', str(days), '-extfile', ext)
    return key, cert

def make_cluster_certs(directory, ips):
    """A CA and one certificate per node IP; returns the CA cert path and a
    dict ip -> (key, cert)."""
    os.makedirs(directory, exist_ok=True)
    ca_key, ca_cert = make_ca(directory, 'ca')
    certs = {}
    for ip in ips:
        certs[ip] = make_node_cert(directory, 'node-' + ip, ip, ca_key, ca_cert)
    return ca_cert, certs
