import redis
import requests
import random
import socket
import string
import time
from utils import string_generator, number_generator
from dual_run import dual_run

def create_key(test_name, key_id):
    return test_name + "_" + str(key_id)

def run_key_value_tests(c, max_keys=1000, max_payload=1024):
    #Set some
    test_name="KEY_VALUE"
    print("Running %s tests" % test_name)
    for x in range(0, max_keys):
        key = create_key(test_name, x)
        c.run_verify("set", key, string_generator(size=random.randint(1, max_payload)))
    # get them and see
    for x in range(0, max_keys):
        key = create_key(test_name, x)
        c.run_verify("get", key)
    # append a key
    key = create_key(test_name, random.randint(0, max_keys-1))
    value = string_generator()
    c.run_verify("append", key, value)
    c.run_verify("get", key)
    # expire a few
    key = create_key(test_name, random.randint(0, max_keys-1))
    c.run_verify("expire", key, 5)
    time.sleep(7)
    c.run_verify("exists", key)

def run_multikey_test(c, max_keys=1000, max_payload=10):
    #Set some
    test_name="MULTIKEY"
    print("Running %s tests" % test_name)
    for n in range(0, 100):
        kv_pairs = {}
        len = random.randint(1, 50)
        for x in range(0, len):
            key_id = random.randint(0, max_keys-1)
            key = create_key(test_name, key_id)
            value = string_generator(size=random.randint(1, max_payload))
            kv_pairs[key] = value
        c.run_verify("mset", kv_pairs)
        keys = []
        len = random.randint(1, 50)
        for x in range(0, len):
            key_id = random.randint(0, max_keys-1)
            key = create_key(test_name, key_id)
            keys.append(key)
        c.run_verify("mget", keys)

def run_large_multikey_test(c, max_keys=100, max_payload=40000):
    # Values larger than an mbuf (16K) and many values per request make the
    # bulks, and their "$<len>" headers, span mbufs when the request is
    # fragmented.
    test_name="LARGE_MULTIKEY"
    print("Running %s tests" % test_name)
    chars = string.ascii_letters + string.digits
    for n in range(0, 20):
        kv_pairs = {}
        for x in range(0, random.randint(1, 30)):
            key = create_key(test_name, random.randint(0, max_keys-1))
            size = random.randint(1, max_payload)
            kv_pairs[key] = ''.join(random.choices(chars, k=size))
        c.run_verify("mset", kv_pairs)
        c.run_verify("mget", list(kv_pairs.keys()))
    for n in range(0, 20):
        kv_pairs = {}
        for x in range(0, random.randint(100, 1000)):
            key = create_key(test_name, random.randint(0, max_keys*10-1))
            kv_pairs[key] = string_generator(size=random.randint(1, 100))
        c.run_verify("mset", kv_pairs)
        c.run_verify("mget", list(kv_pairs.keys()))

def run_script_tests(c):
    TEST_NAME="SCRIPTS"
    print("Running %s tests" % TEST_NAME)

    # This script basically executes 'GET <key>'.
    SCRIPT_BODY='{}'.format("return redis.call('get', KEYS[1])")
    EXPECTED_VALUE = "value1"

    # Load a simple script.
    script_hash = c.run_verify("script_load", SCRIPT_BODY)

    # Make sure that the script exists.
    assert c.run_verify("script_exists", script_hash)[0] == True

    # Create a key to test with.
    key = create_key(TEST_NAME, "key1")
    c.run_verify("set", key, EXPECTED_VALUE)

    # Verify that the result of the script is the same in both Dynomite and Redis using
    # EVALSHA.
    evalsha_result = c.run_verify("evalsha", script_hash, 1, key)

    # Decode from UTF-8 before comparing the result.
    assert str(evalsha_result, 'utf-8') == EXPECTED_VALUE

    # Flush the Redis script cache through Dynomite.
    c.run_dynomite_only("script_flush")

    # Verify that the script no longer exists.
    assert c.run_dynomite_only("evalsha", script_hash, 1, key) == None


def run_hash_tests(c, max_keys=10, max_fields=1000):
    def create_key_field(keyid=None, fieldid=None):
        if keyid is None:
            keyid = random.randint(0, max_keys - 1)
        if fieldid is None:
            fieldid = random.randint(0, max_fields- 1)
        key = create_key(test_name, keyid)
        field = create_key("_field", fieldid)
        return (key, key + field)

    test_name="HASH_MAP"
    print("Running %s tests" % test_name)

    #hset
    for key_iter in range(0, max_keys):
        for field_iter in range(0, max_fields):
            key, field = create_key_field(key_iter, field_iter)
            value = number_generator()
            c.run_verify("hset", key, field, value)

    # hmset
    keyid = random.randint(0, max_keys-1)
    key, _ = create_key_field(keyid)
    kv_pairs = {}
    for x in range(0, 50):
        _, field = create_key_field(keyid)
        value = number_generator()
        kv_pairs[field] = value
    c.run_verify("hmset", key, kv_pairs)

    run_multi_field_hset_tests(c, create_key(test_name, "_multi"))

    # hmget
    keyid = random.randint(0, max_keys-1)
    key, _ = create_key_field(keyid)
    list_args = [key]
    for x in range(0, 5):
        _, field = create_key_field(keyid)
        list_args.append(field)
    args = tuple(list_args)
    c.run_verify("hmget", *args)

    # hincrby, hdel, hexists
    key, field = create_key_field()
    c.run_verify("hincrby", key, field, 50)
    c.run_verify("hdel", key, field)
    c.run_verify("hexists", key, field)
    key, _ = create_key_field()
    c.run_verify("hlen", key)

    # hgetall, hkeys, hvals
    # We may get differently ordered results from both Redis and Dynomite, so instruct
    # client to sort all results before comparing.
    c.set_sort_before_compare(True)
    key, _ = create_key_field()
    c.run_verify("hgetall", key)
    key, _ = create_key_field()
    c.run_verify("hkeys", key)
    key, _ = create_key_field()
    c.run_verify("hvals", key)

    # TODO: Still have ordering issues with HSCAN. Find another way to test.
    # finally do a hscan
    #key, _ = create_key_field()
    #next_index = 0;
    #while True:
        #result = c.run_verify("hscan", key, next_index)
        #next_index = result[0]
        #print next_index
        #if next_index == 0:
            #break

def run_multi_field_hset_tests(c, key):
    # HSET takes several fields since Redis 4.0. With read repairs it is
    # rewritten as a script like HMSET: its reply must still be the number of
    # fields added.
    print("\t-Running HSET with several fields on key '%s'" % key)
    c.run_verify("execute_command", "HSET", key, "f1", "v1")
    c.run_verify("execute_command", "HSET", key, "f1", "v2", "f2", "v2", "f3", "v3")
    c.run_verify("execute_command", "HSET", key, "f2", "x", "f3", "x")
    c.run_verify("execute_command", "HMSET", key, "f3", "y", "f4", "y")
    c.run_verify("execute_command", "HMSET", key, "f5", "z")
    c.set_sort_before_compare(True)
    c.run_verify("hgetall", key)
    c.set_sort_before_compare(False)

def run_zadd_tests(c, key):
    # ZADD is rewritten as a script with read repairs: its options, several
    # members, and its reply must come out as Redis' own.
    print("\t-Running ZADD with options on key '%s'" % key)
    c.run_verify("execute_command", "ZADD", key, 1, "a", 2, "b")
    c.run_verify("execute_command", "ZADD", key, 5, "a", 3, "c")
    c.run_verify("execute_command", "ZADD", key, "NX", 9, "a", 4, "d")
    c.run_verify("execute_command", "ZADD", key, "xx", "ch", 6, "a", 7, "e")
    c.run_verify("execute_command", "ZADD", key, "CH", 0, "b", 7, "c", 8, "f")
    c.run_verify("execute_command", "ZADD", key, "INCR", 2, "a")
    c.run_verify("execute_command", "ZADD", key, "NX", "INCR", 2, "a")
    c.run_verify("execute_command", "ZADD", key, "XX", "GT", "CH", "INCR", 1, "c")
    c.run_verify("execute_command", "ZADD", key, "GT", "CH", 1, "a", 9, "b")
    c.run_verify("execute_command", "ZADD", key, "LT", 1, "f", 1, "g")
    c.run_verify("zrange", key, 0, -1, False, True)

def run_read_repair_test(c, num_keys=10, min_size=1, max_size=1024):
    # Enable read repairs (TODO)

    test_name="READ_REPAIR_TEST"
    print("Running %s tests, values of %d to %d bytes" %
          (test_name, min_size, max_size))

    dyno_cluster = c.get_dynomite_cluster()

    # We need a connection to a DC that has RF > 1, since we're going to be using quorum.
    print("\t-Ensuring we have a connection to a multi-rack DC")
    c.ensure_underlying_dyno_conn_is_multi_dc()

    print("\t-Enabling read repairs")
    # Enable read repairs in the cluster
    dyno_cluster.enable_read_repairs()

    print("\t-Enabling DC_SAFE_QUORUM")
    # Make the consistency level of the cluster as "DC_SAFE_QUORUM"
    # as read repairs are disabled on "DC_ONE"
    # TODO: Remove when no longer true
    dyno_cluster.set_cluster_consistency_level("DC_SAFE_QUORUM")

    # Set a key
    key = create_key(test_name, "_%d" % max_size)
    value = string_generator(size=random.randint(min_size, max_size))

    print("\t-Writing to key '%s'" % key)
    c.run_verify("set", key, value)

    # 'get' and verify that the value is the same.
    assert value == str(c.run_verify("get", key), 'utf-8')

    run_zadd_tests(c, create_key(test_name, "_zadd_%d" % max_size))
    run_multi_field_hset_tests(c, create_key(test_name, "_hset_%d" % max_size))

    print("\t-Simulating partial update to key '%s'" % key)
    # Below, we simulate a partial write by updating the value of 'key' on only
    # one replica, and updating its timestamp on that replica.
    # After simulating such a write, doing a quorum read will eventually fix the
    # key on all replicas.
    ADD_SET_MD_KEY = "._add-set"
    REPAIRED_VALUE = "REPAIRED_VALUE" + string_generator(
        size=random.randint(min_size, max_size))

    # Find a DC with multiple racks (i.e. RF > 1)
    dc_name, racks = dyno_cluster.get_multi_rack_dc()
    num_racks = len(racks)

    # Pick any rack
    rack = next(iter(racks.keys()))

    # Find the exact node(shard) in the rack that has 'key'
    node_with_key = dyno_cluster.find_node_with_key(dc_name, rack, key)
    # Get the connection to the Redis process on that node.
    redis_conn = node_with_key.get_data_store_connection()
    assert redis_conn.exists(key) == True
    assert redis_conn.delete(key) == 1
    assert redis_conn.exists(key) == False

    # Make sure that we have the dynomite reserved metadata key
    assert redis_conn.exists(ADD_SET_MD_KEY) == True

    # Get the TS of our 'key' and update it by 1.
    add_ts = redis_conn.zscore(ADD_SET_MD_KEY, key) + 1.0
    # Update the TS of that key to make it appear that this node has
    # the latest value.
    # Note: Redis-py unfortunately has backward compatibility issues, so we change the API
    # based on the version.
    # https://github.com/andymccurdy/redis-py/issues/1068#issuecomment-439175760
    if redis.VERSION[0] < 3:
        redis_conn.zadd(ADD_SET_MD_KEY, add_ts, key)
    else:
        redis_conn.zadd(ADD_SET_MD_KEY, {key: add_ts})
    score = redis_conn.zscore(ADD_SET_MD_KEY, key)
    assert score == add_ts , score

    # Update with a value that we want to repair with
    assert redis_conn.set(key, REPAIRED_VALUE) == True

    test_success = False
    # Attempt to retrieve the up to 20 times and confirm that we read the
    # repaired value.
    # We retry multiple times since the quorum can be achieved with the majority replicas
    # that have the older data, but we'll eventually conflict with the replica with the
    # latest data causing the repair on all nodes.
    for i in range(0, 20):
        final_val = str(c.run_dynomite_only("get", key), 'utf-8')
        if final_val == REPAIRED_VALUE:
            test_success = True
            break

    assert test_success == True
    print("\t-Confirmed read repair took place successfully")
    print("\t-Disabling read repairs")
    dyno_cluster.disable_read_repairs()


def send_peer_request(node, key, flags):
    """Sends 'SET key value' to node's peer port as a peer in the same DC
    would, with the given dnode header flags. Returns what came back before
    the node closed the connection or a 2 s timeout."""
    payload = '*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$5\r\nvalue\r\n'.format(
        len(key), key).encode()
    header = '   $2014$ 4242 3 {} 1 1 *1 d *{}\r\n'.format(
        flags, len(payload)).encode()
    received = b''
    with socket.create_connection((node.ip, node.dnode_port), timeout=2) as s:
        s.sendall(header + payload)
        try:
            while True:
                data = s.recv(4096)
                if not data:
                    break
                received += data
        except socket.timeout:
            pass
        except ConnectionResetError:
            pass
    return received

def run_encrypted_peer_msg_test(c):
    print("Running ENCRYPTED_PEER_MSG test")
    cluster = c.get_dynomite_cluster()
    node = cluster.nodes[0]
    store = node.get_data_store_connection()
    # Built-in peer encryption was removed: a message with the encryption bit
    # gets the connection closed, without a reply or a write.
    key = create_key('ENCRYPTED_PEER_MSG', 'encrypted')
    received = send_peer_request(node, key, 1)
    assert received == b'', received
    assert not store.exists(key)
    # The same message without the bit is executed, so the test above checks
    # the bit and not a malformed message.
    key = create_key('ENCRYPTED_PEER_MSG', 'plain')
    received = send_peer_request(node, key, 0)
    if cluster.peer_tls:
        # With peer TLS, a plaintext message is closed before it is read.
        assert received == b'', received
        assert not store.exists(key)
        print("\t-Both plaintext peer messages rejected with peer TLS")
        return
    assert received.endswith(b'+OK\r\n'), received
    assert store.get(key) == b'value'
    print("\t-Encrypted peer message rejected, plaintext one accepted")

def run_incr_replicas_test(c, incrs_per_dc=3):
    """INCRs a key through one node in each DC, then checks every node's
    datastore: each rack holds one replica, and every replica was incremented
    exactly once per INCR, under the cluster's consistency level."""
    print("Running INCR_REPLICAS test")
    dyno_cluster = c.get_dynomite_cluster()
    key = create_key('INCR_REPLICAS', 'counter')
    coordinators = {}
    for node in dyno_cluster.nodes:
        coordinators.setdefault(node.spec.dc, node)
    for node in coordinators.values():
        conn = node.get_connection()
        for _ in range(incrs_per_dc):
            conn.incr(key)
    expected = incrs_per_dc * len(coordinators)
    num_racks = sum(len(racks) for racks in dyno_cluster.counts_by_rack.values())
    # Replication to other DCs is asynchronous: give it a few seconds.
    for _ in range(50):
        values = [node.get_data_store_connection().get(key)
                  for node in dyno_cluster.nodes]
        replicas = [int(v) for v in values if v is not None]
        if len(replicas) == num_racks and all(v == expected for v in replicas):
            break
        time.sleep(0.1)
    assert len(replicas) == num_racks, (num_racks, values)
    assert all(v == expected for v in replicas), (expected, values)
    print("\t-%d replicas, each incremented %d times" % (num_racks, expected))

def run_rest_api_test(c):
    """Checks the stats REST API's HTTP statuses: unknown paths, bad
    arguments, other methods and malformed requests get errors, not OK."""
    print("Running REST_API test")
    nodes = c.get_dynomite_cluster().nodes
    node = nodes[0]
    base = 'http://{}:{}'.format(node.ip, node.spec.stats_port)
    expected = {
        '/info': 200, '/ping': 200, '/help': 200, '/get_consistency': 200,
        '/state/get_state': 200, '/peer/up/' + nodes[1].ip: 200,
        '/bogus': 404, '/state': 404, '/stateX': 404, '/state/bogus': 404,
        '/setloglevelX': 404, '/peer/bogus': 404, '/peer/update': 404,
        '/peer/up/10.255.255.1': 404, '/read_repairs': 404,
        '/setloglevel': 400, '/setloglevel/abc': 400, '/setloglevel/12': 400,
        '/set_timeout_factor/0': 400, '/set_consistency/write': 400,
        '/set_consistency/read/bogus': 400,
        '/set_consistency/read/dc_one_bogus': 400, '/peer/up': 400,
    }
    for path, status in expected.items():
        r = requests.get(base + path)
        assert r.status_code == status, (path, r.status_code, r.text)
    r = requests.post(base + '/info')
    assert r.status_code == 405 and r.headers['Allow'] == 'GET', r.headers
    # A malformed request line gets one 400 reply and nothing else.
    with socket.create_connection((node.ip, node.spec.stats_port),
                                  timeout=2) as s:
        s.sendall(b'HELLO\r\n\r\n')
        received = b''
        while True:
            data = s.recv(4096)
            if not data:
                break
            received += data
    assert received.startswith(b'HTTP/1.1 400 '), received
    assert received.endswith(b'\r\n\r\nBad Request\r\n'), received
    print("\t-%d paths, POST and a malformed request answered as expected"
          % len(expected))

def comparison_test(redis, dynomite, debug):
    c = dual_run(redis, dynomite, debug)
    run_key_value_tests(c)

    # XLarge payloads
    run_key_value_tests(c, max_keys=10, max_payload=5*1024*1024)

    run_multikey_test(c)
    run_large_multikey_test(c)
    run_hash_tests(c, max_keys=10, max_fields=100)
    run_script_tests(c)
    run_encrypted_peer_msg_test(c)
    run_incr_replicas_test(c)
    run_rest_api_test(c)

    # Run read repairs tests last since we change the state of the cluster to use
    # DC_SAFE_QUORUM
    run_read_repair_test(c)
    # Values larger than an mbuf (16 KB), in the requests, the replies and the
    # repairs
    run_read_repair_test(c, min_size=64*1024, max_size=100*1024)
    print("All test ran fine")
