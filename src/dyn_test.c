/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2015 Netflix, Inc.
 */

#include <fcntl.h>
#include <getopt.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>

#include "dyn_conf.h"
#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_signal.h"

#define TEST_CONF_PATH "conf/dynomite.yml"

#define TEST_LOG_DEFAULT LOG_NOTICE
#define TEST_LOG_PATH NULL

#define TEST_MBUF_SIZE 16384
#define TEST_ALLOC_MSGS_MAX 300000

static int show_help;
static int test_conf;

static struct option long_options[] = {
    {"help", no_argument, NULL, 'h'},
    {"version", no_argument, NULL, 'V'},
    {"test-conf", no_argument, NULL, 't'},
    {"describe-stats", no_argument, NULL, 'D'},
    {"verbose", required_argument, NULL, 'v'},
    {"output", required_argument, NULL, 'o'},
    {"conf-file", required_argument, NULL, 'c'},
    {"pid-file", required_argument, NULL, 'p'},
    {NULL, 0, NULL, 0}};

static char short_options[] = "hVtDgv:o:c:s:i:a:p";

static void dn_show_usage(void) {
  log_stderr("Usage: test [-?hVdDt] [-v verbosity level] [-o output file]" CRLF
             "                  [-c conf file] [-m mbuf size] [-M max alloc "
             "messages]" CRLF "");
  log_stderr("Options:" CRLF "  -h, --help             : this help" CRLF
             "  -V, --version          : show version and exit" CRLF
             "  -t, --test-conf        : test configuration for syntax errors "
             "and exit");
  log_stderr(
      "  -v, --verbosity=N      : set logging level (default: %d, min: %d, "
      "max: %d)" CRLF
      "  -o, --output=S         : set logging file (default: %s)" CRLF
      "  -c, --conf-file=S      : set configuration file (default: %s)" CRLF "",
      TEST_LOG_DEFAULT, TEST_LOG_DEFAULT, TEST_LOG_DEFAULT,
      TEST_LOG_PATH != NULL ? TEST_LOG_PATH : "stderr", TEST_CONF_PATH);
}

struct context *ctx;

static rstatus_t test_pre_run(struct instance *nci) {
  rstatus_t status;

  status = log_init(nci->log_level, nci->log_filename);
  if (status != DN_OK) {
    return status;
  }

  status = signal_init();
  if (status != DN_OK) {
    return status;
  }

  return DN_OK;
}

static void test_set_default_options(struct instance *nci) {
  int status;

  nci->ctx = NULL;
  nci->log_level = TEST_LOG_DEFAULT;
  nci->log_filename = TEST_LOG_PATH;
  nci->conf_filename = TEST_CONF_PATH;

  status = dn_gethostname(nci->hostname, DN_MAXHOSTNAMELEN);
  if (status < 0) {
    log_warn("gethostname failed, ignored: %s", strerror(errno));
    dn_snprintf(nci->hostname, DN_MAXHOSTNAMELEN, "unknown");
  }

  nci->hostname[DN_MAXHOSTNAMELEN - 1] = '\0';
}

static rstatus_t test_get_options(int argc, char **argv, struct instance *nci) {
  int c, value;

  opterr = 0;

  for (;;) {
    c = getopt_long(argc, argv, short_options, long_options, NULL);
    if (c == -1) {
      /* no more options */
      break;
    }

    switch (c) {
      case 'h':
        show_help = 1;
        break;

      case 't':
        test_conf = 1;
        nci->log_level = 11;
        break;

      case 'v':
        value = dn_atoi(optarg, strlen(optarg));
        if (value < 0) {
          log_stderr("test: option -v requires a number");
          return DN_ERROR;
        }
        nci->log_level = value;
        break;

      case 'o':
        nci->log_filename = optarg;
        break;

      case 'c':
        nci->conf_filename = optarg;
        break;

      case '?':
        switch (optopt) {
          case 'o':
          case 'c':
          case 'p':
            log_stderr("test: option -%c requires a file name", optopt);
            break;

          case 'v':
          case 's':
          case 'i':
            log_stderr("test: option -%c requires a number", optopt);
            break;

          case 'a':
            log_stderr("test: option -%c requires a string", optopt);
            break;

          default:
            log_stderr("test: invalid option -- '%c'", optopt);
            break;
        }
        return DN_ERROR;

      default:
        log_stderr("dynomite: invalid option -- '%c'", optopt);
        return DN_ERROR;
    }
  }

  return DN_OK;
}

static void print_banner(const char *s) {
  loga("===================================================================");
  loga("    Running Test: %s", s);
  loga("===================================================================");
}

static rstatus_t init_peer(struct node *s) {
  s->idx = 0;
  s->owner = NULL;

  struct string pname = string("127.0.0.1:8102");
  string_copy(&s->endpoint.pname, pname.data, pname.len);

  struct string name = string("127.0.0.1");
  string_copy(&s->name, name.data, name.len);

  s->state = UNKNOWN;

  s->endpoint.port = (uint16_t)8102;

  struct string rack = string("rack1");
  string_copy(&s->rack, rack.data, rack.len);

  struct string dc = string("dc1");
  string_copy(&s->dc, dc.data, dc.len);

  s->is_local = false;
  // TODO-need to init tokens
  // s->tokens = cseed->tokens;

  static struct sockinfo info_buf;
  struct sockinfo *info = &info_buf;

  memset(info, 0, sizeof(*info));
  dn_resolve(&name, s->endpoint.port, info);

  s->endpoint.family = info->family;
  s->endpoint.addrlen = info->addrlen;
  s->endpoint.addr = (struct sockaddr *)&info->addr;

  s->next_retry_ms = 0ULL;
  s->failure_count = 0;

  s->processed = 0;
  s->is_secure = 0;

  log_debug(LOG_NOTICE, "Filling up server data");

  return DN_OK;
}

static void deinit_peer(struct node *s) {
  string_deinit(&s->endpoint.pname);
  string_deinit(&s->name);
  string_deinit(&s->rack);
  string_deinit(&s->dc);
  free(s);
}

/*
 * A message the parser should produce: its type and, for a request, its keys
 * (each followed by its hash tag in parentheses, when it has one that isn't
 * the whole key), or for a response, its args.
 */
struct parse_req {
  msg_type_t type;
  char keys[1024]; /* space-separated */
};

#define PARSE_DATA_MAX 8192
#define PARSE_REQS_MAX 32

/* A stream of requests or responses, and what the parser should make of it. */
struct parse_case {
  const char *name;
  data_store_t data_store;
  bool dyn_mode; /* sent by a dnode peer, each message with a dnode header */
  bool response;
  const char *hash_tag; /* the pool's hash_tag, if any */
  char data[PARSE_DATA_MAX];
  size_t len;
  struct parse_req reqs[PARSE_REQS_MAX];
  size_t nreqs;
  bool reject; /* the stream ends in a message the parser must reject */
};

/*
 * Append a message to the stream. For redis requests, words are
 * space-separated and sent as a RESP array, unless raw; memcache requests and
 * all responses are always raw, sent as they are.
 */
static void parse_append(struct parse_case *tc, const char *words, bool raw) {
  char *out = tc->data + tc->len;
  size_t room = PARSE_DATA_MAX - tc->len;
  int n = 0;

  if (tc->dyn_mode) {
    n += snprintf(out, room, "$2014$ %zu %d 0 1 1 *1 d *0\r\n", tc->nreqs + 1,
                  tc->response ? DMSG_RES : DMSG_REQ);
  }
  if (raw || tc->response || tc->data_store == DATA_MEMCACHE) {
    n += snprintf(out + n, room - (size_t)n, "%s", words);
  } else {
    char buf[PARSE_DATA_MAX], *word, *save;
    int nwords = 0;

    dn_snprintf(buf, sizeof(buf), "%s", words);
    for (const char *w = words; *w != '\0'; w++) {
      if ((w == words || w[-1] == ' ') && *w != ' ') nwords++;
    }

    n += snprintf(out + n, room - (size_t)n, "*%d\r\n", nwords);
    for (word = strtok_r(buf, " ", &save); word != NULL;
         word = strtok_r(NULL, " ", &save)) {
      n += snprintf(out + n, room - (size_t)n, "$%zu\r\n%s\r\n", strlen(word),
                    word);
    }
  }

  ASSERT(n > 0 && (size_t)n < room);
  tc->len += (size_t)n;
}

/* Append a request the parser should accept, with its type and keys. */
static void parse_add_req(struct parse_case *tc, msg_type_t type,
                          const char *keys, const char *words, bool raw) {
  ASSERT(tc->nreqs < PARSE_REQS_MAX && !tc->reject);
  parse_append(tc, words, raw);
  tc->reqs[tc->nreqs].type = type;
  dn_snprintf(tc->reqs[tc->nreqs].keys, sizeof(tc->reqs[0].keys), "%s", keys);
  tc->nreqs++;
}

static void parse_add(struct parse_case *tc, msg_type_t type, const char *keys,
                      const char *words) {
  parse_add_req(tc, type, keys, words, false);
}

/* Append a response the parser should accept, with its type and args. */
static void parse_add_rsp(struct parse_case *tc, msg_type_t type,
                          const char *args, const char *data) {
  ASSERT(tc->response);
  parse_add_req(tc, type, args, data, true);
}

/* End the stream with a message the parser must reject. */
static void parse_reject(struct parse_case *tc, const char *words) {
  parse_append(tc, words, false);
  tc->reject = true;
}

/* For the error messages: the parsers take other paths with read repairs. */
static const char *parse_mode(void) {
  return g_read_repairs_enabled ? ", read repairs" : "";
}

/* Compare a parsed message with the one expected. */
static rstatus_t parse_check(const struct parse_case *tc, size_t chunk,
                             size_t i, struct msg *msg) {
  const char *what = tc->response ? "response" : "request";
  char keys[sizeof(tc->reqs[0].keys)] = "";
  size_t len = 0;

  /*
   * The args of a response split between mbufs aren't checked: only the part
   * of an arg in its last mbuf is recorded.
   */
  bool args_unchecked = tc->response && STAILQ_FIRST(&msg->mhdr) !=
                                            STAILQ_LAST(&msg->mhdr, mbuf, next);
  if (tc->response) {
    for (uint32_t k = 0; k < array_n(msg->args); k++) {
      struct argpos *ap = array_get(msg->args, k);
      len += (size_t)dn_snprintf(keys + len, sizeof(keys) - len, "%s%.*s",
                                 k == 0 ? "" : " ", (int)(ap->end - ap->start),
                                 ap->start);
    }
  }
  for (uint32_t k = 0; k < array_n(msg->keys); k++) {
    struct keypos *kp = array_get(msg->keys, k);
    len += (size_t)dn_snprintf(keys + len, sizeof(keys) - len, "%s%.*s",
                               k == 0 ? "" : " ", (int)(kp->end - kp->start),
                               kp->start);
    if (kp->tag_start != kp->start || kp->tag_end != kp->end) {
      len += (size_t)dn_snprintf(keys + len, sizeof(keys) - len, "(%.*s)",
                                 (int)(kp->tag_end - kp->tag_start),
                                 kp->tag_start);
    }
  }

  if (i >= tc->nreqs) {
    log_error("%s (mbuf %zu, chunk %zu%s): unexpected %s %zu, %.*s [%s]",
              tc->name, mbuf_data_size(), chunk, parse_mode(), what, i + 1,
              msg_type_string(msg->type)->len, msg_type_string(msg->type)->data,
              keys);
    return DN_ERROR;
  }
  if (msg->type != tc->reqs[i].type ||
      (!args_unchecked && strcmp(keys, tc->reqs[i].keys) != 0)) {
    log_error("%s (mbuf %zu, chunk %zu%s): %s %zu is %.*s [%s], want "
              "%.*s [%s]",
              tc->name, mbuf_data_size(), chunk, parse_mode(), what, i + 1,
              msg_type_string(msg->type)->len, msg_type_string(msg->type)->data,
              keys, msg_type_string(tc->reqs[i].type)->len,
              msg_type_string(tc->reqs[i].type)->data, tc->reqs[i].keys);
    return DN_ERROR;
  }
  return DN_OK;
}

/*
 * Feed the stream to the parser at most chunk bytes at a time, the way
 * msg_recv_chain() and msg_parse() do when data arrives in pieces: reads go
 * into the last mbuf until it's full, a parsed request with data after it is
 * split off with that data as the next message (msg_parsed()), and a request
 * cut at the end of an mbuf is moved to a new one (msg_repair()).
 */
static rstatus_t parse_run(struct conn *conn, const struct parse_case *tc,
                           size_t chunk) {
  struct msg *msg, *nmsg;
  struct mbuf *mbuf, *nbuf;
  size_t fed = 0, nparsed = 0;
  bool rejected = false;
  rstatus_t status = DN_OK;

  g_data_store = tc->data_store;
  conn->dyn_mode = tc->dyn_mode ? 1 : 0;
  string_init(&ctx->pool.hash_tag);
  if (tc->hash_tag != NULL) {
    string_set_raw(&ctx->pool.hash_tag, tc->hash_tag);
  }
  msg = msg_get(conn, !tc->response, __FUNCTION__);

  while (fed < tc->len && !rejected && status == DN_OK) {
    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
      mbuf = mbuf_get();
      mbuf_insert(&msg->mhdr, mbuf);
      msg->pos = mbuf->pos;
    }
    size_t n = MIN(MIN(chunk, tc->len - fed), mbuf_remaining_space(mbuf));
    mbuf_copy(mbuf, (uint8_t *)tc->data + fed, n);
    fed += n;
    msg->mlen += (uint32_t)n;

    while (status == DN_OK && !msg_empty(msg)) {
      msg->parser(msg, ctx);

      if (msg->result == MSG_PARSE_AGAIN) {
        break;
      }
      if (msg->result == MSG_PARSE_REPAIR) {
        nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
        mbuf_insert(&msg->mhdr, nbuf);
        msg->pos = nbuf->pos;
        break;
      }
      if (msg->result != MSG_PARSE_OK) {
        rejected = true;
        break;
      }

      status = parse_check(tc, chunk, nparsed++, msg);

      mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
      if (msg->pos == mbuf->last) {
        msg_put(msg);
        msg = msg_get(conn, !tc->response, __FUNCTION__);
        break;
      }
      nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
      nmsg = msg_get(conn, !tc->response, __FUNCTION__);
      mbuf_insert(&nmsg->mhdr, nbuf);
      nmsg->pos = nbuf->pos;
      nmsg->mlen = mbuf_length(nbuf);
      msg_put(msg);
      msg = nmsg;
    }
  }

  if (status == DN_OK && (nparsed != tc->nreqs || rejected != tc->reject)) {
    log_error("%s (mbuf %zu, chunk %zu%s): parsed %zu of %zu %ss, %s",
              tc->name, mbuf_data_size(), chunk, parse_mode(), nparsed,
              tc->nreqs, tc->response ? "response" : "request",
              rejected ? "then rejected the rest"
                       : (msg_empty(msg) ? "no data left"
                                         : "with unparsed data left"));
    status = DN_ERROR;
  }
  msg_put(msg);
  string_init(&ctx->pool.hash_tag);
  return status;
}

static struct parse_case parse_cases[48];
static size_t nparse_cases;

static struct parse_case *parse_case_new(const char *name,
                                         data_store_t data_store,
                                         bool dyn_mode) {
  ASSERT(nparse_cases < NELEMS(parse_cases));
  struct parse_case *tc = &parse_cases[nparse_cases++];
  memset(tc, 0, sizeof(*tc));
  tc->name = name;
  tc->data_store = data_store;
  tc->dyn_mode = dyn_mode;
  return tc;
}

static void parse_cases_init(void) {
  struct parse_case *tc;
  char big[600 + 1], words[700], rsp[2048], args[1024];

  /* a value larger than the smallest mbuf */
  memset(big, 'x', sizeof(big) - 1);
  big[sizeof(big) - 1] = '\0';

  tc = parse_case_new("redis pipeline", DATA_REDIS, false);
  parse_add(tc, MSG_REQ_REDIS_PING, "", "PING");
  parse_add_req(tc, MSG_REQ_REDIS_PING, "", "PING\r\n", true); /* inline */
  parse_add(tc, MSG_REQ_REDIS_PING, "", "ping hello");
  parse_add(tc, MSG_REQ_REDIS_SET, "foo", "SET foo bar");
  parse_add(tc, MSG_REQ_REDIS_GET, "foo", "get foo");
  dn_snprintf(words, sizeof(words), "SET big %s", big);
  parse_add(tc, MSG_REQ_REDIS_SET, "big", words);
  parse_add(tc, MSG_REQ_REDIS_MSET, "k1 k2", "MSET k1 v1 k2 v2");
  parse_add(tc, MSG_REQ_REDIS_MGET, "k1 k2 k3", "MGET k1 k2 k3");
  parse_add(tc, MSG_REQ_REDIS_DEL, "k1 k2", "DEL k1 k2");
  parse_add(tc, MSG_REQ_REDIS_EXISTS, "k1", "Exists k1");
  parse_add(tc, MSG_REQ_REDIS_HSET, "h", "HSET h f v");
  parse_add(tc, MSG_REQ_REDIS_ZADD, "z", "ZADD z 1 m");
  parse_add(tc, MSG_REQ_REDIS_LPUSH, "l", "LPUSH l a b");
  parse_add(tc, MSG_REQ_REDIS_EVAL, "ek", "EVAL return(1) 1 ek arg");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_LOAD, "", "SCRIPT LOAD return(1)");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_EXISTS, "",
            "script exists e0e1f9fabfc9d4800c877a703b823ac0578ff8db");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_FLUSH, "", "Script Flush");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_KILL, "", "SCRIPT KILL");
  parse_add(tc, MSG_REQ_REDIS_CLIENT_SETINFO, "",
            "CLIENT SETINFO lib-name test");
  parse_add(tc, MSG_REQ_REDIS_HELLO, "", "HELLO 3");
  parse_add(tc, MSG_REQ_REDIS_GET, "last", "GET last");

  /* requests sent by a dnode peer, each with a dnode header */
  tc = parse_case_new("dnode", DATA_REDIS, true);
  parse_add(tc, MSG_REQ_REDIS_SET, "foo1", "set foo1 bar1");
  dn_snprintf(words, sizeof(words), "set foo2 %s", big);
  parse_add(tc, MSG_REQ_REDIS_SET, "foo2", words);
  parse_add(tc, MSG_REQ_REDIS_SET, "foo3", "set foo3 bar3");

  tc = parse_case_new("memcache", DATA_MEMCACHE, false);
  parse_add(tc, MSG_REQ_MC_GET, "k1 k2", "get k1 k2\r\n");
  parse_add(tc, MSG_REQ_MC_GETS, "k3", "gets k3\r\n");
  parse_add(tc, MSG_REQ_MC_SET, "k1", "set k1 0 0 5\r\nhello\r\n");
  dn_snprintf(words, sizeof(words), "set big 0 0 %zu\r\n%s\r\n", strlen(big),
              big);
  parse_add(tc, MSG_REQ_MC_SET, "big", words);
  parse_add(tc, MSG_REQ_MC_INCR, "n", "incr n 1\r\n");
  parse_add(tc, MSG_REQ_MC_DELETE, "k1", "delete k1\r\n");

  /* each in a stream of its own, after a valid request; the SCRIPT ones were
   * accepted before fd14eb7 */
  static const char *const rejects[] = {
      "SCRIPT GET foo", "SCRIPT DEBUG yes", "SCRIPT",      "KILL",
      "LOAD return(1)", "FLUSH",            "CLIENT LIST", "NOSUCHCOMMAND x",
  };
  for (size_t i = 0; i < NELEMS(rejects); i++) {
    tc = parse_case_new(rejects[i], DATA_REDIS, false);
    parse_add(tc, MSG_REQ_REDIS_GET, "foo", "GET foo");
    parse_reject(tc, rejects[i]);
  }

  /* the rest are named, since the messages end in CRLF */
  static const struct {
    const char *name, *data;
  } mc_rejects[] = {
      {"memcache unknown command", "bogus k\r\n"},
      {"memcache bad length", "set k 0 0 x\r\nhello\r\n"},
      {"memcache value too long", "set k 0 0 5\r\nhelloX\r\n"},
  };
  for (size_t i = 0; i < NELEMS(mc_rejects); i++) {
    tc = parse_case_new(mc_rejects[i].name, DATA_MEMCACHE, false);
    parse_add(tc, MSG_REQ_MC_GET, "foo", "get foo\r\n");
    parse_reject(tc, mc_rejects[i].data);
  }
  /* a key one byte longer than memcache allows (MEMCACHE_MAX_KEY_LENGTH) */
  tc = parse_case_new("memcache long key", DATA_MEMCACHE, false);
  parse_add(tc, MSG_REQ_MC_GET, "foo", "get foo\r\n");
  dn_snprintf(words, sizeof(words), "get %.*s\r\n", 251, big);
  parse_reject(tc, words);

  tc = parse_case_new("redis hash tags", DATA_REDIS, false);
  tc->hash_tag = "{}";
  parse_add(tc, MSG_REQ_REDIS_GET, "user:{42}:name(42)", "GET user:{42}:name");
  parse_add(tc, MSG_REQ_REDIS_MGET, "{a}1(a) {a}2(a) b", "MGET {a}1 {a}2 b");
  parse_add(tc, MSG_REQ_REDIS_GET, "{}x()", "GET {}x");
  parse_add(tc, MSG_REQ_REDIS_GET, "a{b", "GET a{b");
  parse_add(tc, MSG_REQ_REDIS_GET, "a}b{c}(c)", "GET a}b{c}");
  /* the second key is cut at the end of the smallest mbuf */
  dn_snprintf(words, sizeof(words), "MGET %.*s {t}abcdefghijklmnop", 430, big);
  dn_snprintf(rsp, sizeof(rsp), "%.*s {t}abcdefghijklmnop(t)", 430, big);
  parse_add(tc, MSG_REQ_REDIS_MGET, rsp, words);

  tc = parse_case_new("memcache hash tags", DATA_MEMCACHE, false);
  tc->hash_tag = "{}";
  parse_add(tc, MSG_REQ_MC_GET, "user:{42}:name(42) b",
            "get user:{42}:name b\r\n");
  parse_add(tc, MSG_REQ_MC_SET, "{a}1(a)", "set {a}1 0 0 1\r\nv\r\n");

  tc = parse_case_new("redis responses", DATA_REDIS, false);
  tc->response = true;
  parse_add_rsp(tc, MSG_RSP_REDIS_STATUS, "", "+OK\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_ERROR_ERR, "", "-ERR unknown command\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_ERROR_WRONGTYPE, "",
                "-WRONGTYPE Operation against a key holding the wrong kind "
                "of value\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_ERROR_NOSCRIPT, "",
                "-NOSCRIPT No matching script\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_ERROR, "", "-MOVED 3999 127.0.0.1:6381\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_INTEGER, "", ":1000\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_INTEGER, "", ":-1\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_BULK, "", "$3\r\nbar\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_BULK, "", "$0\r\n\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_BULK, "", "$-1\r\n"); /* nil */
  dn_snprintf(rsp, sizeof(rsp), "$%zu\r\n%s\r\n", strlen(big), big);
  parse_add_rsp(tc, MSG_RSP_REDIS_BULK, "", rsp);
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "", "*0\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "", "*-1\r\n"); /* nil */
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "a  bc",
                "*4\r\n$1\r\na\r\n$0\r\n\r\n$-1\r\n$2\r\nbc\r\n");
  /* simple elements keep their type byte, which read repairs rely on */
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, ":1 +OK -ERR no-such:key",
                "*3\r\n:1\r\n+OK\r\n-ERR no-such:key\r\n");
  /* the reply of a read repair script */
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "E :1700000000000 bar",
                "*3\r\n$1\r\nE\r\n:1700000000000\r\n$3\r\nbar\r\n");
  /* SCAN: a cursor, then the keys */
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "0 a b",
                "*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n");
  /* GEOPOS: arrays nested anywhere */
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "1.5 2.5",
                "*2\r\n*2\r\n$3\r\n1.5\r\n$3\r\n2.5\r\n*-1\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "a b",
                "*3\r\n*1\r\n$1\r\na\r\n*0\r\n*1\r\n$1\r\nb\r\n");
  dn_snprintf(rsp, sizeof(rsp), "*2\r\n$%zu\r\n%s\r\n$1\r\nz\r\n",
              strlen(big), big);
  dn_snprintf(args, sizeof(args), "%s z", big);
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, args, rsp);
  /* larger than the smallest mbuf, so some element is split between two */
  int n = dn_snprintf(rsp, sizeof(rsp), "*150\r\n");
  size_t alen = 0;
  for (int i = 0; i < 150; i++) {
    n += dn_snprintf(rsp + n, sizeof(rsp) - (size_t)n, ":%d\r\n", i);
    alen += (size_t)dn_snprintf(args + alen, sizeof(args) - alen, "%s:%d",
                                i == 0 ? "" : " ", i);
  }
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, args, rsp);
  n = dn_snprintf(rsp, sizeof(rsp), "*150\r\n");
  alen = 0;
  for (int i = 0; i < 150; i++) {
    n += dn_snprintf(rsp + n, sizeof(rsp) - (size_t)n, "$%d\r\n%d\r\n",
                     i < 10 ? 1 : (i < 100 ? 2 : 3), i);
    alen += (size_t)dn_snprintf(args + alen, sizeof(args) - alen, "%s%d",
                                i == 0 ? "" : " ", i);
  }
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, args, rsp);
  parse_add_rsp(tc, MSG_RSP_REDIS_STATUS, "", "+OK\r\n");

  /* responses from a dnode peer, each with a dnode header */
  tc = parse_case_new("dnode responses", DATA_REDIS, true);
  tc->response = true;
  parse_add_rsp(tc, MSG_RSP_REDIS_STATUS, "", "+OK\r\n");
  dn_snprintf(rsp, sizeof(rsp), "$%zu\r\n%s\r\n", strlen(big), big);
  parse_add_rsp(tc, MSG_RSP_REDIS_BULK, "", rsp);
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "a b",
                "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_ERROR_ERR, "", "-ERR x\r\n");

  tc = parse_case_new("memcache responses", DATA_MEMCACHE, false);
  tc->response = true;
  parse_add_rsp(tc, MSG_RSP_MC_STORED, "", "STORED\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_NOT_STORED, "", "NOT_STORED\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_EXISTS, "", "EXISTS\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_NOT_FOUND, "", "NOT_FOUND\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_DELETED, "", "DELETED\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_TOUCHED, "", "TOUCHED\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_NUM, "", "42\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_END, "", "END\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_END, "", "VALUE k1 0 5\r\nhello\r\nEND\r\n");
  dn_snprintf(rsp, sizeof(rsp),
              "VALUE k1 0 5 12345\r\nhello\r\nVALUE k2 3 %zu 678\r\n%s\r\n"
              "END\r\n",
              strlen(big), big);
  parse_add_rsp(tc, MSG_RSP_MC_END, "", rsp);
  parse_add_rsp(tc, MSG_RSP_MC_ERROR, "", "ERROR\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_CLIENT_ERROR, "",
                "CLIENT_ERROR bad data chunk\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_SERVER_ERROR, "",
                "SERVER_ERROR out of memory\r\n");
  parse_add_rsp(tc, MSG_RSP_MC_STORED, "", "STORED\r\n");

  static const struct {
    const char *name, *data;
  } rsp_rejects[] = {
      {"redis unknown reply type", "?\r\n"},
      {"redis bulk too long", "$3\r\nbarX\r\n"},
      {"redis bad integer", ":12a\r\n"},
      {"redis bulk without length", "$\r\n"},
      {"redis bad array length", "*x\r\n"},
      {"redis bad array element", "*1\r\n!x\r\n"},
      {"redis CR without LF", "+OK\rX"},
  };
  for (size_t i = 0; i < NELEMS(rsp_rejects); i++) {
    tc = parse_case_new(rsp_rejects[i].name, DATA_REDIS, false);
    tc->response = true;
    parse_add_rsp(tc, MSG_RSP_REDIS_STATUS, "", "+OK\r\n");
    parse_reject(tc, rsp_rejects[i].data);
  }
  static const struct {
    const char *name, *data;
  } mc_rsp_rejects[] = {
      {"memcache unknown reply", "BOGUS\r\n"},
      {"memcache bad number", "12a\r\n"},
      {"memcache reply value too long", "VALUE k1 0 5\r\nhelloX\r\n"},
      {"memcache bad flags", "VALUE k1 x 5\r\nhello\r\nEND\r\n"},
  };
  for (size_t i = 0; i < NELEMS(mc_rsp_rejects); i++) {
    tc = parse_case_new(mc_rsp_rejects[i].name, DATA_MEMCACHE, false);
    tc->response = true;
    parse_add_rsp(tc, MSG_RSP_MC_STORED, "", "STORED\r\n");
    parse_reject(tc, mc_rsp_rejects[i].data);
  }
}

static rstatus_t test_parser(struct conn *conn) {
  static const size_t mbuf_sizes[] = {MBUF_MIN_SIZE, TEST_MBUF_SIZE};
  static const size_t chunks[] = {SIZE_MAX, 1, 2, 3, 7, 64, 509};
  size_t nruns = 0, nfailed = 0;

  print_banner("PARSER");
  parse_cases_init();

  /* the redis parser takes another path through the mbufs with read repairs */
  for (int repairs = 0; repairs <= 1; repairs++) {
    g_read_repairs_enabled = repairs;
    g_read_consistency = g_write_consistency = repairs ? DC_QUORUM : DC_ONE;
    for (size_t m = 0; m < NELEMS(mbuf_sizes); m++) {
      mbuf_deinit();
      mbuf_init(mbuf_sizes[m]);
      for (size_t t = 0; t < nparse_cases; t++) {
        for (size_t c = 0; c < NELEMS(chunks); c++) {
          nruns++;
          if (parse_run(conn, &parse_cases[t], chunks[c]) != DN_OK) {
            nfailed++;
          }
        }
      }
    }
  }
  mbuf_deinit();
  mbuf_init(TEST_MBUF_SIZE);
  g_data_store = DATA_REDIS;
  g_read_repairs_enabled = false;
  g_read_consistency = g_write_consistency = DC_ONE;

  if (nfailed != 0) {
    log_error("%zu of %zu parser runs failed", nfailed, nruns);
    return DN_ERROR;
  }
  loga("%zu parser runs passed", nruns);
  return DN_OK;
}

static void gen_random(unsigned char *s, const int len) {
  static const unsigned char possible_data[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz\r\n";
  int i;
  for (i = 0; i < len; ++i) {
    s[i] = possible_data[rand() % (sizeof(possible_data) - 1)];
  }

  s[len] = 0;
}

#define MAX_MSG_LEN 512
static rstatus_t aes_test(void) {
  unsigned char msg[MAX_MSG_LEN + 1];
  print_banner("AES");
  unsigned char *aes_key = generate_aes_key();
  SCOPED_CHARPTR(aes_key_print) = base64_encode(aes_key, AES_KEYLEN);
  loga("aesKey is '%s'", aes_key_print);

  size_t i = 0;
  size_t count = 1000000;
  loga("Running %lu encryption/decryption messages", count);
  for (; i < count; i++) {
    gen_random(msg, rand() % MAX_MSG_LEN);
    unsigned int msg_len = strlen(msg) + 1;  // Also encrypt the \0 at the end

    // Encrypt the message with AES
    unsigned char *enc_msg = NULL;
    rstatus_t ret =
        aes_encrypt((const unsigned char *)msg, msg_len, &enc_msg, aes_key);
    if (ret == DN_ERROR) {
      log_panic("msg:'%s'\nencryption failed aes_key '%s'\n", msg,
                aes_key_print);
      return ret;
    }
    size_t enc_msg_len = (size_t)ret; /* if success, aes_encrypt returns len */

    size_t expected_output_len = 16 * (msg_len / 16 + 1);
    if (enc_msg_len != expected_output_len) {
      log_panic("msg:'%s'\nexpected encrypted len: %lu encrypted len %lu\n\n",
                msg, expected_output_len, enc_msg_len);
      return DN_ERROR;
    }

    // Decrypt the message
    char *dec_msg = NULL;
    ret = aes_decrypt((unsigned char *)enc_msg, enc_msg_len,
                      (unsigned char **)&dec_msg, aes_key);
    ASSERT_LOG(ret != DN_ERROR,
               "msg '%s'\nencrypted msg:'%.*s'\ndecryption failed aes_key %s",
               msg, enc_msg_len, enc_msg, aes_key_print);

    if (strcmp(msg, dec_msg) != 0) {
      loga_hexdump(msg, strlen(msg), "Original Message:");
      loga_hexdump(dec_msg, strlen(dec_msg), "Decrypted Message:");
      log_panic("encryption/Decryption mismatch");
    }

    free(enc_msg);
    free(dec_msg);
    if ((i + 1) % 1000000 == 0) {
      loga("Completed Running %lu messages", i + 1);
    }
  }
  return DN_OK;
}
/**
 * I recently added a counter to the tailQ in dyn_queue.h
 * This is a function to test the counter.
 * It is implied that all changes to the tailQ should be
 * done via the macros provided.
 */
struct test {
  TAILQ_ENTRY(test) test_tqe; /* link in active connection pool */
};

TAILQ_HEAD(test_tqh, test);
static rstatus_t test_tailq(void) {
  struct test_tqh test_q1;
  print_banner("TAILQ COUNTER");
  TAILQ_INIT(&test_q1);
  unsigned int i = 0;
  for (i = 0; i < 5; i++) {
    struct test *test = dn_zalloc(sizeof(struct test));
    TAILQ_INSERT_TAIL(&test_q1, test, test_tqe);
    if ((i + 1) != TAILQ_COUNT(&test_q1)) {
      log_error("Count mismatch expected %d, got %lu", i + 1,
                TAILQ_COUNT(&test_q1));
      return DN_ERROR;
    }
  }
  struct test_tqh test_q2;
  TAILQ_INIT(&test_q2);
  for (i = 0; i < 5; i++) {
    struct test *test = dn_zalloc(sizeof(struct test));
    TAILQ_INSERT_TAIL(&test_q2, test, test_tqe);
    if ((i + 1) != TAILQ_COUNT(&test_q2)) {
      log_error("Count mismatch expected %u, got %" PRIu64, i + 1,
                TAILQ_COUNT(&test_q2));
      return DN_ERROR;
    }
  }
  // CONCATENATE
  TAILQ_CONCAT(&test_q1, &test_q2, test_tqe);
  if ((10 != TAILQ_COUNT(&test_q1)) || (0 != TAILQ_COUNT(&test_q2))) {
    log_error("Count mismatch: q1: %lu, q2: %lu", TAILQ_COUNT(&test_q1),
              TAILQ_COUNT(&test_q2));
    return DN_ERROR;
  }
  // REMOVE HEAD
  struct test *test = TAILQ_FIRST(&test_q1);
  TAILQ_REMOVE(&test_q1, test, test_tqe);
  dn_free(test);
  if (9 != TAILQ_COUNT(&test_q1)) {
    log_error("Count mismatch expected 9, got %lu", TAILQ_COUNT(&test_q1));
    return DN_ERROR;
  }
  // REMOVE TAIL
  test = TAILQ_LAST(&test_q1, test_tqh);
  TAILQ_REMOVE(&test_q1, test, test_tqe);
  dn_free(test);
  if (8 != TAILQ_COUNT(&test_q1)) {
    log_error("Count mismatch expected 8, got %lu", TAILQ_COUNT(&test_q1));
    return DN_ERROR;
  }

  // SWAP TAILQ
  TAILQ_SWAP(&test_q1, &test_q2, test, test_tqe);
  if ((0 != TAILQ_COUNT(&test_q1)) || (8 != TAILQ_COUNT(&test_q2))) {
    log_error("Count mismatch: q1: %lu, q2: %lu", TAILQ_COUNT(&test_q1),
              TAILQ_COUNT(&test_q2));
    return DN_ERROR;
  }

  // SWAP BACK
  TAILQ_SWAP(&test_q1, &test_q2, test, test_tqe);
  if ((8 != TAILQ_COUNT(&test_q1)) || (0 != TAILQ_COUNT(&test_q2))) {
    log_error("Count mismatch: q1: %lu, q2: %lu", TAILQ_COUNT(&test_q1),
              TAILQ_COUNT(&test_q2));
    return DN_ERROR;
  }

  // FOREACH SAFE, REMOVE
  struct test *temp_test;
  i = 8;
  TAILQ_FOREACH_SAFE(test, &test_q1, test_tqe, temp_test) {
    if (i != TAILQ_COUNT(&test_q1)) {
      log_error("Count mismatch expected %u, got %" PRIu64, i,
                TAILQ_COUNT(&test_q1));
      return DN_ERROR;
    }
    TAILQ_REMOVE(&test_q1, test, test_tqe);
    dn_free(test);
    i--;
  }
  loga(".....SUCCESS...");
  return DN_OK;
}

static void peer_ref(struct conn *conn, void *owner) {}

struct conn_ops peer_ops = {
    NULL,     NULL, NULL, NULL, NULL, NULL, NULL, NULL,
    peer_ref, NULL, NULL, NULL, NULL, NULL, NULL,
};

static void init_peer_conn(struct conn *conn) {
  conn->dyn_mode = 1;
  conn->sd = 0;
  conn->ops = &peer_ops;
}
static void test_core_ctx_create(struct instance *nci) {

  srand((unsigned)time(NULL));

  ctx = dn_zalloc(sizeof(*ctx));
  if (ctx == NULL) {
    loga("Failed to create context!!!");
  }
  nci->ctx = ctx;
  ctx->instance = nci;
  ctx->cf = NULL;
  ctx->stats = NULL;
  ctx->evb = NULL;
  ctx->dyn_state = INIT;
}

/**
 * This is very primitive
 */
static void test_server_pool(struct instance *nci) {
  struct context *ctx = nci->ctx;
  struct server_pool *sp = &ctx->pool;
  sp->mbuf_size = TEST_MBUF_SIZE;
  sp->alloc_msgs_max = TEST_ALLOC_MSGS_MAX;
  char *filename = getenv("DYNOMITE_TEST_PEM");
  if (filename == NULL) {
    filename = "conf/dynomite.pem";
  }
  string_copy(&sp->pem_key_file, filename, strlen(filename));
  sp->secure_server_option = SECURE_OPTION_DC;

  mbuf_init(sp->mbuf_size);
  msg_init(sp->alloc_msgs_max);
}

static void init_test(int argc, char **argv) {
  rstatus_t status;
  struct instance nci;

  test_set_default_options(&nci);

  status = test_get_options(argc, argv, &nci);
  if (status != DN_OK) {
    dn_show_usage();
    exit(1);
  }

  test_pre_run(&nci);

  test_core_ctx_create(&nci);
  conn_init();

  test_server_pool(&nci);

  status = crypto_init(&(nci.ctx->pool));
  if (status != DN_OK) {
    loga("Failed to initialize crypto!!!");
    exit(1);
  }
}

int main(int argc, char **argv) {
  // rstatus_t status;
  init_test(argc, argv);

  struct node *peer = malloc(sizeof(struct node));
  memset(peer, 0, sizeof(struct node));
  init_peer(peer);

  struct conn *conn = conn_get(peer, init_peer_conn);

  rstatus_t ret = test_parser(conn);
  if (ret != DN_OK) {
    loga("Error in testing the parser!!!");
    goto err_out;
  }

  ret = test_tailq();
  if (ret != DN_OK) {
    loga("Error in testing TAILQ counter!!!");
    goto err_out;
  }

  ret = aes_test();
  if (ret != DN_OK) {
    loga("Error in testing AES !!!");
    goto err_out;
  }

  loga("Testing is done!!!");
err_out:
  deinit_peer(peer);
  return ret;
}
