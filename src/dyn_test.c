/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2015 Netflix, Inc.
 */

#include <ctype.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include <unistd.h>

#include "dyn_conf.h"
#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_tls.h"
#include "proto/dyn_proto.h"

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

  s->processed = 0;

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
 * the whole key), or for a response, its args. A redis request can also have
 * the args read repairs record for it.
 */
struct parse_req {
  msg_type_t type;
  char keys[1024];  /* space-separated */
  const char *args; /* space-separated, NULL if not checked */
  bool too_long;    /* a key was skipped as too long (keys has the others) */
  bool wrong_arity; /* a command came with the wrong number of args */
};

#define PARSE_DATA_MAX 81920
#define PARSE_REQS_MAX 64

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
  tc->reqs[tc->nreqs].args = NULL;
  tc->reqs[tc->nreqs].too_long = false;
  tc->reqs[tc->nreqs].wrong_arity = false;
  tc->nreqs++;
}

static void parse_add(struct parse_case *tc, msg_type_t type, const char *keys,
                      const char *words) {
  parse_add_req(tc, type, keys, words, false);
}

/* Append a redis request, also with the args read repairs record for it. */
static void parse_add_args(struct parse_case *tc, msg_type_t type,
                           const char *keys, const char *args,
                           const char *words) {
  parse_add(tc, type, keys, words);
  tc->reqs[tc->nreqs - 1].args = args;
}

/*
 * Append a request with a key too long to keep, which the parser should skip
 * and flag; keys are the other ones.
 */
static void parse_add_too_long(struct parse_case *tc, msg_type_t type,
                               const char *keys, const char *words) {
  parse_add(tc, type, keys, words);
  tc->reqs[tc->nreqs - 1].too_long = true;
}

/*
 * Append a redis command with the wrong number of args, which the parser
 * should read past and flag; keys are the ones it has.
 */
static void parse_add_wrong_arity(struct parse_case *tc, msg_type_t type,
                                  const char *keys, const char *words) {
  parse_add(tc, type, keys, words);
  tc->reqs[tc->nreqs - 1].wrong_arity = true;
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

/*
 * Copy an arg recorded for msg, from as many mbufs as it spans, plus extra
 * bytes after it, to dst. Fails if the arg doesn't start in one of msg's
 * mbufs, or msg ends before it does.
 */
static bool parse_copy_arg(struct msg *msg, const struct argpos *ap,
                           uint32_t extra, uint8_t *dst, uint32_t size) {
  struct argpos arg = *ap;
  struct mbuf *b;

  STAILQ_FOREACH(b, &msg->mhdr, next) {
    if (b == ap->mbuf) break;
  }
  if (b == NULL || ap->start < b->pos || ap->start > b->last) {
    return false;
  }
  arg.len += extra;
  return arg.len <= size && argpos_copy(&arg, dst, size) == arg.len;
}

/* Append the args recorded for msg to out, space-separated. */
static bool parse_render_args(struct msg *msg, char *out, size_t size,
                              size_t *len) {
  static uint8_t buf[PARSE_DATA_MAX];

  for (uint32_t k = 0; k < array_n(msg->args); k++) {
    struct argpos *ap = array_get(msg->args, k);
    if (!parse_copy_arg(msg, ap, 0, buf, sizeof(buf))) {
      return false;
    }
    if (*len < size) {
      *len += (size_t)dn_snprintf(out + *len, size - *len, "%s%.*s",
                                  k == 0 ? "" : " ", (int)ap->len, buf);
    }
  }
  return true;
}

/*
 * Check the args read repairs recorded for a redis request: each must be a
 * whole bulk string, recorded once, however many mbufs it spans. They must
 * also be the ones expected, if given.
 */
static rstatus_t parse_check_args(const struct parse_case *tc, size_t chunk,
                                  size_t i, struct msg *msg) {
  static uint8_t buf[PARSE_DATA_MAX];
  char args[1024] = "";
  size_t len = 0;

  for (uint32_t k = 0; k < array_n(msg->args); k++) {
    struct argpos *ap = array_get(msg->args, k);
    uint8_t *q;
    size_t n = 0, scale = 1;

    /* CRLF after it, and "$<len>\r\n" before it, as far as it's in its mbuf */
    bool whole = parse_copy_arg(msg, ap, CRLF_LEN, buf, sizeof(buf)) &&
                 buf[ap->len] == CR && buf[ap->len + 1] == LF;
    for (q = ap->start - 1; whole && q >= ap->mbuf->pos; q--) {
      if (q >= ap->start - 2) {
        whole = *q == (q == ap->start - 1 ? LF : CR);
      } else if (isdigit(*q)) {
        n += (size_t)(*q - '0') * scale;
        scale *= 10;
      } else {
        whole = *q == '$' && n == ap->len;
        break;
      }
    }
    if (!whole) {
      log_error("%s (mbuf %zu, chunk %zu%s): request %zu has arg %u of "
                "length %" PRIu32 ", not a whole bulk string",
                tc->name, mbuf_data_size(), chunk, parse_mode(), i + 1, k + 1,
                ap->len);
      return DN_ERROR;
    }
  }

  if (tc->reqs[i].args != NULL &&
      (!parse_render_args(msg, args, sizeof(args), &len) ||
       strcmp(args, tc->reqs[i].args) != 0)) {
    log_error("%s (mbuf %zu, chunk %zu%s): request %zu has args [%s], want "
              "[%s]",
              tc->name, mbuf_data_size(), chunk, parse_mode(), i + 1, args,
              tc->reqs[i].args);
    return DN_ERROR;
  }
  return DN_OK;
}

/* Compare a parsed message with the one expected. */
static rstatus_t parse_check(const struct parse_case *tc, size_t chunk,
                             size_t i, struct msg *msg) {
  const char *what = tc->response ? "response" : "request";
  char keys[sizeof(tc->reqs[0].keys)] = "";
  size_t len = 0;

  if (tc->response && !parse_render_args(msg, keys, sizeof(keys), &len)) {
    log_error("%s (mbuf %zu, chunk %zu%s): %s %zu has an arg outside it",
              tc->name, mbuf_data_size(), chunk, parse_mode(), what, i + 1);
    return DN_ERROR;
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
  if (msg->key_too_long != tc->reqs[i].too_long) {
    log_error("%s (mbuf %zu, chunk %zu%s): %s %zu %s a key too long", tc->name,
              mbuf_data_size(), chunk, parse_mode(), what, i + 1,
              msg->key_too_long ? "has" : "doesn't have");
    return DN_ERROR;
  }
  if (msg->wrong_arity != tc->reqs[i].wrong_arity) {
    log_error("%s (mbuf %zu, chunk %zu%s): %s %zu is %sflagged wrong arity",
              tc->name, mbuf_data_size(), chunk, parse_mode(), what, i + 1,
              msg->wrong_arity ? "" : "not ");
    return DN_ERROR;
  }
  if (msg->type != tc->reqs[i].type ||
      strcmp(keys, tc->reqs[i].keys) != 0) {
    log_error("%s (mbuf %zu, chunk %zu%s): %s %zu is %.*s [%s], want "
              "%.*s [%s]",
              tc->name, mbuf_data_size(), chunk, parse_mode(), what, i + 1,
              msg_type_string(msg->type)->len, msg_type_string(msg->type)->data,
              keys, msg_type_string(tc->reqs[i].type)->len,
              msg_type_string(tc->reqs[i].type)->data, tc->reqs[i].keys);
    return DN_ERROR;
  }
  if (g_read_repairs_enabled && !tc->response &&
      tc->data_store == DATA_REDIS) {
    return parse_check_args(tc, chunk, i, msg);
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
  /* the expected request args must outlive this function */
  static char big[600 + 1], big_v[700];
  /* a key longer than the largest test mbuf, of letters in turn too */
  static char huge[TEST_MBUF_SIZE + 600 + 1], long_words[sizeof(huge) + 700];
  char words[700], rsp[2048], args[1024];

  /*
   * A value larger than the smallest mbuf, of letters in turn, so that a byte
   * lost or repeated where it's split between mbufs shows.
   */
  for (size_t i = 0; i < sizeof(big) - 1; i++) {
    big[i] = (char)('a' + i % 26);
  }
  big[sizeof(big) - 1] = '\0';
  dn_snprintf(big_v, sizeof(big_v), "%s v", big);
  for (size_t i = 0; i < sizeof(huge) - 1; i++) {
    huge[i] = (char)('a' + i % 26);
  }
  huge[sizeof(huge) - 1] = '\0';

  tc = parse_case_new("redis pipeline", DATA_REDIS, false);
  parse_add(tc, MSG_REQ_REDIS_PING, "", "PING");
  parse_add_req(tc, MSG_REQ_REDIS_PING, "", "PING\r\n", true); /* inline */
  parse_add_args(tc, MSG_REQ_REDIS_PING, "", "hello", "ping hello");
  parse_add_args(tc, MSG_REQ_REDIS_SET, "foo", "bar", "SET foo bar");
  parse_add(tc, MSG_REQ_REDIS_GET, "foo", "get foo");
  dn_snprintf(words, sizeof(words), "SET big %s", big);
  parse_add_args(tc, MSG_REQ_REDIS_SET, "big", big, words);
  dn_snprintf(words, sizeof(words), "HSET h %s v", big);
  parse_add_args(tc, MSG_REQ_REDIS_HSET, "h", big_v, words);
  parse_add_args(tc, MSG_REQ_REDIS_MSET, "k1 k2", "v1 v2", "MSET k1 v1 k2 v2");
  parse_add(tc, MSG_REQ_REDIS_MGET, "k1 k2 k3", "MGET k1 k2 k3");
  parse_add(tc, MSG_REQ_REDIS_DEL, "k1 k2", "DEL k1 k2");
  parse_add(tc, MSG_REQ_REDIS_EXISTS, "k1", "Exists k1");
  parse_add_args(tc, MSG_REQ_REDIS_HSET, "h", "f v", "HSET h f v");
  parse_add_args(tc, MSG_REQ_REDIS_HSET, "h", "f1 v1 f2 v2",
                 "HSET h f1 v1 f2 v2");
  /* Redis rejects these: passed on for it to answer */
  parse_add_args(tc, MSG_REQ_REDIS_HSET, "h", "f1 v1 f2", "HSET h f1 v1 f2");
  parse_add(tc, MSG_REQ_REDIS_HSET, "h", "HSET h");
  parse_add_args(tc, MSG_REQ_REDIS_ZADD, "z", "1 m", "ZADD z 1 m");
  parse_add_args(tc, MSG_REQ_REDIS_LPUSH, "l", "a b", "LPUSH l a b");
  parse_add_args(tc, MSG_REQ_REDIS_EVAL, "ek", "return(1) 1 arg",
                 "EVAL return(1) 1 ek arg");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_LOAD, "", "SCRIPT LOAD return(1)");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_EXISTS, "",
            "script exists e0e1f9fabfc9d4800c877a703b823ac0578ff8db");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_FLUSH, "", "Script Flush");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_KILL, "", "SCRIPT KILL");
  parse_add(tc, MSG_REQ_REDIS_CLIENT_SETINFO, "",
            "CLIENT SETINFO lib-name test");
  parse_add(tc, MSG_REQ_REDIS_HELLO, "", "HELLO 3");
  parse_add(tc, MSG_REQ_REDIS_LPOP, "list", "LPOP list");
  parse_add(tc, MSG_REQ_REDIS_LPOP, "list", "LPOP list 2");
  parse_add(tc, MSG_REQ_REDIS_RPOP, "list", "rpop list 3");
  parse_add(tc, MSG_REQ_REDIS_SRANDMEMBER, "set", "SRANDMEMBER set");
  parse_add(tc, MSG_REQ_REDIS_SRANDMEMBER, "set", "SRANDMEMBER set -5");
  parse_add_args(tc, MSG_REQ_REDIS_SELECT, "", "0", "SELECT 0");
  parse_add_args(tc, MSG_REQ_REDIS_SELECT, "", "15", "select 15");
  parse_add_args(tc, MSG_HACK_SETTING_CONN_CONSISTENCY, "", "dc_Quorum",
                 "dyno_config:conn_consistency dc_Quorum");
  parse_add_args(tc, MSG_HACK_SETTING_CONN_CONSISTENCY, "",
                 "DC_EACH_SAFE_QUORUM",
                 "DYNO_CONFIG:CONN_CONSISTENCY DC_EACH_SAFE_QUORUM");
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
   * accepted before fd14eb7, the dyno_config ones with a level that isn't
   * one whole, or extra args, before the commit that added them (a bare
   * SCRIPT is answered for its arity, see below) */
  static const char *const rejects[] = {
      "SCRIPT GET foo",
      "SCRIPT DEBUG yes",
      "KILL",
      "LOAD return(1)",
      "FLUSH",
      "CLIENT LIST",
      "NOSUCHCOMMAND x",
      "dyno_config:conn_consistency DC_ONEX",
      "dyno_config:conn_consistency DC_ON",
      "dyno_config:conn_consistency bogus",
      "dyno_config:conn_consistency DC_ONE DC_ONE",
      "dyno_config:conn_consistency",
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
  /*
   * Keys too long to keep: longer than both test mbufs for redis, than
   * memcached allows (MEMCACHE_MAX_KEY_LENGTH) for memcache. From a client,
   * the parser skips them and flags the request, which goes on; requests
   * after it must parse as usual. From a peer, it rejects them.
   */
  tc = parse_case_new("redis long keys", DATA_REDIS, false);
  parse_add(tc, MSG_REQ_REDIS_GET, "foo", "GET foo");
  dn_snprintf(long_words, sizeof(long_words), "GET %s", huge);
  parse_add_too_long(tc, MSG_REQ_REDIS_GET, "", long_words);
  dn_snprintf(long_words, sizeof(long_words), "SET %s %s", huge, big);
  parse_add_too_long(tc, MSG_REQ_REDIS_SET, "", long_words);
  dn_snprintf(long_words, sizeof(long_words), "MGET k1 %s k3", huge);
  parse_add_too_long(tc, MSG_REQ_REDIS_MGET, "k1 k3", long_words);
  dn_snprintf(long_words, sizeof(long_words), "MSET k1 v1 %s v2", huge);
  parse_add_too_long(tc, MSG_REQ_REDIS_MSET, "k1", long_words);
  parse_add(tc, MSG_REQ_REDIS_GET, "last", "GET last");

  /*
   * Commands with the wrong number of args: from a client, the parser reads
   * past the args left (one larger than the smallest mbuf), flags the
   * command, and the requests after it parse as usual; from a peer, it
   * rejects it. The ones that may have no args parse as usual.
   */
  tc = parse_case_new("redis wrong arity", DATA_REDIS, false);
  parse_add(tc, MSG_REQ_REDIS_GET, "foo", "GET foo");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SMEMBERS, "", "SMEMBERS");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_GET, "", "get");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_MGET, "", "MGET");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_EVAL, "", "EVAL");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SCRIPT_LOAD, "", "SCRIPT LOAD");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SCRIPT, "", "SCRIPT");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_HGET, "h", "HGET h");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_GET, "k", "GET k x");
  dn_snprintf(words, sizeof(words), "GET k %s y", big);
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_GET, "k", words);
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_EXPIRE, "k", "EXPIRE k");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SETEX, "k", "SETEX k 10");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_LINSERT, "l", "LINSERT l a b c d");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_MSET, "k1", "MSET k1 v1 k2");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_PING, "", "PING a b");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_CLIENT_SETINFO, "",
                        "CLIENT SETINFO lib-name");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SELECT, "", "SELECT");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_LPOP, "", "LPOP");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SRANDMEMBER, "", "SRANDMEMBER");
  parse_add_wrong_arity(tc, MSG_REQ_REDIS_SELECT, "", "SELECT 0 1");
  parse_add(tc, MSG_REQ_REDIS_PING, "", "PING");
  parse_add(tc, MSG_REQ_REDIS_PING, "", "PING hi");
  parse_add(tc, MSG_REQ_REDIS_INFO, "", "INFO");
  parse_add(tc, MSG_REQ_REDIS_SCRIPT_FLUSH, "", "SCRIPT FLUSH");
  parse_add(tc, MSG_REQ_REDIS_GET, "last", "GET last");

  static const char *dnode_arity[] = {"SMEMBERS", "GET k x", "HGET h"};
  for (size_t i = 0; i < NELEMS(dnode_arity); i++) {
    tc = parse_case_new("dnode wrong arity", DATA_REDIS, true);
    parse_add(tc, MSG_REQ_REDIS_SET, "foo1", "set foo1 bar1");
    parse_reject(tc, dnode_arity[i]);
  }

  tc = parse_case_new("dnode long key", DATA_REDIS, true);
  parse_add(tc, MSG_REQ_REDIS_SET, "foo1", "set foo1 bar1");
  dn_snprintf(long_words, sizeof(long_words), "GET %s", huge);
  parse_reject(tc, long_words);

  tc = parse_case_new("memcache long keys", DATA_MEMCACHE, false);
  parse_add(tc, MSG_REQ_MC_GET, "foo", "get foo\r\n");
  dn_snprintf(words, sizeof(words), "get %.*s\r\n", 251, big);
  parse_add_too_long(tc, MSG_REQ_MC_GET, "", words);
  dn_snprintf(words, sizeof(words), "get k1 %.*s k2\r\n", 300, big);
  parse_add_too_long(tc, MSG_REQ_MC_GET, "k1 k2", words);
  dn_snprintf(long_words, sizeof(long_words), "set %s 0 0 5\r\nhello\r\n",
              huge);
  parse_add_too_long(tc, MSG_REQ_MC_SET, "", long_words);
  dn_snprintf(words, sizeof(words), "set %.*s 0 0 5 noreply\r\nhello\r\n",
              251, big);
  parse_add_too_long(tc, MSG_REQ_MC_SET, "", words);
  dn_snprintf(words, sizeof(words), "delete %.*s\r\n", 251, big);
  parse_add_too_long(tc, MSG_REQ_MC_DELETE, "", words);
  /* the longest memcached allows */
  dn_snprintf(words, sizeof(words), "get %.*s\r\n", 250, big);
  dn_snprintf(rsp, sizeof(rsp), "%.*s", 250, big);
  parse_add(tc, MSG_REQ_MC_GET, rsp, words);
  parse_add(tc, MSG_REQ_MC_GET, "last", "get last\r\n");

  tc = parse_case_new("memcache dnode long key", DATA_MEMCACHE, true);
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
  /*
   * The first key is cut at the end of the smallest mbuf (456 bytes), then
   * the second one, after its hash tag.
   */
  dn_snprintf(words, sizeof(words), "MGET %.*s {t}abcdefghijklmnop", 438, big);
  dn_snprintf(rsp, sizeof(rsp), "%.*s {t}abcdefghijklmnop(t)", 438, big);
  parse_add(tc, MSG_REQ_REDIS_MGET, rsp, words);
  dn_snprintf(words, sizeof(words), "MGET %.*s {t}abcdefghijklmnop", 420, big);
  dn_snprintf(rsp, sizeof(rsp), "%.*s {t}abcdefghijklmnop(t)", 420, big);
  parse_add(tc, MSG_REQ_REDIS_MGET, rsp, words);

  /* the number of keys is the last byte of the smallest mbuf */
  tc = parse_case_new("redis eval cut", DATA_REDIS, false);
  dn_snprintf(words, sizeof(words), "EVAL %.*s 1 ek arg", 429, big);
  parse_add(tc, MSG_REQ_REDIS_EVAL, "ek", words);

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
  /* the reply of a read repair script: status, timestamp (a ZSCORE), value */
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "E 1700000000000 bar",
                "*3\r\n$1\r\nE\r\n$13\r\n1700000000000\r\n$3\r\nbar\r\n");
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, "X :0 :0",
                "*3\r\n$1\r\nX\r\n:0\r\n:0\r\n");
  dn_snprintf(rsp, sizeof(rsp),
              "*3\r\n$1\r\nE\r\n$13\r\n1700000000000\r\n$%zu\r\n%s\r\n",
              strlen(big), big);
  dn_snprintf(args, sizeof(args), "E 1700000000000 %s", big);
  parse_add_rsp(tc, MSG_RSP_REDIS_MULTIBULK, args, rsp);
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

/* Parse one whole message from len bytes of data, as msg_parse_data() does. */
static struct msg *parse_msg(struct conn *conn, bool request, const char *data,
                             size_t len) {
  struct msg *msg = msg_get(conn, request, __FUNCTION__);

  if (msg == NULL) return NULL;
  if (msg_parse_data(ctx, msg, (const uint8_t *)data, len) != DN_OK) {
    log_error("failed to parse '%.40s...'", data);
    msg_put(msg);
    return NULL;
  }
  return msg;
}

static struct msg *repair_parse(struct conn *conn, bool request,
                                const char *data) {
  return parse_msg(conn, request, data, strlen(data));
}

/* Copy what would be sent of msg, from all its mbufs, to out. */
static const char *repair_data(struct msg *msg, char *out, size_t size) {
  struct mbuf *mbuf;
  size_t len = 0;

  STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
    size_t n = MIN(mbuf_length(mbuf), size - 1 - len);
    memcpy(out + len, mbuf->pos, n);
    len += n;
  }
  out[len] = '\0';
  return out;
}

/*
 * A GET with read repairs: the replies of its script from two replicas, the
 * first older than the second, or the same age (no repair). Values are larger
 * than the mbufs, so the replies, the repair and what the client gets are
 * split between them.
 */
struct repair_case {
  const char *name;
  const char *rsp[2];
  bool repair;
  const char *repair_has[2]; /* in the repair query, if any */
  const char *client;        /* what the client gets */
};

/* For the error messages: the key, and the pool's hash tag if it has one. */
static const char *repair_mode(const char *key) {
  static char mode[128];
  dn_snprintf(mode, sizeof(mode), "mbuf %zu, key %s%s%.*s", mbuf_data_size(),
              key, ctx->pool.hash_tag.len > 0 ? ", hash tag " : "",
              (int)ctx->pool.hash_tag.len, ctx->pool.hash_tag.data);
  return mode;
}

/*
 * Parse a request and rewrite it as a read repair script, as
 * rewrite_query_with_timestamp_md() does. Returns NULL if it isn't rewritten.
 */
static struct msg *repair_rewrite(struct conn *conn, const char *data) {
  struct msg *req = repair_parse(conn, true, data), *wreq = NULL;
  bool did_rewrite = false;

  if (req == NULL) return NULL;
  if (redis_rewrite_query_with_timestamp_md(req, ctx, &did_rewrite, &wreq) !=
          DN_OK ||
      !did_rewrite) {
    msg_put(req);
    return NULL;
  }
  wreq->orig_msg = req;
  wreq->orig_type = req->type;
  return wreq;
}

/* Whether msg has s in it, and if not, say so. */
static bool repair_has(struct msg *msg, const char *s, const char *what,
                       const char *name, const char *key) {
  static char buf[4096];

  if (strstr(repair_data(msg, buf, sizeof(buf)), s) != NULL) return true;
  log_error("%s (%s): %s '%.60s...' lacks '%.60s...'", name, repair_mode(key),
            what, buf, s);
  return false;
}

static rstatus_t repair_run(struct conn *conn, const struct repair_case *rc,
                            const char *key) {
  static char buf[4096];
  char get[256], has_key[256];
  struct msg *wreq, *repair = NULL;
  struct response_mgr rspmgr;
  rstatus_t status = DN_ERROR;

  memset(&rspmgr, 0, sizeof(rspmgr));
  dn_snprintf(get, sizeof(get), "*2\r\n$3\r\nGET\r\n$%zu\r\n%s\r\n",
              strlen(key), key);
  dn_snprintf(has_key, sizeof(has_key), "\r\n$%zu\r\n%s\r\n", strlen(key),
              key);
  wreq = repair_rewrite(conn, get);
  if (wreq == NULL) {
    log_error("%s (%s): GET not rewritten", rc->name, repair_mode(key));
    return DN_ERROR;
  }
  if (!repair_has(wreq, has_key, "script", rc->name, key)) {
    msg_put(wreq);
    return DN_ERROR;
  }

  rspmgr.msg = wreq;
  for (int i = 0; i < 2; i++) {
    rspmgr.responses[i] = repair_parse(conn, false, rc->rsp[i]);
    if (rspmgr.responses[i] == NULL) goto out;
    rspmgr.good_responses++;
  }

  if (redis_make_repair_query(ctx, &rspmgr, &repair) != DN_OK ||
      (repair != NULL) != rc->repair ||
      rspmgr.responses[0]->needs_repair != rc->repair ||
      rspmgr.responses[1]->needs_repair) {
    log_error("%s (%s): repair %s, replies needing it %d %d", rc->name,
              repair_mode(key), repair != NULL ? "made" : "not made",
              rspmgr.responses[0]->needs_repair,
              rspmgr.responses[1]->needs_repair);
    goto out;
  }
  if (repair != NULL && !repair_has(repair, has_key, "repair", rc->name, key)) {
    goto out;
  }
  for (int i = 0; repair != NULL && i < 2; i++) {
    if (rc->repair_has[i] != NULL &&
        !repair_has(repair, rc->repair_has[i], "repair", rc->name, key)) {
      goto out;
    }
  }
  if (strcmp(repair_data(rspmgr.responses[1], buf, sizeof(buf)), rc->client) !=
      0) {
    log_error("%s (%s): client gets '%.60s...', want '%.60s...'", rc->name,
              repair_mode(key), buf, rc->client);
    goto out;
  }
  status = DN_OK;

out:
  for (int i = 0; i < rspmgr.good_responses; i++) {
    msg_put(rspmgr.responses[i]);
  }
  if (repair != NULL) msg_put(repair);
  msg_put(wreq);
  return status;
}

/*
 * The script that clears the metadata of a field once all replicas deleted it,
 * for an HDEL of a field larger than the mbufs.
 */
static rstatus_t cleanup_run(struct conn *conn, const char *key,
                             const char *field) {
  static char hdel[1300], has_key[256], has_field[1100];
  struct msg *wreq, *cleanup = NULL;
  rstatus_t status = DN_ERROR;

  dn_snprintf(hdel, sizeof(hdel),
              "*3\r\n$4\r\nHDEL\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen(key),
              key, strlen(field), field);
  dn_snprintf(has_key, sizeof(has_key), "\r\n$%zu\r\n%s\r\n", strlen(key),
              key);
  dn_snprintf(has_field, sizeof(has_field), "\r\n$%zu\r\n%s\r\n",
              strlen(field), field);
  wreq = repair_rewrite(conn, hdel);
  if (wreq == NULL) {
    log_error("cleanup (%s): HDEL not rewritten", repair_mode(key));
    return DN_ERROR;
  }
  memset(&wreq->rspmgr, 0, sizeof(wreq->rspmgr));
  wreq->rspmgr.max_responses = 1;

  if (redis_clear_repair_md_for_key(ctx, wreq, &cleanup) != DN_OK) {
    log_error("cleanup (%s): no script", repair_mode(key));
    goto out;
  }
  if (repair_has(cleanup, has_key, "script", "cleanup", key) &&
      repair_has(cleanup, has_field, "script", "cleanup", key)) {
    status = DN_OK;
  }

out:
  if (cleanup != NULL) msg_put(cleanup);
  msg_put(wreq);
  return status;
}

/*
 * A write rewritten as a read repair script: what the script must have, or
 * NULL if it mustn't be rewritten (Redis rejects it).
 */
struct rewrite_case {
  const char *words;
  const char *has[2];
};

static rstatus_t rewrite_run(struct conn *conn, const struct rewrite_case *wc) {
  static char data[512];
  struct msg *wreq;
  rstatus_t status = DN_OK;
  char buf[256], *word, *save;
  int nwords = 0, n = 0;

  /* the words as a RESP array */
  dn_snprintf(buf, sizeof(buf), "%s", wc->words);
  for (const char *w = wc->words; *w != '\0'; w++) {
    if ((w == wc->words || w[-1] == ' ') && *w != ' ') nwords++;
  }
  n = dn_snprintf(data, sizeof(data), "*%d\r\n", nwords);
  for (word = strtok_r(buf, " ", &save); word != NULL;
       word = strtok_r(NULL, " ", &save)) {
    n += dn_snprintf(data + n, sizeof(data) - (size_t)n, "$%zu\r\n%s\r\n",
                     strlen(word), word);
  }

  wreq = repair_rewrite(conn, data);
  if ((wreq != NULL) != (wc->has[0] != NULL)) {
    log_error("%s (%s): %s", wc->words, repair_mode("-"),
              wreq != NULL ? "rewritten" : "not rewritten");
    status = DN_ERROR;
  }
  for (int i = 0; wreq != NULL && i < 2 && status == DN_OK; i++) {
    if (wc->has[i] != NULL &&
        !repair_has(wreq, wc->has[i], "script", wc->words, "-")) {
      status = DN_ERROR;
    }
  }
  if (wreq != NULL) msg_put(wreq);
  return status;
}

static rstatus_t test_read_repair(struct conn *conn) {
  static const size_t mbuf_sizes[] = {MBUF_MIN_SIZE, TEST_MBUF_SIZE};
  static char big[1000 + 1], rsp_e1[1100], rsp_e2[1100], rsp_r[1100],
      has_big[1100], client_big[1100];
  size_t nruns = 0, nfailed = 0;

  print_banner("READ REPAIR");

  for (size_t i = 0; i < sizeof(big) - 1; i++) {
    big[i] = (char)('a' + i % 26);
  }
  big[sizeof(big) - 1] = '\0';
  dn_snprintf(rsp_e1, sizeof(rsp_e1),
              "*3\r\n$1\r\nE\r\n$13\r\n1700000000000\r\n$3\r\nold\r\n");
  dn_snprintf(rsp_e2, sizeof(rsp_e2),
              "*3\r\n$1\r\nE\r\n$13\r\n1700000000001\r\n$%zu\r\n%s\r\n",
              strlen(big), big);
  dn_snprintf(rsp_r, sizeof(rsp_r),
              "*3\r\n$1\r\nR\r\n$13\r\n1700000000000\r\n$%zu\r\n%s\r\n",
              strlen(big), big);
  dn_snprintf(has_big, sizeof(has_big), "\r\n$%zu\r\n%s\r\n", strlen(big),
              big);
  dn_snprintf(client_big, sizeof(client_big), "$%zu\r\n%s\r\n", strlen(big),
              big);

  const struct repair_case cases[] = {
      {"newer value", {rsp_e1, rsp_e2}, true,
       {has_big, "\r\n$13\r\n1700000000001\r\n"}, client_big},
      {"newer removal",
       {rsp_r, "*3\r\n$1\r\nR\r\n$13\r\n1700000000001\r\n$-1\r\n"}, true,
       {"\r\n$3\r\nDEL\r\n", "\r\n$13\r\n1700000000001\r\n"}, "$-1\r\n"},
      {"same age", {rsp_r, rsp_r}, false, {NULL, NULL}, client_big},
      {"missing everywhere",
       {"*3\r\n$1\r\nX\r\n:0\r\n$-1\r\n", "*3\r\n$1\r\nX\r\n:0\r\n$-1\r\n"},
       false, {NULL, NULL}, "$-1\r\n"},
      {"not a script reply", {"-ERR x\r\n", "-ERR x\r\n"}, false, {NULL, NULL},
       "-ERR x\r\n"},
  };

  /*
   * ZADD: its options come before the first score; the script gets the number
   * of options, the number of members and the timestamp, then the options, then
   * each member and its score.
   */
  static const struct rewrite_case rewrites[] = {
      {"ZADD z 1 m",
       {"$4\r\nZADD\r\n$1\r\n0\r\n$1\r\n1\r\n$", "\r\n$1\r\nm\r\n$1\r\n1\r\n"}},
      {"ZADD z NX 1 m",
       {"$4\r\nZADD\r\n$1\r\n1\r\n$1\r\n1\r\n$",
        "\r\n$2\r\nNX\r\n$1\r\nm\r\n$1\r\n1\r\n"}},
      {"ZADD z xx ch 1 a 2 b",
       {"$4\r\nZADD\r\n$1\r\n2\r\n$1\r\n2\r\n$",
        "\r\n$2\r\nxx\r\n$2\r\nch\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"}},
      {"ZADD z XX GT CH INCR 5 m",
       {"$4\r\nZADD\r\n$1\r\n4\r\n$1\r\n1\r\n$",
        "\r\n$2\r\nXX\r\n$2\r\nGT\r\n$2\r\nCH\r\n$4\r\nINCR\r\n$1\r\nm\r\n$1\r\n5\r\n"}},
      /* a member named like an option */
      {"ZADD z 1 nx",
       {"$4\r\nZADD\r\n$1\r\n0\r\n$1\r\n1\r\n$", "\r\n$2\r\nnx\r\n$1\r\n1\r\n"}},
      /* rejected by Redis, so sent to it as they are */
      {"ZADD z NX XX 1 m", {NULL, NULL}},
      {"ZADD z GT LT 1 m", {NULL, NULL}},
      {"ZADD z NX GT 1 m", {NULL, NULL}},
      {"ZADD z NX", {NULL, NULL}},
      {"ZADD z 1 m 2", {NULL, NULL}},
      /*
       * HSET and HMSET: the script gets the number of fields and the
       * timestamp, then each field and its value.
       */
      {"HSET h f v",
       {"$4\r\nHSET\r\n$1\r\n1\r\n$", "\r\n$1\r\nf\r\n$1\r\nv\r\n"}},
      {"HSET h f1 v1 f2 v2",
       {"$4\r\nHSET\r\n$1\r\n2\r\n$",
        "\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"}},
      {"HMSET h f1 v1 f2 v2",
       {"$5\r\nHMSET\r\n$1\r\n2\r\n$",
        "\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n"}},
      /* rejected by Redis, so sent to it as they are */
      {"HSET h f1 v1 f2", {NULL, NULL}},
      {"HSET h", {NULL, NULL}},
      {"HMSET h f1", {NULL, NULL}},
  };
  /* keys with and without a hash tag in them, with and without one configured */
  static const struct {
    const char *hash_tag, *key;
  } keys[] = {{NULL, "k"}, {"{}", "k"}, {"{}", "user:{42}:name"}};

  g_data_store = DATA_REDIS;
  conn->dyn_mode = 0;
  g_read_repairs_enabled = true;
  g_read_consistency = g_write_consistency = DC_QUORUM;
  for (size_t m = 0; m < NELEMS(mbuf_sizes); m++) {
    mbuf_deinit();
    mbuf_init(mbuf_sizes[m]);
    for (size_t k = 0; k < NELEMS(keys); k++) {
      string_init(&ctx->pool.hash_tag);
      if (keys[k].hash_tag != NULL) {
        string_set_raw(&ctx->pool.hash_tag, keys[k].hash_tag);
      }
      for (size_t c = 0; c < NELEMS(cases); c++) {
        nruns++;
        if (repair_run(conn, &cases[c], keys[k].key) != DN_OK) {
          nfailed++;
        }
      }
      nruns++;
      if (cleanup_run(conn, keys[k].key, big) != DN_OK) {
        nfailed++;
      }
    }
    string_init(&ctx->pool.hash_tag);
    for (size_t w = 0; w < NELEMS(rewrites); w++) {
      nruns++;
      if (rewrite_run(conn, &rewrites[w]) != DN_OK) {
        nfailed++;
      }
    }
  }
  mbuf_deinit();
  mbuf_init(TEST_MBUF_SIZE);
  g_read_repairs_enabled = false;
  g_read_consistency = g_write_consistency = DC_ONE;

  if (nfailed != 0) {
    log_error("%zu of %zu read repair runs failed", nfailed, nruns);
    return DN_ERROR;
  }
  loga("%zu read repair runs passed", nruns);
  return DN_OK;
}

/*
 * SMEMBERS key -> SORT key ALPHA with DC_SAFE_QUORUM, for a binary key and for
 * the longest key a client may send, whose SORT doesn't fit in one mbuf.
 */
static rstatus_t smembers_run(struct conn *conn, const char *key,
                              size_t keylen) {
  static char req[TEST_MBUF_SIZE + 100], want[TEST_MBUF_SIZE + 100],
      got[TEST_MBUF_SIZE + 100];
  struct msg *msg, *wreq = NULL;
  bool did_rewrite = false;
  rstatus_t status = DN_ERROR;
  int n;

  n = dn_snprintf(req, sizeof(req), "*2\r\n$8\r\nSMEMBERS\r\n$%zu\r\n", keylen);
  memcpy(req + n, key, keylen);
  memcpy(req + n + keylen, "\r\n", 2);
  msg = parse_msg(conn, true, req, (size_t)n + keylen + 2);
  if (msg == NULL) {
    log_error("SMEMBERS (mbuf %zu, key of %zu): not parsed", mbuf_data_size(),
              keylen);
    return DN_ERROR;
  }

  n = dn_snprintf(want, sizeof(want), "*3\r\n$4\r\nsort\r\n$%zu\r\n", keylen);
  memcpy(want + n, key, keylen);
  memcpy(want + n + keylen, "\r\n$5\r\nalpha\r\n", 13);
  size_t wantlen = (size_t)n + keylen + 13, gotlen = 0;

  if (redis_rewrite_query(msg, ctx, &did_rewrite, &wreq) != DN_OK ||
      !did_rewrite) {
    log_error("SMEMBERS (mbuf %zu, key of %zu): not rewritten",
              mbuf_data_size(), keylen);
    goto out;
  }
  struct mbuf *mbuf;
  STAILQ_FOREACH(mbuf, &wreq->mhdr, next) {
    size_t len = mbuf_length(mbuf);
    if (gotlen + len > sizeof(got)) break;
    memcpy(got + gotlen, mbuf->pos, len);
    gotlen += len;
  }
  uint32_t wkeylen;
  uint8_t *wkey = msg_get_full_key(wreq, 0, &wkeylen);
  if (wreq->type != MSG_REQ_REDIS_SORT || gotlen != wantlen ||
      memcmp(got, want, wantlen) != 0 || wreq->mlen != wantlen ||
      wkey == NULL || wkeylen != keylen || memcmp(wkey, key, keylen) != 0) {
    log_error("SMEMBERS (mbuf %zu, key of %zu): rewritten as type %d, '%.*s'",
              mbuf_data_size(), keylen, wreq->type, (int)MIN(gotlen, 60), got);
    goto out;
  }
  status = DN_OK;

out:
  if (wreq != NULL) msg_put(wreq);
  msg_put(msg);
  return status;
}

/* Take every msg off the free list, so the next msg_get() allocates. */
static rstatus_t take_free_msgs(struct conn *conn, struct msg ***taken,
                                size_t *ntaken) {
  size_t n = msg_free_queue_size();
  struct msg **all = dn_realloc(*taken, (*ntaken + n) * sizeof(*all));

  if (all == NULL && *ntaken + n > 0) return DN_ENOMEM;
  *taken = all;
  for (size_t i = 0; i < n; i++) {
    all[(*ntaken)++] = msg_get(conn, true, __FUNCTION__);
  }
  return DN_OK;
}

/*
 * Replicas that disagree under DC_SAFE_QUORUM: the client gets a "no quorum"
 * error, also with max_msgs reached and the free list empty. The error used
 * to count against max_msgs, and the client code dereferenced the NULL.
 */
static rstatus_t no_quorum_run(struct conn *conn, data_store_t store) {
  static char buf[256];
  const char *name, *get, *rsp[2];
  struct msg *(*reconcile)(struct response_mgr *);
  struct response_mgr rspmgr;
  struct msg **taken = NULL, *err;
  size_t ntaken = 0;
  rstatus_t status = DN_ERROR;

  if (store == DATA_REDIS) {
    name = "redis";
    get = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
    rsp[0] = "$3\r\none\r\n";
    rsp[1] = "$3\r\ntwo\r\n";
    reconcile = redis_reconcile_responses;
  } else {
    name = "memcache";
    get = "get k\r\n";
    rsp[0] = "VALUE k 0 3\r\none\r\nEND\r\n";
    rsp[1] = "VALUE k 0 3\r\ntwo\r\nEND\r\n";
    reconcile = memcache_reconcile_responses;
  }
  g_data_store = store;
  conn->dyn_mode = 0;
  memset(&rspmgr, 0, sizeof(rspmgr));
  rspmgr.msg = repair_parse(conn, true, get);
  if (rspmgr.msg == NULL) return DN_ERROR;
  rspmgr.msg->consistency = DC_SAFE_QUORUM;
  for (int i = 0; i < 2; i++) {
    rspmgr.responses[i] = repair_parse(conn, false, rsp[i]);
    if (rspmgr.responses[i] == NULL) goto out;
    rspmgr.good_responses++;
  }
  rspmgr.quorum_responses = 2;

  if (take_free_msgs(conn, &taken, &ntaken) != DN_OK) goto out;
  msg_init(msg_alloc_msgs());

  err = reconcile(&rspmgr);
  if (err == NULL) {
    log_error("%s: no error reply without a free msg", name);
  } else if (!err->is_error ||
             err->dyn_error_code != DYNOMITE_NO_QUORUM_ACHIEVED ||
             strstr(repair_data(err, buf, sizeof(buf)),
                    "Failed to achieve Quorum") == NULL) {
    log_error("%s: wrong error reply '%s'", name, buf);
  } else {
    status = DN_OK;
  }

out:
  /* restore the limit with an empty free list, msg_init() drops it */
  if (take_free_msgs(conn, &taken, &ntaken) != DN_OK) status = DN_ERROR;
  msg_init(TEST_ALLOC_MSGS_MAX);
  for (size_t i = 0; i < ntaken; i++) msg_put(taken[i]);
  dn_free(taken);
  rspmgr_free_other_responses(&rspmgr, NULL);
  msg_put(rspmgr.msg);
  return status;
}

static rstatus_t test_no_quorum(struct conn *conn) {
  static const data_store_t stores[] = {DATA_REDIS, DATA_MEMCACHE};
  size_t nfailed = 0;

  print_banner("NO QUORUM");
  for (size_t i = 0; i < sizeof(stores) / sizeof(stores[0]); i++) {
    if (no_quorum_run(conn, stores[i]) != DN_OK) nfailed++;
  }
  loga("no quorum runs: %zu, failed: %zu", sizeof(stores) / sizeof(stores[0]),
       nfailed);
  return nfailed == 0 ? DN_OK : DN_ERROR;
}

/*
 * At max_msgs with the free list empty, only a new client request is refused,
 * with ENOMEM: replies from the datastore and from peers, and requests from
 * peers, are still read. A refused reply used to stop reading the datastore
 * connection, with a stale errno as its error.
 */
static rstatus_t test_msg_limit(struct conn *conn) {
  static const struct {
    const char *name;
    connection_type_t type;
    bool request;
    bool refused;
  } cases[] = {
      {"client request", CONN_CLIENT, true, true},
      {"datastore reply", CONN_SERVER, false, false},
      {"peer reply", CONN_DNODE_PEER_SERVER, false, false},
      {"peer request", CONN_DNODE_PEER_CLIENT, true, false},
  };
  connection_type_t type = conn->type;
  struct msg **taken = NULL;
  size_t ntaken = 0, nfailed = 0;

  print_banner("MSG LIMIT");
  g_data_store = DATA_REDIS;
  if (take_free_msgs(conn, &taken, &ntaken) != DN_OK) return DN_ERROR;
  msg_init(msg_alloc_msgs());

  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
    struct msg *msg;

    conn->type = cases[i].type;
    conn->err = 0;
    msg = cases[i].request ? req_get(conn) : rsp_get(conn);
    if (cases[i].refused ? msg != NULL || conn->err != ENOMEM : msg == NULL) {
      log_error("%s: %s, err '%s'", cases[i].name,
                msg != NULL ? "allocated" : "refused", strerror(conn->err));
      nfailed++;
    }
    if (msg != NULL) {
      /* keep it, so the free list stays empty for the next case */
      struct msg **all = dn_realloc(taken, (ntaken + 1) * sizeof(*all));
      if (all == NULL) {
        msg_put(msg);
        nfailed++;
        break;
      }
      taken = all;
      taken[ntaken++] = msg;
    }
  }
  conn->type = type;
  conn->err = 0;

  if (take_free_msgs(conn, &taken, &ntaken) != DN_OK) nfailed++;
  msg_init(TEST_ALLOC_MSGS_MAX);
  for (size_t i = 0; i < ntaken; i++) msg_put(taken[i]);
  dn_free(taken);

  loga("msg limit runs: %zu, failed: %zu", sizeof(cases) / sizeof(cases[0]),
       nfailed);
  return nfailed == 0 ? DN_OK : DN_ERROR;
}

static rstatus_t test_rewrite(struct conn *conn) {
  static const size_t mbuf_sizes[] = {MBUF_MIN_SIZE, TEST_MBUF_SIZE};
  static char longest[TEST_MBUF_SIZE];
  size_t nruns = 0, nfailed = 0;

  print_banner("REWRITE");

  g_data_store = DATA_REDIS;
  conn->dyn_mode = 0;
  conn->read_consistency = DC_SAFE_QUORUM;
  for (size_t m = 0; m < NELEMS(mbuf_sizes); m++) {
    mbuf_deinit();
    mbuf_init(mbuf_sizes[m]);
    /* the longest key a client may send fills an mbuf but one byte */
    size_t longest_len = mbuf_data_size() - 1;
    for (size_t i = 0; i < longest_len; i++) {
      longest[i] = (char)('a' + i % 26);
    }
    const struct {
      const char *key;
      size_t len;
    } keys[] = {{"s", 1}, {"a\0b", 3}, {"\0", 1}, {longest, longest_len}};
    for (int r = 0; r < 2; r++) {
      g_read_repairs_enabled = r == 1;
      for (size_t k = 0; k < NELEMS(keys); k++) {
        nruns++;
        if (smembers_run(conn, keys[k].key, keys[k].len) != DN_OK) {
          nfailed++;
        }
      }
    }
  }
  mbuf_deinit();
  mbuf_init(TEST_MBUF_SIZE);
  g_read_repairs_enabled = false;
  conn->read_consistency = DC_ONE;

  if (nfailed != 0) {
    log_error("%zu of %zu rewrite runs failed", nfailed, nruns);
    return DN_ERROR;
  }
  loga("%zu rewrite runs passed", nruns);
  return DN_OK;
}

/*
 * msg_append_format() with each kind of specifier the read repair scripts use,
 * after filler that makes the formatted string end just before, exactly at or
 * just past the end of an mbuf. The text must come out whole, and no mbuf may
 * be written past its end: the struct mbuf there starts with its magic.
 */
static rstatus_t test_append_format(struct conn *conn) {
  static char filler[MBUF_MIN_SIZE], big[1000 + 1], want[2048], got[2048];
  size_t nruns = 0, nfailed = 0;

  print_banner("APPEND FORMAT");

  memset(filler, 'x', sizeof(filler));
  for (size_t i = 0; i < sizeof(big) - 1; i++) {
    big[i] = (char)('a' + i % 26);
  }
  big[sizeof(big) - 1] = '\0';

  mbuf_deinit();
  mbuf_init(MBUF_MIN_SIZE);
  int data_size = (int)mbuf_data_size();
  for (int c = 0; c < 5; c++) {
    for (int delta = -2; delta <= 2; delta++) {
      int n;
      switch (c) {
        case 0:
          n = dn_snprintf(want, sizeof(want), "*%d\r\n", 12345);
          break;
        case 1:
          n = dn_snprintf(want, sizeof(want), "$%d\r\n%llu\r\n", 13,
                          1700000000000ULL);
          break;
        case 2:
          n = dn_snprintf(want, sizeof(want), "$%d\r\n%.*s\r\n", 5, 5,
                          "HMSET");
          break;
        case 3:
          n = dn_snprintf(want, sizeof(want), "%s", "local key = KEYS[1]\n");
          break;
        default: /* longer than an mbuf */
          n = dn_snprintf(want, sizeof(want), "%s", big);
          break;
      }
      int fill = (data_size - n % data_size + delta + data_size) % data_size;
      struct msg *msg = msg_get(conn, true, __FUNCTION__);
      rstatus_t status = DN_ERROR;

      nruns++;
      if (msg == NULL) {
        nfailed++;
        continue;
      }
      if (fill > 0 && msg_append_format(msg, "%.*s", fill, filler) != DN_OK) {
        goto out;
      }
      switch (c) {
        case 0:
          status = msg_append_format(msg, "*%d\r\n", 12345);
          break;
        case 1:
          status = msg_append_format(msg, "$%d\r\n%llu\r\n", 13,
                                     1700000000000ULL);
          break;
        case 2:
          status = msg_append_format(msg, "$%d\r\n%.*s\r\n", 5, 5, "HMSET");
          break;
        case 3:
          status = msg_append_format(msg, "%s", "local key = KEYS[1]\n");
          break;
        default:
          status = msg_append_format(msg, "%s", big);
          break;
      }
      if (status != DN_OK) goto out;
      status = DN_ERROR;

      struct mbuf *mbuf;
      STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
        if (mbuf->magic != MBUF_MAGIC || mbuf->last > mbuf->end) {
          log_error("case %d, fill %d: mbuf magic %#x, %td bytes past its end",
                    c, fill, mbuf->magic, mbuf->last - mbuf->end);
          goto out;
        }
      }
      repair_data(msg, got, sizeof(got));
      if (strncmp(got, filler, (size_t)fill) != 0 ||
          strcmp(got + fill, want) != 0 ||
          msg->mlen != (uint32_t)(fill + n)) {
        log_error("case %d, fill %d: got '%.40s...' (mlen %u), want '%.40s...'",
                  c, fill, got + fill, msg->mlen, want);
        goto out;
      }
      status = DN_OK;

    out:
      if (status != DN_OK) nfailed++;
      msg_put(msg);
    }
  }
  mbuf_deinit();
  mbuf_init(TEST_MBUF_SIZE);

  if (nfailed != 0) {
    log_error("%zu of %zu append format runs failed", nfailed, nruns);
    return DN_ERROR;
  }
  loga("%zu append format runs passed", nruns);
  return DN_OK;
}

/*
 * A configuration test case: conf/redis_single.yml plus extra pool lines.
 * A valid case also checks one int field of the parsed conf_pool.
 */
struct conf_case {
  const char *lines;
  bool valid;
  const char *field;
  size_t offset;
  int want;
};

#define CONF_VALID(lines, field, want) \
  { lines, true, #field, offsetof(struct conf_pool, field), want }
#define CONF_INVALID(lines) \
  { lines, false, NULL, 0, 0 }

static const struct conf_case conf_cases[] = {
    /* unset directives get their defaults */
    CONF_VALID("", timeout, 5000),
    CONF_VALID("", backlog, 512),
    CONF_VALID("", enable_gossip, false),
    CONF_VALID("", read_repairs_enabled, false),
    CONF_VALID("", server_retry_timeout_ms, 10000),
    CONF_VALID("", gos_interval, 30000),
    CONF_VALID("", stats_interval, 30000),
    CONF_VALID("", conn_msg_rate, 50000),
    CONF_VALID("", datastore_connections, 1),
    CONF_VALID("", mbuf_size, CONF_UNSET_NUM),
    CONF_VALID("", alloc_msgs_max, CONF_UNSET_NUM),
    /* explicit false and 0 are kept, not taken for unset */
    CONF_VALID("  enable_gossip: false\n", enable_gossip, false),
    CONF_VALID("  read_repairs_enabled: true\n", read_repairs_enabled, true),
    CONF_VALID("  server_retry_timeout: 0\n", server_retry_timeout_ms, 0),
    CONF_VALID("  data_store: 1\n", data_store, DATA_MEMCACHE),
    /* limits */
    CONF_VALID("  timeout: 1\n", timeout, 1),
    CONF_VALID("  timeout: 2147483647\n", timeout, INT_MAX),
    CONF_VALID("  datastore_connections: 255\n", datastore_connections, 255),
    CONF_VALID("  local_peer_connections: 255\n", local_peer_connections, 255),
    CONF_VALID("  remote_peer_connections: 1\n", remote_peer_connections, 1),
    CONF_VALID("  mbuf_size: 512\n", mbuf_size, 512),
    CONF_VALID("  max_msgs: 1000000\n", alloc_msgs_max, 1000000),
    CONF_INVALID("  timeout: 0\n"),
    CONF_INVALID("  timeout: 2147483648\n"),
    CONF_INVALID("  timeout: -1\n"),
    CONF_INVALID("  backlog: 0\n"),
    CONF_INVALID("  gos_interval: 0\n"),
    CONF_INVALID("  stats_interval: 0\n"),
    CONF_INVALID("  conn_msg_rate: 0\n"),
    CONF_INVALID("  data_store: 2\n"),
    CONF_INVALID("  datastore_connections: 0\n"),
    CONF_INVALID("  datastore_connections: 256\n"),
    CONF_INVALID("  local_peer_connections: 0\n"),
    CONF_INVALID("  remote_peer_connections: 256\n"),
    CONF_INVALID("  mbuf_size: 0\n"),
    CONF_INVALID("  mbuf_size: 520\n"),
    CONF_INVALID("  max_msgs: 0\n"),
    CONF_INVALID("  read_repairs_enabled: maybe\n"),
    /* deprecated directives are accepted and ignored */
    CONF_VALID("  auto_eject_hosts: true\n  server_failure_limit: 3\n"
               "  preconnect: false\n  secure_server_option: none\n"
               "  pem_key_file: /nonexistent.pem\n",
               timeout, 5000),
    /* peer encryption was removed: refuse configs that ask for it */
    CONF_INVALID("  secure_server_option: datacenter\n"),
    CONF_INVALID("  secure_server_option: rack\n"),
    CONF_INVALID("  secure_server_option: all\n"),
    CONF_INVALID("  secure_server_option: nonesuch\n"),
    CONF_INVALID("  secure_server_option: None\n"),
    /* duplicates are caught for false and 0 too */
    CONF_INVALID("  enable_gossip: false\n  enable_gossip: true\n"),
    CONF_INVALID("  server_retry_timeout: 0\n  server_retry_timeout: 5\n"),
    /* only one datastore server, in the same list or a repeated key */
    CONF_INVALID("  - 127.0.0.1:22123:1\n"),
    CONF_INVALID("  - 127.0.0.1:22123:1\n  timeout: 1\n"),
    CONF_INVALID("  timeout: 1\n  servers:\n  - 127.0.0.1:22123:1\n"),
    /* tokens are 0 to 4294967295; the base's tokens are checked the same way */
    CONF_VALID("  dyn_seeds:\n  - 127.0.0.2:8101:r1:dc1:0\n"
               "  - 127.0.0.3:8101:r1:dc1:4294967295\n",
               timeout, 5000),
    CONF_INVALID("  dyn_seeds:\n  - 127.0.0.2:8101:r1:dc1:-5\n"),
    CONF_INVALID("  dyn_seeds:\n  - 127.0.0.2:8101:r1:dc1:4294967296\n"),
    CONF_INVALID("  dyn_seeds:\n  - 127.0.0.2:8101:r1:dc1:99999999999\n"),
    CONF_INVALID("  dyn_seeds:\n  - 127.0.0.2:8101:r1:dc1:abc\n"),
    CONF_INVALID("  dyn_seeds:\n  - 127.0.0.2:8101:r1:dc1:1,\n"),
    /* peer_tls needs the three files; off, they are ignored */
    CONF_VALID("", peer_tls, false),
    CONF_VALID("  peer_tls: false\n  peer_tls_cert: /nonexistent/node.pem\n",
               peer_tls, false),
    CONF_VALID("  peer_tls: true\n  peer_tls_cert: /nonexistent/node.pem\n"
               "  peer_tls_key: /nonexistent/node.key\n"
               "  peer_tls_ca: /nonexistent/ca.pem\n",
               peer_tls, true),
    CONF_INVALID("  peer_tls: true\n"),
    CONF_INVALID("  peer_tls: true\n  peer_tls_key: /nonexistent/node.key\n"
                 "  peer_tls_ca: /nonexistent/ca.pem\n"),
    CONF_INVALID("  peer_tls: true\n  peer_tls_cert: /nonexistent/node.pem\n"
                 "  peer_tls_ca: /nonexistent/ca.pem\n"),
    CONF_INVALID("  peer_tls: true\n  peer_tls_cert: /nonexistent/node.pem\n"
                 "  peer_tls_key: /nonexistent/node.key\n"),
    CONF_INVALID("  peer_tls: yes\n"),
    CONF_INVALID("  peer_tls: false\n  peer_tls: true\n"),
    CONF_INVALID("  peer_tls_ca: /a.pem\n  peer_tls_ca: /b.pem\n"),
};

/* servers comes last, so a case can add entries to its list */
static const char conf_base[] =
    "dyn_o_mite:\n"
    "  dyn_listen: 0.0.0.0:8101\n"
    "  listen: 0.0.0.0:8102\n"
    "  tokens: 437425602\n"
    "  stats_listen: 0.0.0.0:22222\n"
    "  servers:\n"
    "  - 127.0.0.1:22122:1\n";

static rstatus_t conf_run(const struct conf_case *tc) {
  char path[] = "/tmp/dynomite-test-XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0) {
    log_error("conf: mkstemp failed: %s", strerror(errno));
    return DN_ERROR;
  }
  FILE *fh = fdopen(fd, "w");
  if (fh == NULL || fputs(conf_base, fh) == EOF || fputs(tc->lines, fh) == EOF ||
      fclose(fh) != 0) {
    log_error("conf: writing %s failed", path);
    unlink(path);
    return DN_ERROR;
  }

  struct conf *cf = conf_create(path);
  unlink(path);

  if (!tc->valid) {
    if (cf != NULL) {
      log_error("conf '%s': accepted, should be rejected", tc->lines);
      conf_destroy(cf);
      return DN_ERROR;
    }
    return DN_OK;
  }

  if (cf == NULL) {
    log_error("conf '%s': rejected, should be accepted", tc->lines);
    return DN_ERROR;
  }
  int got = *(int *)((uint8_t *)&cf->pool + tc->offset);
  conf_destroy(cf);
  if (got != tc->want) {
    log_error("conf '%s': %s is %d, want %d", tc->lines, tc->field, got,
              tc->want);
    return DN_ERROR;
  }
  return DN_OK;
}

/*
 * Every hash function, on an empty key and a short one: it must succeed and
 * set the token whatever was in it before, the way callers use it.
 */
static rstatus_t test_hash(void) {
  static const char *keys[] = {"", "k", "dynomite"};
  size_t nruns = 0, nfailed = 0;

  print_banner("HASH");

  for (int h = 0; h < HASH_INVALID; h++) {
    hash_func_t hash = get_hash_func((hash_type_t)h);
    for (size_t k = 0; k < NELEMS(keys); k++) {
      struct dyn_token t[2];
      rstatus_t status[2];
      size_t len = strlen(keys[k]);
      for (int i = 0; i < 2; i++) {
        memset(&t[i], i == 0 ? 0 : 0xff, sizeof(t[i]));
        status[i] = hash((const unsigned char *)keys[k], len, &t[i]);
      }
      nruns++;
      if (status[0] != DN_OK || status[1] != DN_OK || t[0].len != 1 ||
          t[1].len != 1 || t[0].mag[0] != t[1].mag[0] ||
          t[0].signum != t[1].signum) {
        log_error("hash %d of '%s': status %d/%d, len %u/%u, token %u/%u", h,
                  keys[k], status[0], status[1], t[0].len, t[1].len,
                  t[0].mag[0], t[1].mag[0]);
        nfailed++;
      }
    }
  }

  /*
   * lookup3's hash of no bytes is its initial value, unmixed, and Paul Hsieh's
   * is 0
   */
  struct dyn_token t;
  nruns += 2;
  if (hash_jenkins((const unsigned char *)"", 0, &t) != DN_OK || t.len != 1 ||
      t.mag[0] != 0xdeadbeef + 13) {
    log_error("hash jenkins of '': token %u, want %u", t.mag[0],
              0xdeadbeef + 13);
    nfailed++;
  }
  if (hash_hsieh((const unsigned char *)"", 0, &t) != DN_OK || t.len != 1 ||
      t.mag[0] != 0) {
    log_error("hash hsieh of '': token %u, want 0", t.mag[0]);
    nfailed++;
  }

  /*
   * one_at_a_time adds bytes from 0x80 up as negative, as x86's signed char
   * did, on every architecture
   */
  static const struct {
    const char *key;
    uint32_t token;
  } oaat[] = {{"dynomite", 3540839119U},
              {"\x80", 3273477671U},
              {"\xff", 2925896852U},
              {"klju\xc4\x8d", 335938375U},
              {"a\xe9\xffz", 7397652U}};
  for (size_t k = 0; k < NELEMS(oaat); k++) {
    nruns++;
    if (hash_one_at_a_time((const unsigned char *)oaat[k].key,
                           strlen(oaat[k].key), &t) != DN_OK ||
        t.len != 1 || t.mag[0] != oaat[k].token) {
      log_error("hash one_at_a_time of key %zu: token %u, want %u", k, t.mag[0],
                oaat[k].token);
      nfailed++;
    }
  }

  if (nfailed != 0) {
    log_error("%zu of %zu hash runs failed", nfailed, nruns);
    return DN_ERROR;
  }
  loga("%zu hash runs passed", nruns);
  return DN_OK;
}

/*
 * Tokens are 32-bit: parse_dyn_token() takes 0 to 4294967295 in decimal and
 * nothing else, leaving the token 0 on errors.
 */
static rstatus_t test_token(void) {
  static const struct {
    const char *str;
    bool valid;
    uint32_t want;
  } cases[] = {
      {"0", true, 0},
      {"00", true, 0},
      {"1", true, 1},
      {"437425602", true, 437425602U},
      {"0004294967295", true, 4294967295U},
      {"4294967295", true, 4294967295U},
      {"", false, 0},
      {"-0", false, 0},
      {"-5", false, 0},
      {"+5", false, 0},
      {" 5", false, 0},
      {"5 ", false, 0},
      {"1a", false, 0},
      {"4294967296", false, 0},
      {"10000000000", false, 0},
      {"99999999999", false, 0},
      {"18446744073709551621", false, 0},
  };
  size_t nfailed = 0;

  print_banner("TOKEN");
  for (size_t i = 0; i < NELEMS(cases); i++) {
    struct dyn_token t;
    memset(&t, 0xff, sizeof(t));
    rstatus_t status = parse_dyn_token((uint8_t *)cases[i].str,
                                       (uint32_t)strlen(cases[i].str), &t);
    uint32_t signum = cases[i].want > 0 ? 1 : 0;
    if ((status == DN_OK) != cases[i].valid || t.len != 1 ||
        t.mag[0] != cases[i].want || t.signum != signum) {
      log_error("token '%s': status %d, signum %u, mag %u, len %u, want %s %u",
                cases[i].str, status, t.signum, t.mag[0], t.len,
                cases[i].valid ? "ok" : "error", cases[i].want);
      nfailed++;
    }
  }

  /* a parsed token orders as its number */
  struct dyn_token a, b;
  parse_dyn_token((uint8_t *)"0", 1, &a);
  parse_dyn_token((uint8_t *)"4294967295", 10, &b);
  if (cmp_dyn_token(&a, &b) >= 0 || cmp_dyn_token(&b, &a) <= 0) {
    log_error("token 0 doesn't sort below 4294967295");
    nfailed++;
  }

  if (nfailed != 0) {
    log_error("%zu of %zu token cases failed", nfailed, NELEMS(cases) + 1);
    return DN_ERROR;
  }
  loga("%zu token cases passed", NELEMS(cases) + 1);
  return DN_OK;
}

static rstatus_t test_config(void) {
  size_t nfailed = 0;

  print_banner("CONF");
  for (size_t i = 0; i < NELEMS(conf_cases); i++) {
    if (conf_run(&conf_cases[i]) != DN_OK) {
      nfailed++;
    }
  }

  if (nfailed != 0) {
    log_error("%zu of %zu conf cases failed", nfailed, NELEMS(conf_cases));
    return DN_ERROR;
  }
  loga("%zu conf cases passed", NELEMS(conf_cases));
  return DN_OK;
}

/*
 * Peer TLS: a CA, a second CA and node certs are generated here (Ed25519,
 * in a temp dir, removed afterwards; nothing is committed), then the TLS
 * context is created from them like at startup, and two contexts shake
 * hands in memory.
 */
static X509 *tls_test_cert(const char *cn, EVP_PKEY *key, X509 *issuer,
                           EVP_PKEY *issuer_key, long not_before,
                           long not_after, bool ca) {
  static long serial = 1;
  X509 *x = X509_new();
  X509V3_CTX v3;
  X509_EXTENSION *ext;

  if (x == NULL) return NULL;
  X509_set_version(x, X509_VERSION_3);
  ASN1_INTEGER_set(X509_get_serialNumber(x), serial++);
  X509_gmtime_adj(X509_getm_notBefore(x), not_before);
  X509_gmtime_adj(X509_getm_notAfter(x), not_after);
  X509_NAME_add_entry_by_txt(X509_get_subject_name(x), "CN", MBSTRING_ASC,
                             (const unsigned char *)cn, -1, -1, 0);
  X509_set_issuer_name(x, issuer != NULL ? X509_get_subject_name(issuer)
                                         : X509_get_subject_name(x));
  X509_set_pubkey(x, key);

  X509V3_set_ctx_nodb(&v3);
  X509V3_set_ctx(&v3, issuer != NULL ? issuer : x, x, NULL, NULL, 0);
  ext = X509V3_EXT_conf_nid(NULL, &v3, NID_basic_constraints,
                            ca ? "critical,CA:TRUE" : "CA:FALSE");
  X509_add_ext(x, ext, -1);
  X509_EXTENSION_free(ext);
  if (!ca) {
    ext = X509V3_EXT_conf_nid(NULL, &v3, NID_ext_key_usage,
                              "serverAuth,clientAuth");
    X509_add_ext(x, ext, -1);
    X509_EXTENSION_free(ext);
  }
  /* Ed25519 signs without a digest */
  if (X509_sign(x, issuer_key != NULL ? issuer_key : key, NULL) == 0) {
    X509_free(x);
    return NULL;
  }
  return x;
}

static bool tls_test_write(const char *path, X509 *cert, EVP_PKEY *key) {
  FILE *fh = fopen(path, "w");
  bool ok = fh != NULL;

  if (ok && cert != NULL) ok = PEM_write_X509(fh, cert) == 1;
  if (ok && key != NULL) {
    ok = PEM_write_PrivateKey(fh, key, NULL, NULL, 0, NULL, NULL) == 1;
  }
  if (fh != NULL) fclose(fh);
  if (!ok) log_error("tls: writing %s failed", path);
  return ok;
}

struct tls_case {
  const char *name;
  const char *cert, *key, *ca;
  bool want_ctx;  /* the context is created */
  bool want_warn; /* the self-check warned */
  int64_t min_days, max_days;
};

static struct tls_ctx *tls_test_ctx(const char *dir, const char *cert,
                                    const char *key, const char *ca) {
  struct conf_pool cp;
  char cert_path[PATH_MAX], key_path[PATH_MAX], ca_path[PATH_MAX];

  memset(&cp, 0, sizeof(cp));
  cp.peer_tls = true;
  snprintf(cert_path, sizeof(cert_path), "%s/%s", dir, cert);
  snprintf(key_path, sizeof(key_path), "%s/%s", dir, key);
  snprintf(ca_path, sizeof(ca_path), "%s/%s", dir, ca);
  string_set_raw(&cp.peer_tls_cert, cert_path);
  string_set_raw(&cp.peer_tls_key, key_path);
  string_set_raw(&cp.peer_tls_ca, ca_path);
  return tls_ctx_create(&cp);
}

static rstatus_t tls_run(const char *dir, const struct tls_case *tc) {
  struct tls_ctx *tls = tls_test_ctx(dir, tc->cert, tc->key, tc->ca);

  if (!tc->want_ctx) {
    if (tls != NULL) {
      log_error("tls %s: context created, should be refused", tc->name);
      tls_ctx_destroy(tls);
      return DN_ERROR;
    }
    return DN_OK;
  }
  if (tls == NULL) {
    log_error("tls %s: context refused, should be created", tc->name);
    return DN_ERROR;
  }
  rstatus_t status = DN_OK;
  if (tls_ctx_cert_warned(tls) != tc->want_warn) {
    log_error("tls %s: self-check %s, should %s", tc->name,
              tc->want_warn ? "passed" : "warned",
              tc->want_warn ? "warn" : "pass");
    status = DN_ERROR;
  }
  int64_t days = tls_cert_days_left(tls);
  if (days < tc->min_days || days > tc->max_days) {
    log_error("tls %s: %" PRId64 " days left, want %" PRId64 " to %" PRId64,
              tc->name, days, tc->min_days, tc->max_days);
    status = DN_ERROR;
  }
  tls_ctx_destroy(tls);
  return status;
}

struct tls_handshake_case {
  const char *name;
  const char *client_cert, *client_key, *client_ca; /* NULL: no cert */
  const char *server_cert, *server_key, *server_ca;
  bool want_ok;
};

static rstatus_t tls_run_handshake(const char *dir,
                                   const struct tls_handshake_case *tc) {
  struct tls_ctx *client = NULL, *server;
  const char *detail;
  rstatus_t status = DN_OK;

  server = tls_test_ctx(dir, tc->server_cert, tc->server_key, tc->server_ca);
  if (tc->client_cert != NULL) {
    client = tls_test_ctx(dir, tc->client_cert, tc->client_key, tc->client_ca);
  }
  if (server == NULL || (tc->client_cert != NULL && client == NULL)) {
    log_error("tls handshake %s: creating the contexts failed", tc->name);
    status = DN_ERROR;
    goto out;
  }

  rstatus_t hs = tls_test_handshake(client, server, &detail);
  if (tc->want_ok && hs != DN_OK) {
    log_error("tls handshake %s: failed (%s), should succeed", tc->name,
              detail);
    status = DN_ERROR;
  } else if (!tc->want_ok && hs == DN_OK) {
    log_error("tls handshake %s: succeeded, should fail", tc->name);
    status = DN_ERROR;
  } else if (tc->want_ok && strcmp(detail, "TLSv1.3") != 0) {
    log_error("tls handshake %s: negotiated %s, want TLSv1.3", tc->name,
              detail);
    status = DN_ERROR;
  } else {
    loga("tls handshake %s: %s", tc->name, detail);
  }

out:
  tls_ctx_destroy(client);
  tls_ctx_destroy(server);
  return status;
}

static rstatus_t test_tls(void) {
  const long day = 24 * 60 * 60;
  char dir[] = "/tmp/dynomite-test-tls-XXXXXX";
  EVP_PKEY *ca_key = NULL, *ca2_key = NULL, *node_key = NULL, *other_key = NULL;
  X509 *ca = NULL, *ca2 = NULL, *node = NULL, *bad = NULL, *expired = NULL,
       *future = NULL;
  size_t nfailed = 0;
  rstatus_t status = DN_ERROR;
  const char *files[] = {"ca.pem",     "ca2.pem",   "both.pem", "node.pem",
                         "node.key",   "other.key", "bad.pem",  "expired.pem",
                         "future.pem", "empty.pem"};

  print_banner("TLS");

  if (mkdtemp(dir) == NULL) {
    log_error("tls: mkdtemp failed: %s", strerror(errno));
    return DN_ERROR;
  }

  ca_key = EVP_PKEY_Q_keygen(NULL, NULL, "ED25519");
  ca2_key = EVP_PKEY_Q_keygen(NULL, NULL, "ED25519");
  node_key = EVP_PKEY_Q_keygen(NULL, NULL, "ED25519");
  other_key = EVP_PKEY_Q_keygen(NULL, NULL, "ED25519");
  if (ca_key == NULL || ca2_key == NULL || node_key == NULL ||
      other_key == NULL) {
    log_error("tls: key generation failed");
    goto out;
  }
  ca = tls_test_cert("test CA", ca_key, NULL, NULL, -day, 10 * day, true);
  ca2 = tls_test_cert("other CA", ca2_key, NULL, NULL, -day, 10 * day, true);
  node = tls_test_cert("node", node_key, ca, ca_key, -day, 365 * day, false);
  bad = tls_test_cert("node", node_key, ca2, ca2_key, -day, 365 * day, false);
  /* expired half a day ago: -1 day left, with a margin for a slow run */
  expired =
      tls_test_cert("node", node_key, ca, ca_key, -3 * day, -day / 2, false);
  future = tls_test_cert("node", node_key, ca, ca_key, day, 365 * day, false);
  if (ca == NULL || ca2 == NULL || node == NULL || bad == NULL ||
      expired == NULL || future == NULL) {
    log_error("tls: certificate generation failed");
    goto out;
  }

  char path[PATH_MAX];
#define TLS_FILE(name, cert, key)                     \
  do {                                                \
    snprintf(path, sizeof(path), "%s/%s", dir, name); \
    if (!tls_test_write(path, cert, key)) goto out;   \
  } while (0)
  TLS_FILE("ca.pem", ca, NULL);
  TLS_FILE("ca2.pem", ca2, NULL);
  TLS_FILE("node.pem", node, NULL);
  TLS_FILE("node.key", NULL, node_key);
  TLS_FILE("other.key", NULL, other_key);
  TLS_FILE("bad.pem", bad, NULL);
  TLS_FILE("expired.pem", expired, NULL);
  TLS_FILE("future.pem", future, NULL);
  TLS_FILE("empty.pem", NULL, NULL);
#undef TLS_FILE
  /* both CAs in one file, as during a CA rotation */
  snprintf(path, sizeof(path), "%s/both.pem", dir);
  FILE *fh = fopen(path, "w");
  if (fh == NULL || PEM_write_X509(fh, ca2) != 1 ||
      PEM_write_X509(fh, ca) != 1 || fclose(fh) != 0) {
    log_error("tls: writing %s failed", path);
    goto out;
  }

  const struct tls_case cases[] = {
      {"ok", "node.pem", "node.key", "ca.pem", true, false, 364, 365},
      {"two CAs", "node.pem", "node.key", "both.pem", true, false, 364, 365},
      {"key mismatch", "node.pem", "other.key", "ca.pem", false, false, 0, 0},
      {"missing cert", "nonexistent.pem", "node.key", "ca.pem", false, false, 0,
       0},
      {"missing key", "node.pem", "nonexistent.key", "ca.pem", false, false, 0,
       0},
      {"missing ca", "node.pem", "node.key", "nonexistent.pem", false, false, 0,
       0},
      {"empty ca", "node.pem", "node.key", "empty.pem", false, false, 0, 0},
      {"key as cert", "node.key", "node.key", "ca.pem", false, false, 0, 0},
      /* the self-check warns, the node still starts */
      {"other CA", "bad.pem", "node.key", "ca.pem", true, true, 364, 365},
      {"expired", "expired.pem", "node.key", "ca.pem", true, true, -1, -1},
      {"not yet valid", "future.pem", "node.key", "ca.pem", true, true, 364,
       365},
  };
  for (size_t i = 0; i < NELEMS(cases); i++) {
    if (tls_run(dir, &cases[i]) != DN_OK) nfailed++;
  }

  const struct tls_handshake_case handshakes[] = {
      {"same CA", "node.pem", "node.key", "ca.pem", "node.pem", "node.key",
       "ca.pem", true},
      {"CA rotation", "node.pem", "node.key", "both.pem", "bad.pem", "node.key",
       "both.pem", true},
      {"client from another CA", "bad.pem", "node.key", "ca.pem", "node.pem",
       "node.key", "ca.pem", false},
      {"server from another CA", "node.pem", "node.key", "ca.pem", "bad.pem",
       "node.key", "ca.pem", false},
      {"expired client cert", "expired.pem", "node.key", "ca.pem", "node.pem",
       "node.key", "ca.pem", false},
      {"no client cert", NULL, NULL, NULL, "node.pem", "node.key", "ca.pem",
       false},
  };
  for (size_t i = 0; i < NELEMS(handshakes); i++) {
    if (tls_run_handshake(dir, &handshakes[i]) != DN_OK) nfailed++;
  }

  if (nfailed != 0) {
    log_error("%zu of %zu tls cases failed", nfailed,
              NELEMS(cases) + NELEMS(handshakes));
  } else {
    loga("%zu tls cases passed", NELEMS(cases) + NELEMS(handshakes));
    status = DN_OK;
  }

out:
  for (size_t i = 0; i < NELEMS(files); i++) {
    snprintf(path, sizeof(path), "%s/%s", dir, files[i]);
    unlink(path);
  }
  rmdir(dir);
  X509_free(ca);
  X509_free(ca2);
  X509_free(node);
  X509_free(bad);
  X509_free(expired);
  X509_free(future);
  EVP_PKEY_free(ca_key);
  EVP_PKEY_free(ca2_key);
  EVP_PKEY_free(node_key);
  EVP_PKEY_free(other_key);
  return status;
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
  struct server_pool *sp = &nci->ctx->pool;
  sp->mbuf_size = TEST_MBUF_SIZE;
  sp->alloc_msgs_max = TEST_ALLOC_MSGS_MAX;

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

  ret = test_read_repair(conn);
  if (ret != DN_OK) {
    loga("Error in testing read repairs!!!");
    goto err_out;
  }

  ret = test_no_quorum(conn);
  if (ret != DN_OK) {
    loga("Error in testing replies without quorum!!!");
    goto err_out;
  }

  ret = test_msg_limit(conn);
  if (ret != DN_OK) {
    loga("Error in testing the msg limit!!!");
    goto err_out;
  }

  ret = test_rewrite(conn);
  if (ret != DN_OK) {
    loga("Error in testing query rewrites!!!");
    goto err_out;
  }

  ret = test_append_format(conn);
  if (ret != DN_OK) {
    loga("Error in testing msg_append_format()!!!");
    goto err_out;
  }

  ret = test_hash();
  if (ret != DN_OK) {
    loga("Error in testing the hash functions!!!");
    goto err_out;
  }

  ret = test_token();
  if (ret != DN_OK) {
    loga("Error in testing the token parser!!!");
    goto err_out;
  }

  ret = test_config();
  if (ret != DN_OK) {
    loga("Error in testing the configuration!!!");
    goto err_out;
  }

  ret = test_tls();
  if (ret != DN_OK) {
    loga("Error in testing peer TLS!!!");
    goto err_out;
  }

  ret = test_tailq();
  if (ret != DN_OK) {
    loga("Error in testing TAILQ counter!!!");
    goto err_out;
  }

  loga("Testing is done!!!");
err_out:
  deinit_peer(peer);
  return ret;
}
