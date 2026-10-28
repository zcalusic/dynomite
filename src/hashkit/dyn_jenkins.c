/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
 * code any way you wish, private, educational, or commercial.  It's free.
 * Use for hash table lookup, or anything where one collision in 2^^32 is
 * acceptable.  Do NOT use for cryptographic purposes.
 * http://burtleburtle.net/bob/hash/index.html
 *
 * Modified by Brian Pontz for libmemcached
 *
 * Only lookup3's byte at a time path is left: it gives the results of its
 * little-endian word reads on every machine and at any alignment, without
 * reading past the end of the key as they did.
 */

#include <dyn_core.h>
#include <dyn_token.h>

#define hashsize(n) ((uint32_t)1 << (n))
#define hashmask(n) (hashsize(n) - 1)
#define rot(x, k) (((x) << (k)) | ((x) >> (32 - (k))))

#define mix(a, b, c) \
  {                  \
    a -= c;          \
    a ^= rot(c, 4);  \
    c += b;          \
    b -= a;          \
    b ^= rot(a, 6);  \
    a += c;          \
    c -= b;          \
    c ^= rot(b, 8);  \
    b += a;          \
    a -= c;          \
    a ^= rot(c, 16); \
    c += b;          \
    b -= a;          \
    b ^= rot(a, 19); \
    a += c;          \
    c -= b;          \
    c ^= rot(b, 4);  \
    b += a;          \
  }

#define final(a, b, c) \
  {                    \
    c ^= b;            \
    c -= rot(b, 14);   \
    a ^= c;            \
    a -= rot(c, 11);   \
    b ^= a;            \
    b -= rot(a, 25);   \
    c ^= b;            \
    c -= rot(b, 16);   \
    a ^= c;            \
    a -= rot(c, 4);    \
    b ^= a;            \
    b -= rot(a, 14);   \
    c ^= b;            \
    c -= rot(b, 24);   \
  }

#define JENKINS_INITVAL 13

/*
 * jenkins_hash() -- hash a variable-length key into a 32-bit value
 *  k       : the key (the unaligned variable-length array of bytes)
 *  length  : the length of the key, counting by bytes
 *  initval : can be any 4-byte value
 * Returns a 32-bit value.  Every bit of the key affects every bit of
 * the return value.  Two keys differing by one or two bits will have
 * totally different hash values.

 * The best hash table sizes are powers of 2.  There is no need to do
 * mod a prime (mod is sooo slow!).  If you need less than 32 bits,
 * use a bitmask.  For example, if you need only 10 bits, do
 *   h = (h & hashmask(10));
 * In which case, the hash table should have hashsize(10) elements.
 */

rstatus_t hash_jenkins(const unsigned char *key, size_t length,
                       struct dyn_token *token) {
  uint32_t a, b, c; /* internal state */
  const uint8_t *k = (const uint8_t *)key;

  /* Set up the internal state */
  a = b = c = 0xdeadbeef + ((uint32_t)length) + JENKINS_INITVAL;

  /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
  while (length > 12) {
    a += k[0];
    a += ((uint32_t)k[1]) << 8;
    a += ((uint32_t)k[2]) << 16;
    a += ((uint32_t)k[3]) << 24;
    b += k[4];
    b += ((uint32_t)k[5]) << 8;
    b += ((uint32_t)k[6]) << 16;
    b += ((uint32_t)k[7]) << 24;
    c += k[8];
    c += ((uint32_t)k[9]) << 8;
    c += ((uint32_t)k[10]) << 16;
    c += ((uint32_t)k[11]) << 24;
    mix(a, b, c);
    length -= 12;
    k += 12;
  }

  /*-------------------------------- last block: affect all 32 bits of (c) */
  switch (length) /* all the case statements fall through */
  {
    case 12:
      c += ((uint32_t)k[11]) << 24;
    case 11:
      c += ((uint32_t)k[10]) << 16;
    case 10:
      c += ((uint32_t)k[9]) << 8;
    case 9:
      c += k[8];
    case 8:
      b += ((uint32_t)k[7]) << 24;
    case 7:
      b += ((uint32_t)k[6]) << 16;
    case 6:
      b += ((uint32_t)k[5]) << 8;
    case 5:
      b += k[4];
    case 4:
      a += ((uint32_t)k[3]) << 24;
    case 3:
      a += ((uint32_t)k[2]) << 16;
    case 2:
      a += ((uint32_t)k[1]) << 8;
    case 1:
      a += k[0];
      break;
    case 0:
      goto out;
    default:
      goto out;
  }

  final(a, b, c);

out:
  size_dyn_token(token, 1);
  set_int_dyn_token(token, c);

  return DN_OK;
}
