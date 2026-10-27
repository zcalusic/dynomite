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

#ifndef _DYN_SIGNAL_H_
#define _DYN_SIGNAL_H_

#include <signal.h>
#include <stdbool.h>

#include "dyn_types.h"

/**
 * @brief POSIX signal
 */
struct signal {
  int signo;
  char *signame;
  int flags;
  void (*handler)(int signo);
  volatile sig_atomic_t pending; /* set by signal_handler() */
};

rstatus_t signal_init(void);
void signal_deinit(void);
rstatus_t signal_block(void);
const sigset_t *signal_wait_mask(void);
bool signal_wait_begin(void);
void signal_wait_end(void);
void signal_handler(int signo);
bool signal_process(void);

#endif
