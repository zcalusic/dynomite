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

#include "dyn_signal.h"

#include <pthread.h>
#include <signal.h>
#include <stdlib.h>

#include "dyn_core.h"

static void signal_segv(int signo);

/*
 * signal_handler() only records that a signal came; signal_process() acts on
 * it in the main loop, where anything may be done. These signals are blocked
 * in every thread (see signal_block()) except while the main loop waits for
 * events, so a signal always ends that wait, and is acted on at once.
 *
 * SIGTERM (docker stop, kubectl delete, systemd) and SIGINT exit cleanly.
 * SIGQUIT keeps its default action, a core dump.
 */
static struct signal signals[] = {
    {SIGUSR1, "SIGUSR1", 0, signal_handler, 0},
    {SIGUSR2, "SIGUSR2", 0, signal_handler, 0},
    {SIGTTIN, "SIGTTIN", 0, signal_handler, 0},
    {SIGTTOU, "SIGTTOU", 0, signal_handler, 0},
    {SIGHUP, "SIGHUP", 0, signal_handler, 0},
    {SIGINT, "SIGINT", 0, signal_handler, 0},
    {SIGTERM, "SIGTERM", 0, signal_handler, 0},
    {SIGSEGV, "SIGSEGV", (int)SA_RESETHAND, signal_segv, 0},
    {SIGPIPE, "SIGPIPE", 0, SIG_IGN, 0},
    {0, NULL, 0, NULL, 0}};

/* set by signal_handler(), when any signal's pending is */
static volatile sig_atomic_t signal_pending_any;

/* the signals signal_block() blocked, and the mask to wait for events with */
static sigset_t handled;
static sigset_t wait_mask;
static bool blocked;

/**
 * Initialize the list of POSIX signals that dynomite can respond to and map
 * each signal to a handler function.
 * @return status
 */
rstatus_t signal_init(void) {
  struct signal *sig;

  for (sig = signals; sig->signo != 0; sig++) {
    rstatus_t status;
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig->handler;
    sa.sa_flags = sig->flags;
    sigemptyset(&sa.sa_mask);

    status = sigaction(sig->signo, &sa, NULL);
    if (status < 0) {
      log_error("sigaction(%s) failed: %s", sig->signame, strerror(errno));
      return DN_ERROR;
    }
  }

  return DN_OK;
}

void signal_deinit(void) {}

/**
 * Block the signals signal_handler() records, in the calling thread and so in
 * every thread it starts afterwards: call it before any thread is started.
 * The main loop unblocks them only while it waits for events, with
 * signal_wait_mask(), so no other thread takes them, and none comes between
 * checking for signals and starting to wait.
 * @return status
 */
rstatus_t signal_block(void) {
  struct signal *sig;

  sigemptyset(&handled);
  for (sig = signals; sig->signo != 0; sig++) {
    if (sig->handler == signal_handler) {
      sigaddset(&handled, sig->signo);
    }
  }

  int err = pthread_sigmask(SIG_BLOCK, &handled, &wait_mask);
  if (err != 0) {
    log_error("pthread_sigmask failed: %s", strerror(err));
    return DN_ERROR;
  }

  /* wait with them unblocked, even if they were blocked when we started */
  for (sig = signals; sig->signo != 0; sig++) {
    if (sig->handler == signal_handler) {
      sigdelset(&wait_mask, sig->signo);
    }
  }
  blocked = true;

  return DN_OK;
}

/**
 * The signal mask to wait for events with, or NULL before signal_block().
 */
const sigset_t *signal_wait_mask(void) { return blocked ? &wait_mask : NULL; }

/**
 * For event loops without a wait call that takes a signal mask: unblock the
 * signals for the wait, and tell whether one is already pending, so the
 * caller returns instead of waiting. A signal that comes between the check
 * and the wait still ends the wait, only at its timeout. Call
 * signal_wait_end() after the wait.
 */
bool signal_wait_begin(void) {
  if (blocked) {
    pthread_sigmask(SIG_SETMASK, &wait_mask, NULL);
  }
  return signal_pending_any != 0;
}

void signal_wait_end(void) {
  if (blocked) {
    pthread_sigmask(SIG_BLOCK, &handled, NULL);
  }
}

void signal_handler(int signo) {
  struct signal *sig;

  for (sig = signals; sig->signo != 0; sig++) {
    if (sig->signo == signo) {
      sig->pending = 1;
      signal_pending_any = 1;
      return;
    }
  }
}

static void signal_segv(int signo) {
  dn_stacktrace(1);
  loga("signal %d (SIGSEGV) received, core dumping", signo);
  raise(SIGSEGV);
}

/**
 * Act on the signals that came since the last call: log each, and change the
 * log level, reopen the log file, or ask to exit.
 * @return true if dynomite should exit
 */
bool signal_process(void) {
  struct signal *sig;
  bool done = false;

  if (signal_pending_any == 0) {
    return false;
  }
  signal_pending_any = 0;

  for (sig = signals; sig->signo != 0; sig++) {
    void (*action)(void) = NULL;
    char *actionstr = "";

    if (sig->pending == 0) {
      continue;
    }
    sig->pending = 0;

    switch (sig->signo) {
      case SIGUSR1:
      case SIGUSR2:
        break;

      case SIGTTIN:
        actionstr = ", up logging level";
        action = log_level_up;
        break;

      case SIGTTOU:
        actionstr = ", down logging level";
        action = log_level_down;
        break;

      case SIGHUP:
        actionstr = ", reopening log file";
        action = log_reopen;
        break;

      case SIGINT:
      case SIGTERM:
        done = true;
        actionstr = ", exiting";
        break;

      default:
        NOT_REACHED();
    }

    loga("signal %d (%s) received%s", sig->signo, sig->signame, actionstr);

    if (action != NULL) {
      action();
    }
  }

  return done;
}
