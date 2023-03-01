/*
 * Haura for a sample external io engine
 *
 * Should be compiled with:
 *
 * gcc -Wall -O2 -g -D_GNU_SOURCE -include ../config-host.h -shared -rdynamic
 * -fPIC -o haura_external.o haura_external.c (also requires -D_GNU_SOURCE
 * -DCONFIG_STRSEP on Linux)
 *
 */
#include <asm-generic/errno-base.h>
#include <assert.h>
#include <bits/pthreadtypes.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../../betree/include/betree.h"
#include "../fio-fio-3.33/fio.h"
#include "../fio-fio-3.33/optgroup.h"

/*
 * The core of the module is identical to the ones included with fio,
 * read those. You cannot use register_ioengine() and unregister_ioengine()
 * for external modules, they should be gotten through dlsym()
 */

/*
 * The io engine can define its own options within the io engine source.
 * The option member must not be at offset 0, due to the way fio parses
 * the given option. Just add a padding pointer unless the io engine has
 * something usable.
 */
struct fio_haura_options {
  void *pad; /* avoid ->off1 of fio_option becomes 0 */
  int respect_fio_files;
};

struct haura_data {
  db_t *db;
  obj_store_t *obj_s;
  char *obj_counter;
  pthread_mutex_t mtx;
};

static struct haura_data global_data = {
    .db = NULL, .obj_s = NULL, .mtx = PTHREAD_MUTEX_INITIALIZER};

static struct fio_option options[] = {
    {
        .name = "respect-fio-files",
        .lname = "respect-fio-files",
        .type = FIO_OPT_STR_SET,
        .off1 = offsetof(struct fio_haura_options, respect_fio_files),
        .help = "Respect the files given by fio; instead of using the haura "
                "specific configured devices.",
        .category = FIO_OPT_C_ENGINE, /* always use this */
        .group = FIO_OPT_G_INVALID,   /* this can be different */
    },
};

static int bail(struct err_t *error) {
  betree_print_error(error);
  betree_free_err(error);
  return 1;
}

static void fio_haura_translate(struct thread_data *td, struct cfg_t *cfg) {
  betree_configuration_set_direct(cfg, td->o.odirect);
  if (((struct fio_haura_options *)td->eo)->respect_fio_files) {
    char **paths;

    if ((paths = malloc(sizeof(char *) * td->files_index)) == NULL) {
      fprintf(stderr, "fio-haura: OOM");
      exit(2);
    }
    for (size_t idx = 0; idx < td->files_index; idx += 1) {
      paths[idx] = td->files[idx]->file_name;
    }

    betree_configuration_set_disks(cfg, (const char *const *)paths,
                                   td->files_index);
    free(paths);
  }
}

/*
 * The ->event() hook is called to match an event number with an io_u.
 * After the core has called ->getevents() and it has returned eg 3,
 * the ->event() hook must return the 3 events that have completed for
 * subsequent calls to ->event() with [0-2]. Required.
 */
static struct io_u *fio_haura_event(struct thread_data *td, int event) {
  return NULL;
}

/*
 * The ->getevents() hook is used to reap completion events from an async
 * io engine. It returns the number of completed events since the last call,
 * which may then be retrieved by calling the ->event() hook with the event
 * numbers. Required.
 */
static int fio_haura_getevents(struct thread_data *td, unsigned int min,
                               unsigned int max, const struct timespec *t) {
  return 0;
}

/*
 * The ->cancel() hook attempts to cancel the io_u. Only relevant for
 * async io engines, and need not be supported.
 */
static int fio_haura_cancel(struct thread_data *td, struct io_u *io_u) {
  return 0;
}

/*
 * The ->queue() hook is responsible for initiating io on the io_u
 * being passed in. If the io engine is a synchronous one, io may complete
 * before ->queue() returns. Required.
 *
 * The io engine must transfer in the direction noted by io_u->ddir
 * to the buffer pointed to by io_u->xfer_buf for as many bytes as
 * io_u->xfer_buflen. Residual data count may be set in io_u->resid
 * for a short read/writ
 */
static enum fio_q_status fio_haura_queue(struct thread_data *td,
                                         struct io_u *io_u) {
  struct err_t *error = NULL;
  struct obj_t *obj = td->io_ops_data;
  /*
   * Double sanity check to catch errant write on a readonly setup
   */
  fio_ro_check(td, io_u);

  if (io_u->ddir == DDIR_WRITE) {
    unsigned long written;
    betree_object_write_at(obj, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset,
                           &written, &error);
  } else if (io_u->ddir == DDIR_READ) {
    unsigned long read;
    betree_object_read_at(obj, io_u->xfer_buf, io_u->xfer_buflen, io_u->offset,
                          &read, &error);
  } else if (io_u->ddir == DDIR_SYNC) {
    betree_sync_db(global_data.db, &error);
  }
  if (error != NULL) {
    bail(error);
    return FIO_Q_BUSY;
  }

  /*
   * Could return FIO_Q_QUEUED for a queued request,
   * FIO_Q_COMPLETED for a completed request, and FIO_Q_BUSY
   * if we could queue no more at this point (you'd have to
   * define ->commit() to handle that.
   */
  return FIO_Q_COMPLETED;
}

/*
 * The ->prep() function is called for each io_u prior to being submitted
 * with ->queue(). This hook allows the io engine to perform any
 * preparatory actions on the io_u, before being submitted. Not required.
 */
static int fio_haura_prep(struct thread_data *td, struct io_u *io_u) {
  return 0;
}

/*
 * The init function is called once per thread/process, and should set up
 * any structures that this io engine requires to keep track of io. Not
 * required.
 */
static int fio_haura_init(struct thread_data *td) {
  struct err_t *error = NULL;
  struct storage_pref_t pref = {._0 = 0};
  if (td->o.td_ddir & TD_DDIR_READ && !(td->o.td_ddir & TD_DDIR_WRITE)) {
    fprintf(stderr, "Not supported.\n");
    exit(3);
  }
  if (!td->o.use_thread && td->o.numjobs != 1) {
    fprintf(stderr,
            "Cannot use fio-engine-haura with multiple processes. Specify "
            "`--thread` when calling fio.\n");
    exit(1);
  }

  if (0 != pthread_mutex_lock(&global_data.mtx)) {
    fprintf(stderr, "Mutex locking failed.\n");
    exit(1);
  }

  // Initialize the database if not already present
  if (global_data.db == NULL) {
    struct cfg_t *cfg;

    if ((cfg = betree_configuration_from_env(&error)) == NULL) {
      return bail(error);
    }
    fio_haura_translate(td, cfg);
    if ((global_data.db = betree_create_db(cfg, &error)) == NULL) {
      return bail(error);
    }
    if ((global_data.obj_s = betree_create_object_store(
             global_data.db, "fio", 3, pref, &error)) == NULL) {
      return bail(error);
    }
    global_data.obj_counter = malloc(2);
    char init[2] = {1};
    memcpy(global_data.obj_counter, (void *)init, 2);
  }

  // Create a private object for each thread
  global_data.obj_counter[1]++;
  if ((td->io_ops_data =
           betree_object_create(global_data.obj_s, global_data.obj_counter, 2,
                                pref, &error)) == NULL) {
    return bail(error);
  }

  if (0 != pthread_mutex_unlock(&global_data.mtx)) {
    fprintf(stderr, "Mutex unlocking failed.\n");
    exit(1);
  }
  return 0;
}

/*
 * This is paired with the ->init() function and is called when a thread is
 * done doing io. Should tear down anything setup by the ->init() function.
 * Not required.
 */
static void fio_haura_cleanup(struct thread_data *td) {
  if (0 != pthread_mutex_lock(&global_data.mtx)) {
    fprintf(stderr, "Mutex locking failed.\n");
    exit(1);
  }
  if (global_data.db != NULL) {
    betree_close_db(global_data.db);
    global_data.db = NULL;
    global_data.obj_s = NULL;
    free(global_data.obj_counter);
  }
  if (0 != pthread_mutex_unlock(&global_data.mtx)) {
    fprintf(stderr, "Mutex unlocking failed.\n");
    exit(1);
  }
}

/*
 * Hook for opening the given file. Unless the engine has special
 * needs, it usually just provides generic_open_file() as the handler.
 */
static int fio_haura_open(struct thread_data *td, struct fio_file *f) {
  return generic_open_file(td, f);
}

/*
 * Hook for closing a file. See fio_haura_open().
 */
static int fio_haura_close(struct thread_data *td, struct fio_file *f) {
  return generic_close_file(td, f);
}

/*
 * Note that the structure is exported, so that fio can get it via
 * dlsym(..., "ioengine"); for (and only for) external engines.
 */
struct ioengine_ops ioengine = {
    .name = "haura",
    .version = FIO_IOOPS_VERSION,
    .init = fio_haura_init,
    .prep = fio_haura_prep,
    .queue = fio_haura_queue,
    .cancel = fio_haura_cancel,
    .getevents = fio_haura_getevents,
    .event = fio_haura_event,
    .cleanup = fio_haura_cleanup,
    .open_file = fio_haura_open,
    .close_file = fio_haura_close,
    .get_file_size = generic_get_file_size,
    .options = options,
    .option_struct_size = sizeof(struct fio_haura_options),
};
