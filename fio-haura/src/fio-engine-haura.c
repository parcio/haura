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
#include <fcntl.h>
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
  int disrespect_fio_files;
  int disrespect_fio_queue_depth;
  int disrespect_fio_direct;
  int disrespect_fio_options;
  int haura_nvm;
};

struct haura_data {
  size_t cnt;
  size_t jobs;
  char **files;
  db_t *db;
  obj_store_t *obj_s;
  obj_t **objs;
  pthread_mutex_t mtx;
};

struct storage_pref_t pref = {._0 = 0};

static struct haura_data global_data = {.db = NULL,
                                        .obj_s = NULL,
                                        .objs = NULL,
                                        .mtx = PTHREAD_MUTEX_INITIALIZER,
                                        .cnt = 0};

static struct fio_option options[] = {
    {
        .name = "disrespect-fio-files",
        .lname = "disrespect-fio-files",
        .type = FIO_OPT_STR_SET,
        .off1 = offsetof(struct fio_haura_options, disrespect_fio_files),
        .help = "Avoid transferring fio file configuration to haura. Can be "
                "used to use specific disks regardless of fio specification.",
        .category = FIO_OPT_C_ENGINE, /* always use this */
        .group = FIO_OPT_G_INVALID,   /* this can be different */
    },
    //{
    //    .name = "disrespect-fio-queue-depth",
    //    .lname = "disrespect-fio-queue-depth",
    //    .type = FIO_OPT_STR_SET,
    //    .off1 = offsetof(struct fio_haura_options,
    //    disrespect_fio_queue_depth), .help =
    //        "Avoid transferring fio queue configuration to haura. Can be "
    //        "used to use defined I/O depth regardless of fio specification.",
    //    .category = FIO_OPT_C_ENGINE, /* always use this */
    //    .group = FIO_OPT_G_INVALID,   /* this can be different */
    //},
    {
        .name = "disrespect-fio-direct",
        .lname = "disrespect-fio-direct",
        .type = FIO_OPT_STR_SET,
        .off1 = offsetof(struct fio_haura_options, disrespect_fio_direct),
        .help = "Use direct mode only as specified in haura configuration.",
        .category = FIO_OPT_C_ENGINE, /* always use this */
        .group = FIO_OPT_G_INVALID,   /* this can be different */
    },
    {
        .name = "disrespect-fio-options",
        .lname = "disrespect-fio-options",
        .type = FIO_OPT_STR_SET,
        .off1 = offsetof(struct fio_haura_options, disrespect_fio_options),
        .help = "Disregard all fio options in Haura. This only uses the I/O "
                "workflow as executed by fio. Take care to ensure "
                "comparability with results of other engines.",
        .category = FIO_OPT_C_ENGINE, /* always use this */
        .group = FIO_OPT_G_INVALID,   /* this can be different */
    },
};

static int bail(struct err_t *error) {
  betree_print_error(error);
  printf("\n");
  betree_free_err(error);
  return 1;
}

static void fio_haura_translate(struct thread_data *td, struct cfg_t *cfg) {
  if (((struct fio_haura_options *)td->eo)->disrespect_fio_options) {
    return;
  }
  // @jwuensche: This sets the queue depth to one on presumed "sync" workflows
  // which is detrimental to any functionality of haura as it provokes lock ups.
  // if (!((struct fio_haura_options *)td->eo)->disrespect_fio_queue_depth) {
  //   betree_configuration_set_iodepth(cfg, td->o.iodepth);
  // }
  if (!((struct fio_haura_options *)td->eo)->disrespect_fio_files) {
    betree_configuration_set_disks(cfg, (const char *const *)global_data.files,
                                   td->files_index * global_data.jobs);
  }
  if (!((struct fio_haura_options *)td->eo)->disrespect_fio_direct) {
    betree_configuration_set_direct(cfg, td->o.odirect);
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
 * for a short read/write.
 */
static enum fio_q_status fio_haura_queue(struct thread_data *td,
                                         struct io_u *io_u) {
  struct err_t *error = NULL;
  size_t obj_num = *(size_t *)td->io_ops_data;
  struct obj_t *obj = global_data.objs[obj_num];
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
  // This function is intentially left empty.
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
    struct err_t *error = NULL;
    // betree_sync_db(global_data.db, &error);
    // if (error != NULL) {
    //   exit(bail(error));
    // }
    size_t obj_num = *(size_t *)td->io_ops_data;
    betree_object_close(global_data.objs[obj_num], &error);
    if (error != NULL) {
      exit(bail(error));
    }
    global_data.cnt -= 1;
    if (global_data.cnt == 0) {
      betree_close_db(global_data.db);
      global_data.db = NULL;
      global_data.obj_s = NULL;
      free(global_data.files);
    }
  }
  free(td->io_ops_data);
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
** Executed before the Init function
*/
static int fio_haura_setup(struct thread_data *td) {
  struct err_t *error = NULL;
  /* Force single process. */
  td->o.use_thread = 1;

  if (td->thread_number == 1) {

    /*
    ** This section may only ever get executed by the thread with thread_number
    *  1!
    *  Any other thread will deliver incorrect defaults, which are not safe to
    *  run.
    */
    global_data.jobs = td->o.numjobs;
    // Size = numjobs * files per job
    global_data.files =
        malloc(sizeof(char *) * global_data.jobs * td->files_index);
  }
  for (size_t idx = 0; idx < td->files_index; idx += 1) {
    global_data.files[(global_data.cnt * td->files_index) + idx] =
        td->files[idx]->file_name;
    /* Haura needs some additional space to provide extra data like object
     * pointers and metadata. This is more of a hack, but nonetheless. */
    creat(td->files[idx]->file_name, 0644);
    if (truncate(td->files[idx]->file_name,
                 max(td->o.file_size_high, td->o.size) + (50 * 1024 * 1024))) {
      fprintf(
          stderr,
          "Could not retruncate file to provide enough storage for Haura.\n");
    }
  }

  td->io_ops_data = malloc(sizeof(size_t));
  *(size_t *)td->io_ops_data = global_data.cnt;
  global_data.cnt += 1;

  // Initialize the database on last pass
  if (global_data.cnt == global_data.jobs) {
    betree_init_env_logger();
    struct cfg_t *cfg;

    if ((cfg = betree_configuration_from_env(&error)) == NULL) {
      return bail(error);
    }
    fio_haura_translate(td, cfg);

    int is_prefilled = 0;
    /*
    ** Checking for any pre-existing data we might be able to use.
    */
    if ((global_data.db = betree_open_db(cfg, &error)) == NULL ||
        td_write(td)) {
    new_db:
      if ((global_data.db = betree_create_db(cfg, &error)) == NULL) {
        return bail(error);
      }
      if ((global_data.obj_s = betree_create_object_store(
               global_data.db, "fio", 3, pref, &error)) == NULL) {
        return bail(error);
      }
    } else {
      /*
      ** Check if object store exists and objects are valid otherwise open new
      *db.
      */
      if ((global_data.obj_s = betree_create_object_store(
               global_data.db, "fio", 3, pref, &error)) == NULL) {
        betree_close_db(global_data.db);
        global_data.db = NULL;
        goto new_db;
      }

      char init[2] = {1};

      for (size_t idx = 0; idx < global_data.jobs; idx += 1) {
        init[1] += 1;

        int object_size = -1;
        if ((object_size = betree_object_get_size(global_data.obj_s, init, 2,
                                                  &error)) == -1) {
          betree_close_db(global_data.db);
          global_data.db = NULL;
          global_data.obj_s = NULL;
          goto new_db;
        }

        if (td->o.size > object_size) {
          betree_close_db(global_data.db);
          global_data.db = NULL;
          global_data.obj_s = NULL;
          goto new_db;
        }
      }

      // If we made it this far the data present is sufficient for the
      // benchmark. Good job!
      printf("haura: Reusing stored data from previous benchmark\n");
      is_prefilled = 1;
    }

    char init[2] = {1};
    global_data.objs = malloc(sizeof(struct obj_t *) * global_data.jobs);
    // Create a private object for each thread
    for (size_t idx = 0; idx < global_data.jobs; idx += 1) {
      init[1] += 1;
      if ((global_data.objs[idx] = betree_object_open_or_create(
               global_data.obj_s, init, 2, pref, &error)) == NULL) {
        return bail(error);
      }

      /* Due to limitations in the fio initialization process we prepopulate the
       * objects here, which is suboptimal but the only place possible due to
       * the order of execution. */
      if (!td_write(td) && !is_prefilled) {
        unsigned long long block_size = td->o.bs[DDIR_WRITE];
        unsigned long long max_io_size = td->o.size;
        void *buf = malloc(block_size);
        unsigned long long total_written = 0;
        // Fill buffer somewhat random
        for (unsigned long long off = 0; off < (block_size / 4); off += 1) {
          ((u_int32_t *)buf)[off] = random();
        }
        while (max_io_size > total_written) {

          unsigned long written = 0;
          betree_object_write_at(global_data.objs[idx], buf, block_size,
                                 total_written, &written, &error);
          if (error != NULL) {
            exit(bail(error));
          }
          total_written += written;
        }
        free(buf);
        printf("haura: prepopulated object %lu of %zu\n", idx + 1,
               global_data.jobs);
      }
    }
    betree_sync_db(global_data.db, &error);
    if (error != NULL) {
      exit(bail(error));
    }
  }

  return 0;
}

static int fio_haura_prepopulate_file(struct thread_data *td,
                                      struct fio_file *file) {

  /* fio wants this set, soo... */
  file->fd = open(file->file_name, 0, 0644);
  return 0;
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
    .prepopulate_file = fio_haura_prepopulate_file,
    .setup = fio_haura_setup,
    .options = options,
    .option_struct_size = sizeof(struct fio_haura_options),
    .flags = FIO_SYNCIO | FIO_DISKLESSIO | FIO_NOEXTEND | FIO_NODISKUTIL,
};
