//
// options.h
//
// Compile-time options for Yesquel
//

/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  All rights reserved. 

  Written by Marcos K. Aguilera

  MIT License

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the "Software"), to deal in the Software without restriction,
  including without limitation the rights to use, copy, modify, merge,
  publish, distribute, sublicense, and/or sell copies of the Software,
  and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
*/

#ifndef _OPTIONS_H
#define _OPTIONS_H

// COMMON OPTIONS -------------------------------------------------------------

#define SKIPLOG
// If defined, skip logging to disk.

//#define DISKLOG_NOFSYNC
// If defined, skip fsync when logging, which can cause data loss if
// power is lost.

#define DTREE_SPLIT_LOCATION 2
// Indicates where splits occur. 1=client, 2=servers. 2 is more efficient
// while 1 is better tested and more reliable.

#define DTREE_SPLIT_SIZE 50
// Number of cells above which to split a node. This must be at least 2 since
// we cannot split a node with only 2 cells

#define DTREE_SPLIT_SIZE_BYTES 8000 // node size (bytes) above which to split

//#define DTREE_LOADSPLITS
// If set and DTREE_SPLIT_LOCATION==2, then enable load splits.
// Load splits make Yesquel more efficient but it is less tested and therefore
// less reliable.

// YESQUEL SQL PROCESSOR OPTIONS ----------------------------------------------

#define YS_SCHEMA_CACHE 2
// How YS caches table schemas. The following options are available:
//     0: No cache. Less efficient than the other options.
//     1: Simple cache. Efficient but does not support schema changes
//     2: Consistent cache. Efficient, support schema changes with
//        strong consistency, but it is less tested and more bug-prone

// GENERAL OPTIONS ------------------------------------------------------------

#define GAIACONFIG_ENV "GAIACONFIG"
// Name of environment variable that, if set, indicates configuration file

#define GAIA_DEFAULT_CONFIG_FILENAME "config.txt"
// Default configuration file if environment variable is not set. This
// name is relative to the current working directory.

// DEBUG OPTIONS --------------------------------------------------------------

//#define NDEBUG
// If defined, these macros skip run-time debugging checks. The preferred
// way to set this option is via the makefile (see makefile.defs)

//#define DEBUGLOG
// If defined, enable the output of additional debugging information.

//#define DEBUGKVLOG
// If defined, enable the output of additional debugging information
// associated with key-value storage.

//#define DEBUGRELEASE
// Define this to enable debug printfs in release mode

#define GAIADEBUG_ENV_VAR "GAIADEBUGLOGFILE"
// Environment variable with path to file to store gaia debug log

#define GAIADEBUG_DEFAULT_FILE "debuglog.txt"
// Default debug log file if environment variable is not defined

//#define GAIA_DESTROY_MARK
// If defined, write a mark on certain objects being destroyed. This is
// useful for debugging memory but slows down the system.


// KEY-VALUE AND TRANSACTION OPTIONS -----------------------------------------

#define GAIA_WRITE_ON_PREPARE
// If defined, enables the optimization to piggyback small writes on
// prepare phase of transaction

#define GAIA_WRITE_ON_PREPARE_MAX_BYTES 4096
// Max # of bytes to piggyback on prepare phase if GAIA_WRITE_ON_PREPARE is set

#define PENDINGTX_HASHTABLE_SIZE 101
// Size of hash table for pending transactions. Each hash table bucket
// consists of a skiplist. The hash table is mostly useful for
// improving concurrency since at times an entire bucket is locked. The only
// reason to increase this is under transactions with an extremely large
// number of writes.

//#define DISABLE_ONE_PHASE_COMMIT
// If defined, avoids one-phase commit for transactions that affects only
// one server

//#define GAIA_OCC
// If defined, emulate optimistic concurrency control, by checking readsets
// and writesets on commit and by aborting if any object changed

//#define GAIA_NONCOMMUTATIVE
// If defined, updates to the same object always conflict causing a transaction
// to abort, so there are no commutative operations on an object.

//#define DISABLE_DELRANGE_DELRANGE_CONFLICTS
// If set, delranges never conflict with delranges, otherwise they always do.
// Setting this option could corrupt the distributed B-tree as a node may be
// left with no cells

// RPC and TCP OPTIONS -------------------------------------------------------

#define SERVER_DEFAULT_PORT 11223
// Default port number for storage server

#define CLIENT_WORKERTHREADS 1
// Number of worker threads for client. The system was designed to work
// with more than 1 client worker threads but it has not been tested.

#define SERVER_WORKERTHREADS 1
// Number of worker threads for server. The system was designed to work
// with more than 1 server worker threads but it has not been tested.

#define OUTSTANDINGREQUESTS_HASHTABLE_SIZE 101
// Size of hash table for outstanding RPC requests. This could be increased
// to save CPU if a client expects to large a very large number of outstanding
// requests.

#define TCP_RECLEN_DEFAULT 64000
// Size of buffers to receive network data


// IN-MEMORY LOG OPTIONS ----------------------------------------------------

#define LOG_CHECKPOINT_MIN_ITEMS 15
// Store checkpoint in in-memory log if find at least this many items.
// Checkpoints provide a tradeoff between memory and speed. More checkpoints
// consume more memory but permit faster execution by avoiding the need
// to replay operations.

#define LOG_CHECKPOINT_MIN_ADDITEMS 10
// Store checkpoint in in-memory log if find at least this many add items

#define LOG_CHECKPOINT_MIN_DELRANGEITEMS 1
// Store checkpoint in in-memory log if find at least this many delrange items.

#define COID_CACHE_HASHTABLE_SIZE 1159523
// Size of hash table for keeping the in-memory log.

#define COID_CACHE_HASHTABLE_SIZE_LOCAL 4001
// Size of hash table for keeping the in-memory log of the local
// key-value storage system.


// DISK LOG OPTIONS -----------------------------------------------------------

#define LOG_STALE_GC_MS 3000
// Entries older than this value, in ms, will be deleted from the in-memory
// log. Consequently, older versions of data will not be available. If
// a transaction needs such versions, it will abort. Thus, transactions
// that run for longer than this value will abort (if they read any data).


//#define DISKLOG_SIMPLE
// If defined, use a very simple logging algorithm, which just writes
// to the file descriptor directly. Otherwise, use a more complex algorithm
// that groups together many commits for efficiency.

#define FLUSH_FILENAME "kv.dat"
// Default filename where to dump/restore storage checkpoints using callserver.
// This functionality works irrespective of whether disk logging is employed
// or not.

#define WRITEBUFSIZE (64*1024*1024)
// Size of buffer used to group together writes that need to be flushed
// to disk.


// DISTRIBUTED B-TREE OPTIONS -------------------------------------------------

#define DTREE_SPLIT_CLIENT_MAX_RETRIES 100
// Maximum number of times for client to retry split before giving up.
// This is relevant only if DTREE_SPLIT_LOCATION=1. If a client gives
// up, the node is left unsplit, which may result in a large node
// but does not affect correctness.

#define STORAGESERVER_SPLITTER
// If defined, storage server implements splitter functionality.

//#define NOGAIA
// If defined, do not use the storage servers at all but rather
// keep all information locally at the client.

//#define NODIRECTSEEK
// If defined, disable the direct seek optimization.

#define DTREE_MAX_LEVELS 14 // max # of levels in tree
#define DTREE_ROOT_OID   0 // oid of root node
#define DTREE_SPLIT_MINSIZE 3 // minimum size of cell that can be split

//#define DTREE_SPLIT_DEFER_TS
// If set, defers start timestamp of split. This option is made obsolete by
// new commit protocol.

//#define DTREE_SPLIT_TSOFFSET 100
// Splitter will have a start timestamp this much into the past, so its reads
// will not abort others. This option is made obsolete by defer timestamp
// technique in new commit protocol.

#define DTREE_AVOID_DUPLICATE_INTERVAL 1000
// Avoid splitting the same item within this time interval, in ms. This is
// useful because there may be multiple requests to split the same node when
// only one split should occur.

//#define DTREE_NOFIRSTNODE
// If set, do not include a dummy first node for new distributed B-trees.
// The dummy node is useful to avoid split contention when the tree is still
// small, so it is probably best not to set this option.

#define DTREE_OPTIMISTIC_INSERT
// Use optimization of optimistic inserts.

//#define ALL_SPLITS_UNCONDITIONAL
// If defined, splitter server always tries to split a node, even if a recent
// identical request was made


// SQLITE OPTIONS -------------------------------------------------------------
// Do not change.

#define SQLITE_ENABLE_COLUMN_METADATA
//#define SQLITE_OMIT_SHARED_CACHE


// DEFINITIONS --------------------------------------------------------------
// What follows is not settable, just definitions and checks that
// should not be changed.

#ifndef NDEBUG
#define DEBUG          // define this to enable debug printfs in debug mode
#else
#define NODEBUG
#endif

#if SERVER_WORKERTHREADS==1 && !defined(LOCALSTORAGE)
#define SKIP_LOOIM_LOCKS    // do not lock looim. Should be used only if
                            // SERVER_WORKERTHREADS is 1 and this is not the
                            // client-side local storage
#endif

#if defined(SKIP_LOOIM_LOCKS) && SERVER_WORKERTHREADS != 1
#error SKIP_LOOIM_LOCKS should be used only when WORKERTHREADS is 1
#endif

#if DTREE_SPLIT_SIZE <= 1
#error DTREE_SPLIT_SIZE must be at least two, otherwise data will be corrupted
#endif

#if defined(DTREE_LOADSPLITS) && DTREE_SPLIT_LOCATION != 2
#error DTREE_LOADSPLITS works only when DTREE_SPLIT_LOCATION=2
#endif

#if YS_SCHEMA_CACHE == 2
#define GAIA_CLIENT_CONSISTENT_CACHE
// If set, enable the consistent client cache in the key-value storage system.
// This cache is designed only for items that are rarely written since it adds
// delays to writes. It is currently used only for a few items, such as
// table metadata (which contains the schema version number).
#endif

#endif
