//
// disklog.h
//
// Disk log of transactions
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

#ifndef _DISKLOG_H
#define _DISKLOG_H

#include "options.h"

class TaskInfo;

#define ALIGNBUFSIZE 4096    // must be power of 2

#define ALIGNLEN(x)  ((x) & ~(ALIGNBUFSIZE-1)) // clear low bits of x so it
                                               // is aligned
#define ALIGNMOD(x)  ((x) & (ALIGNBUFSIZE-1))    // low bits of x

#include <list>

#include "tmalloc.h"
#include "gaiatypes.h"
#include "pendingtx.h"

using namespace std;

enum LogEntryType { LEMultiWrite, LECommit, LEAbort, LEVoteYes };

struct LogEntry {
  LogEntryType let; // for LECommit, LEAbort, LEVoteYes
  Tid tid;
  Timestamp ts;
};

struct MultiWriteLogEntry {
  LogEntryType let; // for LEMultiWrite
  Tid tid;
  Timestamp ts;
  int ncoids;   // number of objects in this entry
};

struct WriteQueueItemBuf {
  int tofree; // whether to free buf afterwards
  char *buf;
  int len;
};

struct WriteQueueItemUpdates {
  Tid tid;
  Timestamp ts;
  Ptr<PendingTxInfo> pti;
};

struct WriteQueueItem {
  int utype;
  struct {
    WriteQueueItemBuf buf; // type 0
    WriteQueueItemUpdates updates; // type 1
  } u;

  void *notify;         // taskinfo to notif
  WriteQueueItem *next; // linklist
  WriteQueueItem(){ utype = -1; }
  ~WriteQueueItem(){
    if (utype==0 && u.buf.tofree && u.buf.buf) delete u.buf.buf;
  }
};

struct DiskLogThreadContext {
  WriteQueueItem *ToShipHead, *ToShipTail; // link list with dummy head node
  TaskInfo *psdrtask; // taskinfo for PROGShipDiskReqs
  DiskLogThreadContext(){
    ToShipHead = ToShipTail = new WriteQueueItem;
    memset(ToShipHead, 0, sizeof(WriteQueueItem));
    ToShipHead->next = 0;
  }
  ~DiskLogThreadContext(){
    WriteQueueItem *wqi, *next;
    wqi = ToShipHead;
    while (wqi){
      next = wqi->next;
      delete wqi;
      wqi = next;
    }
  }
};

class TaskInfo;

class DiskLog {
private:
  int f; // file handle

  char *RawWritebuf;     // unaligned buffer as returned by new()
  char *Writebuf;        // aligned buffer to be used
  unsigned WritebufSize; // length of aligned buffer
  int WritebufLeft;      // number of bytes left in buffer
  char *WritebufPtr;     // current location in buffer
  u64 FileOffset;        // current offset in file being written

  WriteQueueItem *WriteQueueHead, *WriteQueueTail;  // head and tail of
                                                   // write queue

  void BufWrite(char *buf, int len);   // buffers a write to disk;
                                       // calls AlignWrite
  void auxwrite(char *buf, int len);   // writes an aligned buffer
  void BufFlush(void);                 // flushes write done

  void writeWqi(WriteQueueItem *wqi);  // writes a WriteQueueItem
                                       // (calls BufWrite several times)

  static int PROGShipDiskReqs(TaskInfo *ti);

  int diskLogThreadNo;  // if diskLogThread is running, its thread number
  static OSTHREAD_FUNC diskLogThread(void *parm);  // example of a thread
                                                   // to run disklog

public:
  DiskLog(const char *logname);
  ~DiskLog();
  void launch(void);  // creates disklog thread. If the thread is to do
       // other work, then caller should create the thread herself and then
       // invoke init() below within the thread.
  
  void init(void); // registers immediate function and creates task.
                   // Subsequently, caller should invoke run() on the task
                   // scheduler (or periodically invoke runOnce() ). This
                   // should not be called if calling launch(), since the
                   // thread created by launch() will already call init().


  // --------- client functions, called to log various things -------------

  // Logs updates and Yes Vote
  // Returns 0 if log has been done, non-zero if log being done in backgruond
  // Notification happens only if it returns non-zero
  static int logUpdatesAndYesVote(Tid tid, Timestamp ts,
                                  Ptr<PendingTxInfo> pti, void *notify); 

  // log a commit record
  static void logCommitAsync(Tid tid, Timestamp ts);
  // log an abort record
  static void logAbortAsync(Tid tid, Timestamp ts);

  // runs a test that logs consecutive integers from 0 to niter-1,
  // flushing batches of increasingly larger sizes
  void test(int niter);
};

#endif
