//
// disklog.cpp
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

#include <stdio.h>

#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <malloc.h>
#include <signal.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>
#include <ctype.h>
#include <stddef.h>
#include <unistd.h>

#include <map>
#include <list>
#include <set>

#include "tmalloc.h"
#include "pendingtx.h"
#include "disklog.h"
#include "diskstorage.h"

#ifdef SKIPLOG
DiskLog::DiskLog(const char *logname){
  RawWritebuf = Writebuf = 0;
  WritebufSize = WritebufLeft = 0;
  WritebufPtr = 0;
  FileOffset = 0;
  WriteQueueHead = WriteQueueTail = 0;
  diskLogThreadNo = -1;
}

DiskLog::~DiskLog(){
}
void DiskLog::writeWqi(WriteQueueItem *wqi){}
void DiskLog::logCommitAsync(Tid tid, Timestamp ts){}
void DiskLog::logAbortAsync(Tid tid, Timestamp ts){}
int DiskLog::logUpdatesAndYesVote(Tid tid, Timestamp ts, Ptr<PendingTxInfo> pti,
                                  void *notify){
  return 0; // indicates no notification will happen
}
void DiskLog::launch(void){}
void DiskLog::test(int niter){}

#else
#include "task.h"

// auxilliary function called by logCommitAsync and logAbortAsync
static void logAsync(LogEntry *le);
// sends a write request to the PROGShipDiskReqs task
static void SendDiskLog(WriteQueueItem *wqi);
// immediate function to enqueue a disk request for the PROGShipDiskReqs task
static void ImmediateFuncEnqueueDiskReq(TaskMsgData &msgdata,
                                        TaskScheduler *ts, int srcthread);
static int PROGShipDiskReqs(TaskInfo *ti);

DiskLog::DiskLog(const char *logname){
  char *str, *ptr, *lastptr;

  str = new char[strlen(logname)+1];
  strcpy(str, logname);

  // find the last separator in logname
  ptr = str;
  do {
    lastptr = ptr;
    ptr = DiskStorage::searchseparator(ptr);
  } while (*ptr);
  *lastptr = 0;

#ifdef DISKLOG_SIMPLE
  RawWritebuf = Writebuf = 0;
  WritebufSize = WritebufLeft = 0;
#else
  // allocate writebuf
  RawWritebuf = new char[WRITEBUFSIZE+ALIGNBUFSIZE-1];
  if ((unsigned)(long long)RawWritebuf & (ALIGNBUFSIZE-1)){ // does not align
    Writebuf = RawWritebuf + (ALIGNBUFSIZE - ((unsigned)(long long)
                                              RawWritebuf & (ALIGNBUFSIZE-1)));
  } else Writebuf = RawWritebuf;
  assert(((unsigned)(long long)Writebuf & (ALIGNBUFSIZE-1))==0 &&
         Writebuf >= RawWritebuf);
  WritebufSize = ALIGNLEN((int)(RawWritebuf + WRITEBUFSIZE + ALIGNBUFSIZE-1 -
                                Writebuf));
  assert(WritebufSize >=ALIGNBUFSIZE);
  WritebufLeft = WritebufSize;
#endif

  WritebufPtr = Writebuf;
  FileOffset = 0;

  // allocate dummy nodes for WriteQueue and NotifyQueue
  WriteQueueHead = WriteQueueTail = new WriteQueueItem;
  memset(WriteQueueHead, 0, sizeof(WriteQueueItem));
  WriteQueueHead->next = 0;

  // create path up to filename
  DiskStorage::Makepath(str);

#ifndef DISKLOG_SIMPLE
  f = open(logname, O_CREAT | O_TRUNC | O_WRONLY | O_DIRECT, 0644);
#else
  f = open(logname, O_CREAT | O_TRUNC | O_WRONLY, 0644);
#endif
  if (f<0){
    printf("Disklog: cannot create %s (errno %d)\n", logname, errno);
    exit(1);
  }

  diskLogThreadNo = -1;
  
  delete [] str;
}

DiskLog::~DiskLog(){
  if (diskLogThreadNo != -1){
    TaskMsg msg;
    msg.dest = TASKID_CREATE(diskLogThreadNo, IMMEDIATEFUNC_EXIT);
    msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
    tgetTaskScheduler()->sendMessage(msg);
    SLauncher->waitThread(diskLogThreadNo);
  }
  if (f >= 0) close(f);
  if (RawWritebuf) delete [] RawWritebuf;
}

// auxilliary function for disklog write to log a WriteQueueItem

void DiskLog::writeWqi(WriteQueueItem *wqi){
  int type;
  int celltype;

  if (wqi->utype == 0){
    BufWrite(wqi->u.buf.buf, wqi->u.buf.len);
  } else { // wqi->utype == 1

    MultiWriteLogEntry mwle;
    //MultiWriteLogSubEntry mwlse;
    //list<TxWriteItem *> *writeSet = wqi->u.writes.writeSet;
    Ptr<PendingTxInfo> pti = wqi->u.updates.pti;

    // write header
    mwle.let = LEMultiWrite;
    mwle.tid = wqi->u.updates.tid;
    mwle.ts = wqi->u.updates.ts;
    mwle.ncoids = pti->coidinfo.getNitems();
    BufWrite((char*) &mwle, sizeof(MultiWriteLogEntry));

    // iterator over all objects
    SkipListNode<COid, Ptr<TxUpdateCoid> > *it;
    for (it = pti->coidinfo.getFirst(); it != pti->coidinfo.getLast();
         it = pti->coidinfo.getNext(it)){
      Ptr<TxUpdateCoid> tucoid = it->value;
      if (tucoid->Writevalue) type = 1;
      else if (tucoid->WriteSV) type = 2;
      else type = 0;
      BufWrite((char*) &type, sizeof(int));

      if (type == 0){ // write a delta record
        int len;
        BufWrite((char*)tucoid->SetAttrs, GAIA_MAX_ATTRS);
        BufWrite((char*)tucoid->Attrs, sizeof(u64)*GAIA_MAX_ATTRS);
        len = (int) tucoid->Litems.getNitems(); // number of items
        BufWrite((char*)&len, sizeof(int));
        // for each item
        for (TxListItem *tli = tucoid->Litems.getFirst();
             tli != tucoid->Litems.getLast();
             tli = tucoid->Litems.getNext(tli)){
          BufWrite((char*)&tli->type, sizeof(int));
          if (tli->type == 0){
            TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(tli);
            // item
            BufWrite((char*)&tlai->item.nKey, sizeof(int));
            if (!tlai->item.pKey) celltype=0; // int key
            else celltype=1;
            BufWrite((char*)&celltype, sizeof(int));
            if (celltype) BufWrite((char*)tlai->item.pKey,
                                   (int) tlai->item.nKey);
            BufWrite((char*) &tlai->item.value, sizeof(u64));
          } else { // tli->type == 1
            TxListDelRangeItem *tldri = dynamic_cast<TxListDelRangeItem*>(tli);
            BufWrite((char*) &tldri->intervalType, 1);
            // itemstart
            BufWrite((char*) &tldri->itemstart.nKey, sizeof(int));
            if (!tldri->itemstart.pKey) celltype=0; // int key
            else celltype = 1;
            BufWrite((char*) &celltype, sizeof(int));
            if (celltype) BufWrite((char*)tldri->itemstart.pKey,
                                   (int)tldri->itemstart.nKey);
            BufWrite((char*) &tldri->itemstart.value, sizeof(u64));
            // itemend
            BufWrite((char*) &tldri->itemend.nKey, sizeof(int));
            if (!tldri->itemend.pKey) celltype=0; // int key
            else celltype = 1;
            BufWrite((char*) &celltype, sizeof(int));
            if (celltype) BufWrite((char*)tldri->itemend.pKey,
                                   (int)tldri->itemend.nKey);
            BufWrite((char*) &tldri->itemend.value, sizeof(u64));
          }
        }
      } else if (type == 1){ // write a value record
        TxWriteItem *twi = tucoid->Writevalue;
        BufWrite((char*) &twi->len, sizeof(int));
        BufWrite((char*)twi->buf, twi->len);
      } else { // type == 2
	int nitems;
        // write a supervalue record
        TxWriteSVItem *twsvi = tucoid->WriteSV;
        BufWrite((char*)twsvi, offsetof(TxWriteSVItem, attrs)); // header
        BufWrite((char*)twsvi->attrs, sizeof(u64) * twsvi->nattrs);
	nitems = twsvi->cells.getNitems();
        BufWrite((char*) &nitems, sizeof(int)); // number of cells
        // for each cell
        SkipListNodeBK<ListCellPlus,int> *ptr;
        for (ptr = twsvi->cells.getFirst(); ptr != twsvi->cells.getLast();
             ptr = twsvi->cells.getNext(ptr)){
          ListCellPlus *lc = ptr->key;
          BufWrite((char*) &lc->nKey, sizeof(int));
          if (!lc->pKey) celltype=0; // int key
          else celltype = 1;
          BufWrite((char*) &celltype, sizeof(int));
          if (celltype) BufWrite((char*)lc->pKey, (int)lc->nKey);
          BufWrite((char*) &lc->value, sizeof(u64));
        }
      }
    }
    // log a yes vote
    LogEntry le;
    le.let = LEVoteYes;
    le.tid = wqi->u.updates.tid;
    le.ts.setIllegal();
    BufWrite((char*) &le, sizeof(LogEntry));
  }
}

void logAsync(LogEntry *le){
  char *buf;
  int len;
  len = sizeof(LogEntry);
  buf = new char[len];
  memcpy((void*)buf, (void*)le, len);

  WriteQueueItem *wqi = new WriteQueueItem;
  wqi->utype = 0;
  wqi->u.buf.tofree = 1;
  wqi->u.buf.buf = buf;
  wqi->u.buf.len = len;
  wqi->notify = 0;
  SendDiskLog(wqi);
}

void DiskLog::logCommitAsync(Tid tid, Timestamp ts){
  LogEntry le;
  le.let = LECommit;
  le.tid = tid;
  le.ts = ts;
  logAsync(&le);
}

void DiskLog::logAbortAsync(Tid tid, Timestamp ts){
  LogEntry le;
  le.let = LEAbort;
  le.tid = tid;
  le.ts = ts;
  logAsync(&le);
}

int DiskLog::logUpdatesAndYesVote(Tid tid, Timestamp ts, Ptr<PendingTxInfo> pti,
                                  void *notify){
  WriteQueueItem *wqi = new WriteQueueItem();
  wqi->utype = 1;
  wqi->u.updates.tid = tid;
  wqi->u.updates.ts = ts;
  wqi->u.updates.pti = pti;
  wqi->notify = notify;
  SendDiskLog(wqi);
  if (notify) return 1; // indicate that log will be done in the background,
                        //with notification happening subsequently
  else return 0; // indicate that no notification will happen
}

void ImmediateFuncEnqueueDiskReq(TaskMsgData &msgdata, TaskScheduler *ts,
                                 int srcthread){
  DiskLogThreadContext *dltc = (DiskLogThreadContext*)
    tgetSharedSpace(THREADCONTEXT_SPACE_DISKLOG);
  WriteQueueItem *wqi = *(WriteQueueItem**) &msgdata;
  wqi->next = 0;

  // put disk request in tail of ToShip link list
  assert(dltc->ToShipTail->next == 0);
  dltc->ToShipTail->next = wqi;
  dltc->ToShipTail = wqi;
  ts->wakeUpTask(dltc->psdrtask); // wake up PROGShipDiskReqs task
}

int DiskLog::PROGShipDiskReqs(TaskInfo *ti){
  DiskLogThreadContext *dltc = (DiskLogThreadContext*)
    tgetSharedSpace(THREADCONTEXT_SPACE_DISKLOG);
  DiskLog *dl = (DiskLog*) ti->getTaskData();
  WriteQueueItem *wqi, *next;

  if (dltc->ToShipHead->next){ // if ToShip not empty
    // write items
    for (wqi = dltc->ToShipHead->next; wqi != 0; wqi = wqi->next){
      dl->writeWqi(wqi);
    }
    dl->BufFlush();

    // send notifications
    for (wqi = dltc->ToShipHead->next; wqi != 0; wqi = next){
      if (wqi->notify){
        // send a message to wqi->notify
        TaskMsg msg;
        msg.dest = (TaskInfo*) wqi->notify;
        msg.flags = 0;
        memset(&msg.data, 0, sizeof(TaskMsgData));
        msg.data.data[0] = 0xb0; // check byte only (message carries no
                                 // relevant data; it is just a signal)
        tsendMessage(msg);
      }
      next = wqi->next;
      delete wqi;
    }
    // clear list
    dltc->ToShipHead->next = 0;
    dltc->ToShipTail = dltc->ToShipHead;
  }

  return SchedulerTaskStateWaiting;
}

void DiskLog::launch(void){
  if (diskLogThreadNo == -1){ // not launched yet
    diskLogThreadNo = SLauncher->createThread("DISKLOG", diskLogThread,
                                              (void*) this, 0);
  }
}

OSTHREAD_FUNC DiskLog::diskLogThread(void *parm){
  DiskLog *dl = (DiskLog*) parm;
  TaskScheduler *ts = tgetTaskScheduler();

  dl->init();
  ts->run();
  return (OSThread_return_t)-1;
}

void DiskLog::init(void){
  TaskScheduler *ts = tgetTaskScheduler();
  DiskLogThreadContext *dltc;
  TaskInfo *ti;
  
  assert(threadContext);
  int threadno = tgetThreadNo();
  gContext.setNThreads(TCLASS_DISKLOG, 1); 
  gContext.setThread(TCLASS_DISKLOG, 0, threadno);

  // assign immediate functions and tasks
  ts->assignImmediateFunc(IMMEDIATEFUNC_ENQUEUEDISKREQ,
                          ImmediateFuncEnqueueDiskReq);
  dltc = new DiskLogThreadContext;
  tsetSharedSpace(THREADCONTEXT_SPACE_DISKLOG, dltc);
  ti = ts->createTask(PROGShipDiskReqs, this);
  dltc->psdrtask = ti;
  //ts->assignFixedTask(FIXED TASK NUMBER HERE, ti);
}

void SendDiskLog(WriteQueueItem *wqi){
  sendIFMsg(gContext.hashThread(TCLASS_DISKLOG, 0),
            IMMEDIATEFUNC_ENQUEUEDISKREQ, (void*) &wqi,
            sizeof(WriteQueueItem*));
}

void DiskLog::test(int niter){
  u64 counter;
  int nwrites = 1; // number of BufWrites before BufFlush, which gradually
                   // increases
  int nwriteCount = 0;
  
  for (counter = 0; counter < (u64)niter; ++counter){
    BufWrite((char*) &counter, sizeof(u64));
    if (++nwriteCount == nwrites){
      BufFlush();
      ++nwrites;
      nwriteCount = 0;
      putchar('*');
    }
  }
  BufFlush();
}


#ifdef DISKLOG_SIMPLE
// simple version without flushing to disk
void DiskLog::auxwrite(char *buf, int buflen){}
void DiskLog::BufFlush(){
#ifndef DISKLOG_NOFSYNC
  int res = fdatasync(f); assert(res==0);
#endif
}

void DiskLog::BufWrite(char *buf, int len){
  unsigned long written;

  while (len > 0){
    written = write(f, buf, len);
    if (written < 0){
      printf("Disklog: write() error %d\n", errno);
      exit(1);
    }
    len -= written;
    buf += written;
  }
}

#else // ifdef DISKLOG_SIMPLE
// version that uses queues to flush to disk synchronously

// Writes buf to disk. Assumes buf is aligned and that it is
// legal to read it and fill it with zeroes until buflen + the next align point.
// This is true for Writebuf, and this function is currently used
// only when buf == Writebuf.
// The last block written, if partial, is copied to the beginning of the
// Writebuf. WritebufLeft and WritebufPtr are set accordingly.
// Also sets FileOffset and seeks file handle so that they
// reflect the beginning of Writebuf.
void DiskLog::auxwrite(char *buf, int buflen){
  unsigned long written;
  int writelen;

  // number of bytes to write, must be aligned, so move forward
  writelen = ALIGNLEN(buflen + (ALIGNBUFSIZE-1));

  assert(writelen >= buflen);

  if (writelen != buflen) // zero-fill missing gap (gap until alignment point)
    bzero(buf+buflen, writelen - buflen);

  // this loop writes writelen bytes starting at buf
  while (writelen > 0){
    written = write(f, buf, writelen);
    if (written < 0){
      printf("Disklog: write() error %d\n", errno);
      exit(1);
    }
    writelen -= written;
    buf += written;
  }

#ifndef DISKLOG_NOFSYNC
  int res = fdatasync(f); assert(res==0);
#endif  

  char *lastblock = Writebuf+ALIGNLEN(buflen); //beginning of last block written
  if (lastblock != Writebuf){
    // move last block to beginning to Writebuf
    memmove(Writebuf, lastblock, ALIGNMOD(buflen));
  }

  // adjust WritebufLeft and WritebufPtr
  WritebufPtr = Writebuf + ALIGNMOD(buflen);
  WritebufLeft = WritebufSize - ALIGNMOD(buflen);

  // adjust file offset
  FileOffset = FileOffset+ALIGNLEN(buflen); // offset at the beginning of buffer
  assert(FileOffset == ALIGNLEN(FileOffset));
  lseek(f, FileOffset, 0);
}

void DiskLog::BufFlush(void){
  auxwrite(Writebuf, WritebufSize - WritebufLeft);
  assert(WritebufLeft == Writebuf + WritebufSize - WritebufPtr);
}

void DiskLog::BufWrite(char *buf, int len){
  while (len >= WritebufLeft){
    memcpy((void*) WritebufPtr, buf, WritebufLeft);
    WritebufPtr += WritebufLeft;
    buf += WritebufLeft;
    len -= WritebufLeft;
    WritebufLeft = 0;
    BufFlush(); // flush buffer
  }
  assert(len >= 0);
  // now len < WritebufLeft
  assert(len < WritebufLeft);
  if (len){
    memcpy((void*) WritebufPtr, buf, len);
    WritebufPtr += len;
    WritebufLeft -= len;
  }
}
#endif // else DISKLOG_SIMPLE
#endif // else SKIPLOG
