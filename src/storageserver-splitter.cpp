//
// storageserver-splitter.cpp
//
// Splitter RPCs at the storage server
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
#include <stdarg.h>
#include <float.h>
#include <poll.h>
#include <sys/eventfd.h>

#include <map>
#include <list>

#include "tmalloc.h"
#include "os.h"
#include "options.h"
#include "debug.h"
#include "task.h"
#include "tcpdatagram.h"
#include "grpctcp.h"

#include "gaiarpcaux.h"
#include "warning.h"
#include "util-more.h"

#include "gaiatypes.h"
#include "util.h"
#include "datastruct.h"
#include "newconfig.h"
#include "clientlib.h"
#include "clientdir.h"
#include "dtreeaux.h"

#include "dtreesplit.h"
#include "storageserver-splitter.h"
#include "splitter-client.h"
#include "splitterrpcaux.h"
#include "loadstats.h"

StorageConfig *SC=0;

struct PendingSplitItem {
  int retry;  // 0=no retry requested, 1=retry requested
  PendingSplitItem() : retry(0) {}
};

struct COidListItem {
  COid coid;
  int threadno;
  COidListItem *prev, *next; // linklist stuff
  COidListItem() : threadno(-1) { coid.cid = (Cid)-1; coid.oid = (Oid)-1; }
  COidListItem(COid &c, int tno) : coid(c), threadno(tno){}
};

// maintains the rowid counters for each cid
class RowidCounters {
private:
  SkipList<COid,i64> rowidmap;
public:
  // lookup a cid. If found, increment rowid and return incremented value.
  // If not found, store hint and return it.
  i64 lookup(Cid cid, i64 hint){
    int res;
    i64 *rowidptr;
    i64 rowid;
    COid coid;

    coid.cid = cid;
    coid.oid = 0;

    res = rowidmap.lookupInsert(coid, rowidptr);
    if (res==0) rowid = ++(*rowidptr); // found it
    else rowid = *rowidptr = hint; // not found
    return rowid;
  }

  i64 lookupNohint(Cid cid){
    int res;
    i64 *rowidptr;
    i64 rowid;
    COid coid;

    coid.cid = cid;
    coid.oid = 0;
    res = rowidmap.lookup(coid, rowidptr);
    if (res==0) rowid = ++(*rowidptr); // found it
    else rowid = 0; // not found
    return rowid;
  }
};

// state kept by each worker thread about pending split requests,
// load statistics for load splits,
// stats of the splitter (got from the splitter thread),
// rowid counters (this is not exactly for the splitter, but it is kept here
// too), and current throttling
class ServerSplitterState {
public:
  SkipList<COid,PendingSplitItem*> PendingSplits;
  LinkList<COidListItem> PendingResponses;
  SplitterStats Stats;
  LoadStats Load;
  RowidCounters RC;
  TaskInfo *tiProgSplitter;
  SplitterThrottle throttle;
  ServerSplitterState(){}
};

int ExtractQueueFromServerSplitterState(void *sss){
  return ((ServerSplitterState*)sss)->Stats.splitQueueSize;
}

// extracts the throttle object from a ServerSplitterState
// intended to be used by other module, which do not know about
// ServerSplitterState.
// We don't want to expose ServerSplitterState to other modules since it has
// a lot of irrelevant internal stuff.
SplitterThrottle *ExtractThrottleFromServerSplitterState(void *sss){
  return & ((ServerSplitterState*)sss)->throttle;
}


struct TaskMsgDataSplitterNewWork {
  COid coid; // coid to split
  ListCellPlus *cell; // cell where to split (more precisely, cell indicates
                        // first cell in second node). If 0 then split in half.
  int where; // if 0, put new work item at head (unusual), otherwise put it at
            // tail (more common)
  //TaskMsgDataSplitterNewWork(COid &c, int w) : coid(c), where(w) {}
};

struct TaskMsgDataSplitterReply {
  SplitterStats stats; //
  COid coid; // coid that was just split, if stat.splitTimeRetryingMs == 0,
             // otherwise invalid
};

void SendIFSplitterThreadNewWork(TaskScheduler *myts, COid &coid,
                                 ListCellPlus *cell, int where);
Marshallable *ss_getrowidRpc(GetRowidRPCData *d);
int PROGSplitter(TaskInfo *ti);
void ImmediateFuncSplitterHandleReportWork(TaskMsgData &msgdata,
                                           TaskScheduler *ts, int srcthread);

/******************************************************************************
 *                                                                            *
 *               Functions called by the RPC worker threads                   *
 *                                                                            *
 ******************************************************************************/

// Worker thread calls this function to request a node to be split. The split
// is done by the splitter thread, so this function sends an IF for it to do
// the split.
// If cell==0, the split is done in the middle of the node.
// If cell!=0, the split is done at the indicated cell, which becomes the
// first cell of the split node. This cell should not be the first cell in
// the node. The function will own cell.
void SplitNode(COid &coid, ListCellPlus *cell){
  PendingSplitItem **psipp, *psip;
  TaskScheduler *ts;
  ServerSplitterState *SS = (ServerSplitterState*)
    tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  ts = tgetTaskScheduler();

  // check PendingSplits to see if a split is pending for d->data->coid
  if (!SS->PendingSplits.lookupInsert(coid, psipp)) {
    // found item already, so set retry=1, so that we attempt to split coid
    // again when the current split is done
    (**psipp).retry = 1;
  }
  else {
    // otherwise ask the worker to perform the split (send an immediatefunc
    // message to it)
    psip = *psipp = new PendingSplitItem();
    psip->retry = 0;
    SendIFSplitterThreadNewWork(ts, coid, cell, 1); // request retry to
                                                   // Splitter thread
  }
}

// Reports access to a cell within a coid for load splitting. Periodically
// check if a load split is needed and, if so, call SplitNode to get it.
//
// The cell object and its associated RcKeyInfo will be owned by the reporting
// data structure
void ReportAccess(COid &coid, ListCellPlus *cell){
  ServerSplitterState *SS = (ServerSplitterState*)
    tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);
  assert(SS);
  SS->Load.report(coid, cell);
  SS->Load.check();
}

// getrowid RPC implementation
int ss_getrowidRpcStub(RPCTaskInfo *rti){
  GetRowidRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = ss_getrowidRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
Marshallable *ss_getrowidRpc(GetRowidRPCData *d){
  GetRowidRPCRespData *resp;
  i64 rowid;
  ServerSplitterState *SS = (ServerSplitterState*)
    tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  if (d->data->hint) rowid = SS->RC.lookup(d->data->cid, d->data->hint);
  else rowid = SS->RC.lookupNohint(d->data->cid);

  //printf("GetRowidRPC cid %llx hint %lld rowid %lld\n",
  //       (long long)d->data->cid, (long long)d->data->hint, (long long)rowid);

  resp = new GetRowidRPCRespData;
  resp->data = new GetRowidRPCResp;
  resp->freedata = true;
  resp->data->rowid = rowid;
  dprintf(1, "GETROWID cid %llx hint %lld resp %lld", (long long)d->data->cid,
          (long long)d->data->hint, (long long)rowid);
  return resp;
}

// split item for thread to process
struct ThreadSplitItem {
  COid coid;
  ListCellPlus *cell;
  int srcthread; // threadno that generated request
  u64 starttime;
  ThreadSplitItem *prev, *next; // linklist stuff
  ~ThreadSplitItem(){ if (cell) delete cell; }
  ThreadSplitItem() : cell(0), starttime(0) {
    coid.cid=(Cid)-1;
    coid.oid=(Oid)-1;
  }
  ThreadSplitItem(COid &c, ListCellPlus *cel, int st) : coid(c), cell(cel),
                                                        srcthread(st), starttime(0) { }
};


// this gets called at initialization of each of the RPC worker threads
void initServerTask(TaskScheduler *ts){ // call it from each of the
                                        // worker threads
  TaskInfo *ti;
  ServerSplitterState *SS = new ServerSplitterState;
  tsetSharedSpace(THREADCONTEXT_SPACE_SPLITTER, SS);

  ti = ts->createTask(PROGSplitter, 0); // creates task and assign it as a
                                        // fixed task
  SS->tiProgSplitter = ti;
  ts->assignImmediateFunc(IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK,
                          ImmediateFuncSplitterHandleReportWork);
}

// sends an IF to splitter thread with request for new work
void SendIFSplitterThreadNewWork(TaskScheduler *myts, COid &coid,
                                 ListCellPlus *cell, int where){
  TaskMsgDataSplitterNewWork tmdsnw;
  assert(sizeof(TaskMsgDataSplitterNewWork) <= sizeof(TaskMsgData));
  tmdsnw.coid = coid;
  tmdsnw.cell = cell;
  tmdsnw.where = where;
  sendIFMsg(gContext.getThread(TCLASS_SPLITTER, 0),
            IMMEDIATEFUNC_SPLITTERTHREADNEWWORK, &tmdsnw,
            sizeof(TaskMsgDataSplitterNewWork));
}

// Handle reports of work from ServerSplitterThread.
// Registered as IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK.
// This will enqueue the response for processing in the PROGSplitter below.
void ImmediateFuncSplitterHandleReportWork(TaskMsgData &msgdata,
                                           TaskScheduler *ts, int srcthread){
  TaskMsgDataSplitterReply *tmdsr = (TaskMsgDataSplitterReply*) &msgdata;
  ServerSplitterState *SS = (ServerSplitterState*)
    tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  SS->Stats = tmdsr->stats; // record stats
  SS->throttle.ReportLoad(tmdsr->stats); // update splitter load information
    
  if (tmdsr->stats.dest && tmdsr->stats.splitTimeRetryingMs == 0){
    // our object and split just finished
    SS->PendingResponses.pushTail(new COidListItem(tmdsr->coid, srcthread));
    ts->wakeUpTask(SS->tiProgSplitter); // wake up PROGSplitter
  }
}

// This PROG is run at each worker thread to taking pending responses from
// the splitter, match them with the pending splits, and either remove it
// from the pending splits or reissue the split if retry is true
int PROGSplitter(TaskInfo *ti){
  int res;
  COidListItem *coidli;
  PendingSplitItem **psipp, *psip;
  TaskScheduler *ts = tgetTaskScheduler();
  ServerSplitterState *SS = (ServerSplitterState*)
    tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  while (!SS->PendingResponses.empty()){
    coidli = SS->PendingResponses.popHead();
    res = SS->PendingSplits.lookup(coidli->coid, psipp);
    if (!res){ // found
      psip = *psipp;
      if (psip->retry){
        psip->retry = 0;
        SendIFSplitterThreadNewWork(ts, coidli->coid, 0, 1); // request retry
                                                        // to Splitter thread
      }
      else { // done with item
        SS->PendingSplits.lookupRemove(coidli->coid, 0, psip);
        delete psip;
      }
    }

    delete coidli;
  }
  return SchedulerTaskStateWaiting; // sleep until waken up again
}

/******************************************************************************
 *                                                                            *
 *                        Splitter Thread functions                           *
 *                                                                            *
 ******************************************************************************/

class SplitStats {
public:
  SplitStats() : average(SPLITTER_STAT_MOVING_AVE_WINDOW) {
    timeRetryingMs = 0;
  }
  MovingAverage average; // moving average time of successful splits
  u64 timeRetryingMs;     // time spent retrying split thus far
                          // (0 if no ongoing retries)
};

struct ServerSplitterThreadState {
  SplitStats Stats;
  LinkList<ThreadSplitItem> ThreadSplitQueue;
};

SplitStats *Stats=0;
ServerSplitterThreadState *TSS = 0;

OSTHREAD_FUNC ServerSplitterThread(void *parm);

// Creates splitter thread. This gets called once only
void initServerSplitter(){
  int threadno;
  TSS = new ServerSplitterThreadState; assert(TSS);

  threadno = SLauncher->createThread("ServerSplitter", ServerSplitterThread,
                                     0, false);
  gContext.setNThreads(TCLASS_SPLITTER, 1); 
  gContext.setThread(TCLASS_SPLITTER, 0, threadno);
}

void ImmediateFuncSplitterThreadNewWork(TaskMsgData &msgdata,
                                        TaskScheduler *ts, int srcthread){
  TEvent e;
  TaskMsgDataSplitterNewWork *nw = (TaskMsgDataSplitterNewWork*) &msgdata;
  ThreadSplitItem *tsi = new ThreadSplitItem(nw->coid, nw->cell, srcthread);
  assert(tsi);
  if (nw->where==0) TSS->ThreadSplitQueue.pushHead(tsi);
  else TSS->ThreadSplitQueue.pushTail(tsi);
}

// remove repeated elements from split queue
void cleanupThreadSplitQueue(void){
  ThreadSplitItem *ptr, *next;
  Set<COid> existing;
  for (ptr = TSS->ThreadSplitQueue.getFirst();
       ptr != TSS->ThreadSplitQueue.getLast();
       ptr = next){
    next = TSS->ThreadSplitQueue.getNext(ptr);
    if (existing.insert(ptr->coid)){ // item already exists
      TSS->ThreadSplitQueue.remove(ptr);
    }
  }
}

void SendIFThreadReportWork(TaskScheduler *myts, COid &coid, int dstthread){
  int i,n,target;
  TaskMsgDataSplitterReply tmdsr;
  assert(sizeof(TaskMsgDataSplitterReply) <= sizeof(TaskMsgData));
  cleanupThreadSplitQueue();
  tmdsr.stats.splitQueueSize = TSS->ThreadSplitQueue.getNitems();
  tmdsr.stats.splitTimeRetryingMs = (i32)TSS->Stats.timeRetryingMs;
  tmdsr.stats.splitTimeAvg = (float) TSS->Stats.average.getAvg();
  tmdsr.stats.splitTimeStddev = (float) TSS->Stats.average.getStdDev();
  tmdsr.coid = coid;

  // send the message to all worker threads. Only dstthread will consider
  // the information in tmdsr.coid and tmdsr.stats.splitTimeRetryingMs.
  // We set stats.dest to 0 for the other threads so that they know to
  // ignore these two fields.

  n = gContext.getNThreads(TCLASS_WORKER);
  for (i=0; i<n; ++i){
    target = gContext.getThread(TCLASS_WORKER, i);
    if (target == dstthread) tmdsr.stats.dest = 1;
    else tmdsr.stats.dest = 0;
    sendIFMsg(dstthread, IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK,
              (void*)&tmdsr, sizeof(TaskMsgDataSplitterReply));
  }
}

OSTHREAD_FUNC ServerSplitterThread(void *parm){
  TaskScheduler *ts;
  ThreadSplitItem *tsi=0;
  u64 endtime;
  int res;
  assert(TSS);
  static int scount=0, xcount=0, ocount=0;

  // SLauncher->initThreadContext("ServerSplitter",0);
  ts = tgetTaskScheduler();

  ts->assignImmediateFunc(IMMEDIATEFUNC_SPLITTERTHREADNEWWORK,
                          ImmediateFuncSplitterThreadNewWork);

  int sleepeventfd = ts->getSleepEventFd();
  struct pollfd ev;
  ev.events = POLLIN;
  ev.fd = sleepeventfd;

  int n, something, timeout=0;
  eventfd_t eventdummy;

  while (1){
    something = ts->runOnce();
    if (!tsi){
      if (!TSS->ThreadSplitQueue.empty()){
        if (++scount % 100 == 0) dputchar(1, 'S');
        tsi = TSS->ThreadSplitQueue.popHead();
        tsi->starttime = Time::now();
      } else { // no work to do, try to go to sleep
        if (!something){ // start sleep cycle
          ts->setAsleep(1);
          timeout = ts->findSleepTimeout();
	  //if (timeout == -1) timeout = 10000;
          //printf("Splitter going to sleep for %d\n", timeout);
        } else timeout = 0;
        n = poll(&ev, 1, timeout);
	//printf("Splitter woke up\n");
	if (!something) ts->setAsleep(0);

	if (n==1){
	  assert(ev.fd == sleepeventfd);
	  eventfd_read(sleepeventfd, &eventdummy);
	}
	continue;
      }
    }
    if (tsi){
      res = DtSplit(tsi->coid, tsi->cell, true, 0, 0);  // do not trigger
      // splitting of parents, since this will be detected at each server
      endtime = Time::now();

      if (res && res != GAIAERR_WRONG_TYPE){ // could not complete split
                                          // and node exists
        TSS->Stats.timeRetryingMs = endtime - tsi->starttime;
        if (TSS->Stats.timeRetryingMs == 0)
          TSS->Stats.timeRetryingMs = 1; // avoid 0 since 0 indicates
                                         // completion of split
        mssleep(1);
        if (++xcount % 100 == 0) dputchar(1,'X');

        // report stats to originator, if not us
        if (tsi->srcthread != -1)
          SendIFThreadReportWork(ts, tsi->coid, tsi->srcthread);
      }
      else { // finished split or node does not exist
        if (res != GAIAERR_WRONG_TYPE){
          if (++ocount % 100 == 0) dputchar(1,'O');
          TSS->Stats.average.put((double)(endtime - tsi->starttime));
        }
        TSS->Stats.timeRetryingMs = 0;

        // report completion and stats to originator, if not us.
        // Completion indicated by fact that timeRetryingMs==0
        if (tsi->srcthread != -1){
          SendIFThreadReportWork(ts, tsi->coid, tsi->srcthread);
        }

        delete tsi;
        tsi = 0; // done with this item, process next one
      }
    }
  }
  return 0;
}
