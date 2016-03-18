//
// task.cpp
//
// Task scheduler used in the storage servers.
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
#include <poll.h>
#include <limits.h>
#include <poll.h>
#include <sys/eventfd.h>

#include <map>
#include <list>

#define _TASKC_
#include "os.h"
#include "tmalloc.h"
#include "options.h"
#include "debug.h"
#include "task.h"
#include "datastruct.h"
#include "taskdefs.h"

ChannelManager::ChannelManager(int maxthreads){
  int i, j;
  Maxthreads = maxthreads;
  Nthreads = 0;
  Channels = new Channel<TaskMsg> **[Maxthreads];
  WaitingChannel = new int *[Maxthreads];
  for (i=0; i < Maxthreads; ++i){
    Channels[i] = new Channel<TaskMsg>*[Maxthreads];
    WaitingChannel[i] = new int[Maxthreads];
    for (j=0; j < Maxthreads; ++j){
      Channels[i][j] = 0;
      WaitingChannel[i][j] = 0;
    }
  }
}

ChannelManager::~ChannelManager(){
  int i, j;
  for (i=0; i < Maxthreads; ++i){
    for (j=0; j < Maxthreads; ++j){
      if (Channels[i][j]) delete Channels[i][j];
    }
  }
  delete [] Channels;
}

Channel<TaskMsg> *ChannelManager::getChannel(int i, int j, bool create){
  assert(0 <= i && i < Nthreads && 0 <= j && j < Nthreads);
  if (!create) return Channels[i][j];
  Channel<TaskMsg> *ch = Channels[i][j];
  if (!ch){
    ch = new Channel<TaskMsg>;
    if (CompareSwapPtr((void**)&Channels[i][j], (void*) 0, (void*) ch) != 0){
      delete ch;
      ch = Channels[i][j]; assert(ch);
    }
  }
  return ch;
}

void ChannelManager::expandNthreads(int nthreads){
  i32 i;
  if (nthreads > Maxthreads){
    assert(0);
    printf("Requested %d threads above max %d\n", nthreads, Maxthreads);
    exit(1);
  }
  i = Nthreads;
  while (i < nthreads){
    CompareSwap32(&Nthreads, i, nthreads);
    i = Nthreads;
  }
}

int ChannelManager::sendMessage(int dst, TaskMsg &msg){
  int src = tgetThreadNo();
  Channel<TaskMsg> *ch = getChannel(dst, src, true);
  if (!ch) return -1;
  else return ch->enqueue(msg);
}

void ChannelManager::reportWait(int dst){
  int src = tgetThreadNo();
  ++WaitingChannel[dst][src];
}

void ChannelManager::printWaiting(){
  int i, j;
  int nthreads = getNthreads();
  printf("Frequency of channel waiting times:\n");
  for (i=0; i < nthreads; ++i){
    printf("  ");
    for (j=0; j < nthreads; ++j){
      printf("%8d ", WaitingChannel[i][j]);
    }
    putchar('\n');
  }
}

void TaskInfo::TaskInfoInit(){
  ThreadNo = -1;
  CurrSchedulerTaskState = SchedulerTaskStateNew;
  Func = 0;
  EndFunc = 0;
  MessageValid = false;
  MoreMessages = 0;
  TaskData = 0;
  State = 0;
  next = prev = 0;
}

TaskInfo::TaskInfo(ProgFunc f, void *taskdata, int threadno){
  TaskInfoInit();
  if (threadno == -1) ThreadNo = tgetThreadNo();
  else ThreadNo = threadno;
  Func = f;
  TaskData = taskdata;
}

TaskInfo::~TaskInfo(){
  if (MoreMessages) delete MoreMessages;
}

void TaskInfo::addMessage(TaskMsgData &msg){
  if (!MessageValid){
    // if there is space for message in single slot, add it there
    MessageValid = true;
    memcpy((void*)&Message, (void*)&msg, sizeof(TaskMsgData));
  } else { // otherwise, put in linked list
    if (!MoreMessages) MoreMessages = new LinkList<TaskMsgDataEntry>;
    TaskMsgDataEntry *tmde = new TaskMsgDataEntry;
    memcpy((void*)&tmde->data, (void*)&msg, sizeof(TaskMsgData));
    MoreMessages->pushTail(tmde);
  }
}

bool TaskInfo::hasMessage(){
  return MessageValid || (MoreMessages && !MoreMessages->empty());
}

// returns 0 if got a message, non-zero if there are no more messages
int TaskInfo::getMessage(TaskMsgData &msg){
  if (MessageValid){
    MessageValid = false;
    memcpy((void*)&msg, (void*)&Message, sizeof(TaskMsgData));
    return 0;
  }
  if (!MoreMessages || MoreMessages->empty()) return -1;
  TaskMsgDataEntry *tmde = MoreMessages->popHead();
  memcpy((void*)&msg, (void*)&tmde->data, sizeof(TaskMsgData));
  delete tmde;
  return 0;
}

TaskScheduler::TaskScheduler(u8 tno, ChannelManager *cmanager){
  int i;
  ThreadNo = tno;
  CManager = cmanager;
  ForceEnd = 0;
  for (i=0; i < NFIXEDTASKS; ++i){
    FixedTaskMap[i] = 0;
  }
  for (i=0; i < NIMMEDIATEFUNCS; ++i){
    ImmediateFuncMap[i] = 0;
  }
  //overflowRetry = 0;
  TimeOfNextTimedWaiting = ULLONG_MAX;

  SleepEventFd = eventfd(0, EFD_NONBLOCK); assert(SleepEventFd != -1);
  Asleep = 0;
}

void TaskScheduler::assignFixedTask(int n, TaskInfo *ti){
  assert(0 <= n && n < NFIXEDTASKS);
  if (0 <= n && n < NFIXEDTASKS){
    assert(!FixedTaskMap[n]); // ensure it was not assigned before
    FixedTaskMap[n] = ti;
  }
}

TaskInfo *TaskScheduler::getFixedTask(int n){
  if (0 <= n && n < NFIXEDTASKS) return FixedTaskMap[n];
  else return 0;
}

ImmediateFunc TaskScheduler::getImmediateFunc(int n){
  if (0 <= n && n < NIMMEDIATEFUNCS) return ImmediateFuncMap[n];
  else return 0;
}

void TaskScheduler::assignImmediateFunc(int n, ImmediateFunc func){
  assert(0 <= n && n < NIMMEDIATEFUNCS);
  if (0 <= n && n < NIMMEDIATEFUNCS){
    assert(!ImmediateFuncMap[n]); // ensure it was not assigned before
    ImmediateFuncMap[n] = func;
  }
}

//void TaskScheduler::retryOverflowMessages(){
//  TaskMsgEntry *tme, *nexttme;
//  ChannelManager *cm = getCManager();
//  int res;
//  for (tme = OverflowQueue.getFirst(); tme != OverflowQueue.getLast();
//        tme = nexttme){
//    nexttme = OverflowQueue.getNext(tme);
//    res = cm->sendMessage(tme->msg);
//    if (res==0){ // successfully sent, so remove from OverflowQueue
//      OverflowQueue.remove(tme);
//      delete tme;
//    }
//  }
//}

TaskInfo *TaskScheduler::createTask(ProgFunc f, void *taskdata){
  TaskInfo *ti = new TaskInfo(f, taskdata, ThreadNo);
  NewTasks.pushTail(ti);
  return ti;
}

void TaskScheduler::createTask(TaskInfo *ti){
  assert(ti->CurrSchedulerTaskState == SchedulerTaskStateNew);
  NewTasks.pushTail(ti);
}

// int TaskScheduler::checkSendQueuesAlmostFull(void){
//   int i;
//   int n;
//   Channel<TaskMsg> *ch;
//   int nthreads = CManager->getNthreads();
//   int threadno = tgetThreadNo();
//   for (i=0; i < nthreads; ++i){
//     ch = CManager->getChannel(i, threadno);
//     if (ch){
//       n = ch->available();
//       if (n < TASKSCHEDULER_FULL_ALMOST_QUEUE) return 1;
//     }
//   }
//   return 0;
// }

void TaskScheduler::setTaskState(TaskInfo *ti, int newstate){
  u64 wakeuptime;
  if (ti->CurrSchedulerTaskState == newstate) return;
  switch(ti->CurrSchedulerTaskState){
  case SchedulerTaskStateNew:
    break;
  case SchedulerTaskStateRunning: 
    RunningTasks.remove(ti);
    break;
  case SchedulerTaskStateWaiting: 
    WaitingTasks.remove(ti);
    break;
  case SchedulerTaskStateTimedWaiting: 
    TimedWaitingTasks.remove(ti);
    break;
  case SchedulerTaskStateEnding: 
    break;
  default:
    assert(0);
  }
  ti->CurrSchedulerTaskState = newstate;

  switch(newstate){
  case SchedulerTaskStateNew:
    assert(0);
  case SchedulerTaskStateRunning: 
    RunningTasks.pushTail(ti);
    break;
  case SchedulerTaskStateWaiting: 
    WaitingTasks.pushTail(ti);
    break;
  case SchedulerTaskStateTimedWaiting: 
    TimedWaitingTasks.pushTail(ti);
    wakeuptime = ti->getWakeUpTime();
    if (wakeuptime < TimeOfNextTimedWaiting)
      TimeOfNextTimedWaiting = wakeuptime;
    break;
  case SchedulerTaskStateEnding:
    break;
  default: assert(0);
  };
}

// if task is waiting or timedwaiting, change it to running
void TaskScheduler::wakeUpTask(TaskInfo *ti){
  if (ti->CurrSchedulerTaskState == SchedulerTaskStateWaiting){
    WaitingTasks.remove(ti);
    RunningTasks.pushTail(ti);
    ti->CurrSchedulerTaskState = SchedulerTaskStateRunning;
  } else if (ti->CurrSchedulerTaskState == SchedulerTaskStateTimedWaiting){
    TimedWaitingTasks.remove(ti);
    RunningTasks.pushTail(ti);
    ti->CurrSchedulerTaskState = SchedulerTaskStateRunning;
  }
}

// returns 0 if no incoming messages, != 0 otherwise
int TaskScheduler::hasIncomingMessages(){
  int srcthread;
  int nthreads = CManager->getNthreads();
  int threadno = tgetThreadNo();
  Channel<TaskMsg> *ch;
  int empty;

  for (srcthread=0; srcthread < nthreads; ++srcthread){
    ch = CManager->getChannel(threadno, srcthread);
    if (!ch) continue;

    empty = ch->empty();
    if (!empty) return -1;
  }
  return 0;
}

// wake up thread by writing to SleepEventFd. This will presumably be called by
// another thread to wake up the scheduler loop of the current thread
void TaskScheduler::wake(){
  if (Asleep){
    Asleep = 0;
    eventfd_write(SleepEventFd, 1);
  }
}


int TaskScheduler::processIncomingMessages(){
  int srcthread;
  TaskInfo *ti;
  int nthreads = CManager->getNthreads();
  int threadno = tgetThreadNo();
  Channel<TaskMsg> *ch;
  int res,nmsgs,left;
  TaskMsg *msg;
  ImmediateFunc itf;
  int something = 0; // whether some message was processed or not

  for (srcthread=0; srcthread < nthreads; ++srcthread){
    ch = CManager->getChannel(threadno, srcthread);
    if (!ch) continue;

    // first, dequeue all incoming messages. We do this first to avoid starving
    // ourselves (if we dequeued and processed each message individually,
    // messages may keep arriving so that we never finish)
    nmsgs = 0;
    while (1){
      res = ch->dequeue(MessageBuf[nmsgs]);
      if (res) break; // done
      ++nmsgs;
      assert(nmsgs <= (int) (sizeof(MessageBuf)/sizeof(TaskMsg)));
    }
    if (nmsgs==0) continue; // no messages
    something = 1;

    // now process each message individually
    msg = MessageBuf;
    for (left = 0; left < nmsgs; ++left, ++msg){
      if (msg->flags & TMFLAG_IMMEDIATEFUNC){
        assert(msg->flags & TMFLAG_FIXDEST);
        itf = getImmediateFunc(TASKID_TASKNO(msg->dest));
        if (!itf){
          printf("Immediate function %d is not registered\n",
                 TASKID_TASKNO(msg->dest));
          assert(0);
          exit(1);
        }
        itf(msg->data, this, srcthread); // invoke immediate task
        continue;
      }

      if (msg->flags & TMFLAG_FIXDEST)
        ti = getFixedTask(TASKID_TASKNO(msg->dest));
      else ti = msg->dest;
      if (!ti) continue;
      assert(ti->CurrSchedulerTaskState != SchedulerTaskStateEnding);
      if (msg->flags & TMFLAG_SCHED){
        // currently, the only message to be processed by scheduler
        // is to wake up task, done below
      } else {
        // message intended to task
        ti->addMessage(msg->data);
      }
      // if task is waiting, change it to running
      wakeUpTask(ti);
    } // for (left=0; ...)
  } // for (srcthread=0; ...)
  return something;
}

// assumes that tinit() has been previously executed once by thread
int TaskScheduler::runOnce(){
  int tstate;
  TaskInfo *ti, *nextti;
  int nrunning;
  u64 now;
  int something = 0; // whether something happened or not
  int res;

  // see if there are any incoming messages
  res = processIncomingMessages();
  if (res) something = 1;

  // check tasks that are waiting for some specific time
  now = Time::now();
  if (now >= TimeOfNextTimedWaiting){
    u64 nextearliest=ULLONG_MAX;
    u64 taskwakeup;
    // loop over all timed waiting tasks to see which ones should execute
    ti = TimedWaitingTasks.getFirst();
    while (ti != TimedWaitingTasks.getLast()){
      nextti = TimedWaitingTasks.getNext(ti);
      taskwakeup = ti->getWakeUpTime();
      if (taskwakeup <= now){
        wakeUpTask(ti);
      } else if (taskwakeup < nextearliest)
        nextearliest = taskwakeup; // remember the next earliest one
      ti = nextti;
    }
    TimeOfNextTimedWaiting = nextearliest;
  }

  // execute running tasks
  nrunning = 0;
  ti = RunningTasks.getFirst();
  while (ti != RunningTasks.getLast()){
    nextti = RunningTasks.getNext(ti);
    ++nrunning;
    //do {
      tstate = ti->Func(ti); // invoke function
    //} while (tstate == SchedulerTaskStateRunning); // invoke function while
                                                    // it returns running
    assert(tstate != SchedulerTaskStateNew);

    if (tstate == SchedulerTaskStateWaiting && ti->hasMessage())
      tstate = SchedulerTaskStateRunning;

    setTaskState(ti, tstate);
    
    if (tstate == SchedulerTaskStateEnding){
      if (ti->EndFunc) ti->EndFunc(ti);
      delete ti;
    }
    ti = nextti;
  }

  if (nrunning) something = 1;

  //// if there are overflow messages, try to send them
  //++overflowRetry;
  //if (overflowRetry >= TASKSCHEDULER_OVERFLOWRETRY_PERIOD){
  //  overflowRetry = 0;
  //  if (!OverflowQueue.empty()) retryOverflowMessages();
  //}

  // if there are no running tasks, promote new tasks to running tasks
  //  if (nrunning == 0){
    ti = NewTasks.getFirst();
    while (ti != NewTasks.getLast()){
      nextti = NewTasks.getNext(ti);
      ti->CurrSchedulerTaskState = SchedulerTaskStateRunning;
      something = 1;
      NewTasks.remove(ti);
      RunningTasks.pushTail(ti);
      ti = nextti;
    }
  //}

  // TODO: maybe here call scheduler. Or do it once every 100 interations of
  // the loop.
  return something;
}

// assumes that tinit() has been previously executed once by thread
void TaskScheduler::run(){
  int n, something, timeout=0;
  eventfd_t eventdummy;
  int sleepeventfd = getSleepEventFd();
  struct pollfd ev;
  ev.events = POLLIN;
  ev.fd = sleepeventfd;

  while (!ForceEnd){
    something = runOnce();
    if (!something){ // start sleep cycle
      setAsleep(1);
      timeout = findSleepTimeout();
      //printf("TaskScheduler::run() going to sleep for %d\n", timeout);
    } else timeout = 0;
    n = poll(&ev, 1, timeout);
    //printf("TaskScheduler::run() woke up\n");
    if (!something) setAsleep(0);

    if (n==1){
      assert(ev.fd == sleepeventfd);
      eventfd_read(sleepeventfd, &eventdummy);
    }
  }
}

int TaskScheduler::findSleepTimeout(){
  u64 now;
  int res = -1; // by default, can sleep forever
  if (hasIncomingMessages()) return 0; // no sleeping since there are
                                        // incoming messages

  if (RunningTasks.getFirst() != RunningTasks.getLast()) return 0; // no
                                        // sleeping since there are ready tasks
  if (NewTasks.getFirst() != NewTasks.getLast()) return 0; //there are new tasks
  
  // check for timed waiting tasks
  if (TimeOfNextTimedWaiting != ULLONG_MAX){
    now = Time::now();
    res = TimeOfNextTimedWaiting - now;
  }
  return res;
}
  

// Sleeping should follow this discipline:
//   1. set Asleep = 1
//   2. find possible sleep timeout (possibly zero)
//   3. invoke poll on SleepEventFd using sleep timeout

void TaskScheduler::sleep(){
  struct pollfd pfd;
  int res;
  int sleeptimeout;

  setAsleep(1);

  sleeptimeout = findSleepTimeout();

  // finally, go to sleep
  pfd.fd = SleepEventFd;
  pfd.events = POLLIN;
  pfd.revents = 0;
  res = poll(&pfd, 1, sleeptimeout); assert(res >= 0);
  setAsleep(0);
}


// Creates channels and launch the various schedulers.
//  - nthreads indicates the number of schedulers to create. There will be
//    squared number of channels.
//  - createthread, if set, is a boolean vector indicating which schedulers
//    should be launched.
//    Launching means creating a thread and invoking the scheduler's run method.
//    If createthread is 0, then all schedulers are launched
SchedulerLauncher::SchedulerLauncher(int maxthreads)
  : CManager(maxthreads)
{
  int i;

  Maxthreads = maxthreads;
  NextThread = 0;
  Schedulers = new TaskScheduler*[maxthreads];
  ThreadHandles = new OSThread_t[maxthreads];
  for (i=0; i < maxthreads; ++i){
    Schedulers[i] = 0;
    ThreadHandles[i] = 0;
  }
}

void ImmediateFuncNop(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
}

void ImmediateFuncExit(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  ts->exitThread();
}


struct CreateThreadData {
  int threadno;
  OSTHREAD_FUNC_PTR startroutine;
  char *threadname;
  void *threaddata;
  SchedulerLauncher *slauncher;
  bool pinthread;
  CreateThreadData(){ threadname = 0; }
  ~CreateThreadData(){ if (threadname) delete [] threadname; }
};

void SchedulerLauncher::initThreadContext(const char *threadname, int threadno,
                                          bool pinthread){
  TaskScheduler *ts;
  TaskInfo *ti;

  if (threadContext) return; // already initialized

  ProtectAll.lock();
  assert(0 <= threadno && threadno < NextThread);
  if (Schedulers[threadno]){
    ProtectAll.unlock();
    return; // thread was already created below
  }

  // initialize thread context
  tinit(threadname, threadno);
  ts = new TaskScheduler(threadno, &CManager);
  Schedulers[threadno] = ts;
  tsetTaskScheduler(ts);
  if (pinthread){
    int processor;
    processor = taskGetCore(threadname);
    if (processor<0)
      printf("Warning: cannot pin thread %s to processor because of too "
             "few processors. Performance will suffer\n", threadname);
    else {
      dprintf(1, "Pinning thread %s to processor %d", threadname, processor);
      pinThread(processor); // pin current thread to processor
    }
  }

  // assign immediate tasks and tasks
  ts->assignImmediateFunc(IMMEDIATEFUNC_NOP, ImmediateFuncNop);
  ts->assignImmediateFunc(IMMEDIATEFUNC_EXIT, ImmediateFuncExit);
  ts->assignImmediateFunc(IMMEDIATEFUNC_EVENTSCHEDULER_ADD,
                          TaskEventScheduler::ImmediateFuncEventSchedulerAdd);

  // create event scheduler task
  ti = ts->createTask(TaskEventScheduler::PROGEventScheduler, 0);
  ts->assignFixedTask(FIXEDTASK_EVENTSCHEDULER, ti);

  // create threadcontext space for eventscheduler task
  ThreadSharedEventScheduler *tses = (ThreadSharedEventScheduler*)
    tgetSharedSpace(THREADCONTEXT_SPACE_EVENTSCHEDULER);
  assert(!tses);
  tses = new ThreadSharedEventScheduler;
  tses->EventSchedulerTask = ti;
  tsetSharedSpace(THREADCONTEXT_SPACE_EVENTSCHEDULER, tses);

  ProtectAll.unlock();
}

int SchedulerLauncher::initThreadContext(const char *threadname,
                                         bool pinthread){
  if (threadContext) return threadContext->getThreadNo(); // already initialized
  ProtectAll.lock();
  if (NextThread >= Maxthreads){
    ProtectAll.unlock();
    assert(0);
    printf("Creating threads beyond Maxthreads. Increase Maxthread.\n");
    exit(1);
  }
  int threadno = NextThread++;
  CManager.expandNthreads(NextThread);
  ProtectAll.unlock();
  initThreadContext(threadname, threadno, pinthread);
  return threadno;
}

OSTHREAD_FUNC SchedulerLauncher::createThreadAux(void *parm){
  CreateThreadData *ctd = (CreateThreadData*) parm;
  SchedulerLauncher *slauncher = ctd->slauncher;
  OSTHREAD_FUNC_PTR startroutine = ctd->startroutine;
  void *threaddata = ctd->threaddata;

  slauncher->initThreadContext(ctd->threadname, ctd->threadno, ctd->pinthread);
  delete ctd;

  // call the requested startroutine
  return startroutine(threaddata);
}

int SchedulerLauncher::createThread(const char *threadname,
                                    OSTHREAD_FUNC_PTR startroutine, void *threaddata, bool pinthread){
  int res;
  ProtectAll.lock();
  if (NextThread >= Maxthreads){
    ProtectAll.unlock();
    assert(0);
    printf("Creating too many threads. Increase Maxthread.\n");
    exit(1);
  }
  int threadno = NextThread++;
  CManager.expandNthreads(NextThread);

  CreateThreadData *ctd = new CreateThreadData;
  ctd->threadno = threadno;
  ctd->startroutine = startroutine;
  ctd->threaddata = threaddata;
  ctd->threadname = new char[strlen(threadname)+1];
  strcpy(ctd->threadname, threadname);
  ctd->slauncher = this;
  ctd->pinthread = pinthread;
  res = OSCreateThread(ThreadHandles + threadno, createThreadAux, (void*) ctd);
  if (res){
    ProtectAll.unlock();
    printf("Cannot create thread: error %d\n", res);
    exit(1);
  }
  ProtectAll.unlock();
  return threadno;
}

// waits for a single thread to finish
// Returns 0 if thread finished, non-zero if timeout or error
unsigned long SchedulerLauncher::waitThread(int threadno){
  int res;
  res = pthread_join(ThreadHandles[threadno], 0); assert(res==0);
  ThreadHandles[threadno]=0;
  return res;
}


// wait for threads to finish
void SchedulerLauncher::wait(){
  int res;
  for (int i=0; i < NextThread; ++i){
    if (ThreadHandles[i]){
      res = pthread_join(ThreadHandles[i], 0); assert(res==0);
    }
  }
}

SchedulerLauncher *SLauncher = 0;

void tinitScheduler(int initthread){
  if (!SLauncher){
    SLauncher = new SchedulerLauncher(TASKSCHEDULER_MAX_THREADS);
    if (initthread != -1)
      SLauncher->initThreadContext("(tinitScheduler)",
                                   initthread == 0 ? false : true);
  }
}

//---------------------------- thread context --------------------------------
Tlocal ThreadContext *threadContext;

ThreadContext::ThreadContext(const char *name, int threadno)
{
  Name = new char[strlen(name)+1];
  strcpy(Name, name);
  ThreadNo = threadno;
  TScheduler = 0;
  memset(SharedSpace, 0, sizeof(SharedSpace));
}

ThreadContext::~ThreadContext(){
  delete [] Name;
  if (TScheduler) delete TScheduler;
}

void tinit(const char *name, int threadno){
  if (!threadContext)
    threadContext = new ThreadContext(name, threadno);
}

GlobalContext gContext;

GlobalContext::GlobalContext(){
  for (int i=0; i < TASKSCHEDULER_MAX_THREAD_CLASSES; ++i){
    NThreads[i] = 0;
    Threads[i] = 0;
  }
}

GlobalContext::~GlobalContext(){
  for (int i=0; i < TASKSCHEDULER_MAX_THREAD_CLASSES; ++i){
    if (Threads[i]) delete [] Threads[i];
  }
}

// indicates that class tclass has n threadnos
void GlobalContext::setNThreads(int tclass, int n){
  assert(0 <= tclass && tclass < TASKSCHEDULER_MAX_THREAD_CLASSES && n >= 0);
  NThreads[tclass] = n;
  if (Threads[tclass]) delete [] Threads[tclass];
  Threads[tclass] = new int[n];
  for (int i=0; i < n; ++i){
    Threads[tclass][i] = 0;
  }
}

// indicates that the k-th thread of tclass is threadno
void GlobalContext::setThread(int tclass, int k, int threadno){
  assert(0 <= tclass && tclass < TASKSCHEDULER_MAX_THREAD_CLASSES);
  assert(0 <= k && k < NThreads[tclass]);
  assert(Threads[tclass]);
  Threads[tclass][k] = threadno;
}

// ---------------------------- event scheduler -------------------------------

bool operator < (const TEvent& x, const TEvent& y){
  return x.when > y.when;
}

void TaskEventScheduler::ImmediateFuncEventSchedulerAdd(TaskMsgData &msgdata,
                                           TaskScheduler *ts, int srcthread){
  TEvent e;
  TaskMsgDataAddEvent *ed;
  TaskMsgDataAddEvent *tmdae = (TaskMsgDataAddEvent*) &msgdata;
  // extract shared space entry
  ThreadSharedEventScheduler *tses = (ThreadSharedEventScheduler*)
    tgetSharedSpace(THREADCONTEXT_SPACE_EVENTSCHEDULER);
  assert(tses);

  assert(tmdae->type >= 0 && tmdae->type <= 1);

  ed = new TaskMsgDataAddEvent;

  e.when = Time::now() + tmdae->msFromNow;
  e.ed = ed;
  *ed = *tmdae;

  tses->TEvents.push(e);
  if (tses->TEvents.top().when == e.when){
    if (tses->EventSchedulerTask->getSchedulerTaskState() ==
        SchedulerTaskStateTimedWaiting)
      ts->setTaskState(tses->EventSchedulerTask, SchedulerTaskStateRunning);
    // if this event is at the top (smallest when time), wake up 
    // thread otherwise it will sleep until the next event
  }
}

// event scheduler program
int TaskEventScheduler::PROGEventScheduler(TaskInfo *ti){
  TEvent copyTopEvent;
  TaskMsgDataAddEvent *ed;
  int handlerres;

  // extract shared space entry
  ThreadSharedEventScheduler *tses = (ThreadSharedEventScheduler*)
    tgetSharedSpace(THREADCONTEXT_SPACE_EVENTSCHEDULER);
  assert(tses);

  u64 now = Time::now();

  // invoke all expired events, removing them from queue
  while (!tses->TEvents.empty() && (copyTopEvent = tses->TEvents.top()).when
         <= now){
    do {
      ed = copyTopEvent.ed;
      tses->TEvents.pop();

      handlerres = ed->handler(ed->data);  

      if (ed->type==1 && handlerres==0){ // reschedule event
        copyTopEvent.when = Time::now() + ed->msFromNow;
        tses->TEvents.push(copyTopEvent); // put back event with new time
      } else delete ed; // free space taken by event data
    } while (!tses->TEvents.empty() &&
             (copyTopEvent = tses->TEvents.top()).when <= now);

    now = Time::now();
  }

  // determine and set wake up time
  u64 wakeup = tses->TEvents.empty() ? ULLONG_MAX : copyTopEvent.when;
  tses->EventSchedulerTask->setWakeUpTime(wakeup);

  return SchedulerTaskStateTimedWaiting;
}

void TaskEventScheduler::AddEvent(int threadno, TEventHandler handler,
                                  void *data, int type, int msFromNow){
  TaskMsg msg;
  TaskMsgDataAddEvent *tmdae;
  assert(sizeof(TaskMsgDataAddEvent) <= sizeof(TaskMsgData));

  // prepare a message to send to EventTask
  tmdae = (TaskMsgDataAddEvent*) &msg.data;
  tmdae->handler = handler;
  tmdae->data = data;
  tmdae->type = type;
  tmdae->msFromNow = msFromNow;
  msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_EVENTSCHEDULER_ADD);
  msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;

  // send it to the appropriate thread
  tgetTaskScheduler()->sendMessage(msg);
}


TaskScheduler *tgetThreadTaskScheduler(int threadno){
  return SLauncher->getTaskScheduler(threadno);
}
