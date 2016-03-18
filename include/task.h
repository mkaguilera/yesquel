//
// task.h
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

#ifndef _TASK_H
#define _TASK_H

#include <queue>
#include "gaiatypes.h"
#include "tmalloc.h"
#include "datastruct.h"
#include "datastructmt.h"
#include "ipmisc.h"
#include "taskdefs.h"

using namespace std;

class TaskInfo;
class TaskScheduler;

#define TASKSCHEDULER_MAX_THREADS 256        // maximum number of threads
#define TASKSCHEDULER_MAX_THREAD_CLASSES 8  // maximum number of thread classes

#define TASKSCHEDULER_TASKMSGDATA_SIZE  32 // number of bytes in TaskMsgData
#define TASKSCHEDULER_MAXMESSAGEPROCESS 25 // max number of messages to process
                                    // on a chanel on each event loop iteration
#define TASKSCHEDULER_OVERFLOWRETRY_PERIOD 10 // after how many iterations of
                          // the event loop to retry sending overflow messages
#define TASKSCHEDULER_FULL_ALMOST_QUEUE 1024 // number below which a send queue
                                             // is considered to be almost full
#define CHANNEL_MAXSENDMSGRETRY 100 // max number of retries when queue is full
                                    // before deferring send
#define CHANNEL_SENDMSGRETRY_REPORT_WAIT 1000 // number of sending retry at
                                             // which point to report waiting
#define CHANNEL_SENDMSGRETRY_PROCESS_INCOMING 100 // number of sending retry
                                 // at which point to process incoming message
#define CHANNEL_SENDMSGRETRY_PRINTWAITING 1000000 // number of sending retry at
                              // which point to print waiting times on channels

#define TMFLAG_INVALID       0x01  // if set, this is not a valid message
#define TMFLAG_FIXDEST       0x02  // if set, dest is a fixed task number not
                                   //       a pointer
#define TMFLAG_IMMEDIATEFUNC 0x04  // if set, dest is an immediate function not
                                    // a task (must be used with TMFLAG_FIXDEST)
#define TMFLAG_SCHED         0x08  // if set, this message is to be processed
     // by task scheduler not task. Currently the only message to the task
     // scheduler is to wake up the task. Cannot be used with flag
     // TMFLAG_IMMEDIATEFUNC

#define TASKSCHEDULER_FREEBATCH_NODE_SIZE 128 // number of items in a node of
                                              // the freebatch link list
#define TASKSCHEDULER_FREEBATCH_SIZE 1024 // number of items to batch together
              // for a given destination thread before sending message to free
//#define TASKSCHEDULER_FREEBATCH_NODE_SIZE 2 // number of items in a node of
                                              // the freebatch link list
//#define TASKSCHEDULER_FREEBATCH_SIZE 6 // number of items to batch together
        // for a given destination thread before sending message to free

void tinit(const char *name, int threadno);
TaskScheduler *tgetThreadTaskScheduler(int threadno);

struct TaskMsgData {
  u8 data[TASKSCHEDULER_TASKMSGDATA_SIZE];
};

// sets MsgData with a pointer
inline void SetTaskMsgDataPtr(TaskMsgData &d, void *ptr){
  memset((void*)&d, 0, sizeof(TaskMsgData));
  memcpy((void*)&d, (void*)&ptr, sizeof(void*));
}

// Messages may carry a taskid instead of a pointer to TaskInfo. The following
// macros create a taskid and separate out the thread number and task number
// from a taskid.
#define TASKID_CREATE(threadno,taskno) ((TaskInfo*) (long long) (((threadno) << 16) | (taskno)))
#define TASKID_THREADNO(task) ((u32)(long long)(task) >> 16)
#define TASKID_TASKNO(task) ((u32)(long long)(task) & 0xffff)

struct TaskMsg {
  TaskInfo *dest;    // destination task
  TaskMsgData data;
  u8 flags;
  void setinvalid(){ flags = TMFLAG_INVALID; }
  bool isinvalid(){ return flags & TMFLAG_INVALID; }
};


// manages channels between pairs of threads
class ChannelManager {
  friend class TaskScheduler;
private:
  int Maxthreads;
  Align4 i32 Nthreads;
  Channel<TaskMsg> ***Channels; // Channels[i][j] if non-zero is a channel
                               // from j to i
  int **WaitingChannel; // WaitingChannel[i][j] is the number of times j had
                        // to wait for i to send a emssage
  friend class ChannelCreator;
  Channel<TaskMsg> **InChannels;
  Channel<TaskMsg> **OutChannels;
  ChannelManager(){} // only ChannelCreator should create a ChannelManager
  int sendMessage(int dst, TaskMsg &msg);   // private use only. Users should
      // call sendMessage from the TaskScheduler class, which retries sending

public:
  ChannelManager(int maxthreads);
  ~ChannelManager();
  Channel<TaskMsg> *getChannel(int i, int j, bool create=false); // return
                                          // channel from thread j to thread i
  void expandNthreads(int nthreads);
  int getNthreads(){ return Nthreads; }
  void reportWait(int dst); // report waiting on appropriate channel for
                            // sending given msg
  void printWaiting(); // print waiting counters for every pair of channel
};

// entry for a TaskMsgData to be placed in a linked list
struct TaskMsgDataEntry {
  TaskMsgData data;
  TaskMsgDataEntry *next, *prev; // link list stuff
  TaskMsgDataEntry(){ ::memset((void*) &data, 0, sizeof(TaskMsgData)); }
  TaskMsgDataEntry(TaskMsgData &d) : data(d) { }
};

// entry for a TaskMsg to be placed in a linked list
struct TaskMsgEntry {
  TaskMsg msg;
  TaskMsgEntry *next, *prev; // link list stuff
  TaskMsgEntry(){ ::memset((void*) &msg, 0, sizeof(TaskMsg)); }
  TaskMsgEntry(TaskMsg &m) {
    ::memcpy((void*) &msg, (void*) &m, sizeof(TaskMsg));
  }
};

// information about a task
enum SchedulerTaskState { SchedulerTaskStateNew=-1, 
                          SchedulerTaskStateRunning=0, 
                          SchedulerTaskStateWaiting=1,
                          SchedulerTaskStateTimedWaiting=2, 
                          SchedulerTaskStateEnding=3 };

// A program takes as parameter a TaskInfo*
// and returns its new scheduler task state
typedef int (*ProgFunc)(TaskInfo*);


// Note: TaskInfo does not have a virtual destructor.
//       So classes derived from it should not have anything to destroy
class TaskInfo {
private:
  friend class TaskScheduler;
  // information used by scheduler
  int ThreadNo;
  int CurrSchedulerTaskState;  // current state as far as scheduler is concerned
  u64 SchedulerWakeUp;   // if CurrSchedulerTaskState==
                      // SchedulerTaskStateTimedWaiting, sleep until this time
  ProgFunc Func;         // function to execute next
  ProgFunc EndFunc;      // function to execute when task ends
  bool MessageValid;     // whether Msg is some valid message or not
  TaskMsgData Message;   // message waiting to be delivered to task
  LinkList<TaskMsgDataEntry> *MoreMessages; // If non-zero, more messages
                                          // waiting to be delivered to task
  void *TaskData;        // task specific data given when task is created


public:
  // information used by task
  TaskInfo *next, *prev; // link list stuff

  void *State;    // task-specific state

  void TaskInfoInit(); // common initializer for all constructors
  TaskInfo(){ TaskInfoInit(); }
  TaskInfo(ProgFunc f, void *taskdata, int threadno=(int)-1); // if threadno
                                      // not specified, get it from the context
  ~TaskInfo();
  int getThreadNo(){ return ThreadNo; }
  void addMessage(TaskMsgData &msg);
  bool hasMessage();
  int getMessage(TaskMsgData &msg); // returns 0 if got message and sets msg,
                                      // non-zero if no messages
  void setFunc(ProgFunc f){ Func = f; }
  void setEndFunc(ProgFunc ef){ EndFunc = ef; }
  void *getTaskData(){ return TaskData; }
  void setWakeUpTime(u64 wakeuptime){ SchedulerWakeUp = wakeuptime; }
  u64 getWakeUpTime(){ return SchedulerWakeUp; }
  int getSchedulerTaskState(){ return CurrSchedulerTaskState; }
  void *getState(){ return State; }
  void setState(void *state){ State = state; }
};

typedef void (*ImmediateFunc)(TaskMsgData &msgdata, TaskScheduler *ts,
                              int srcthread);

class TaskScheduler {
private:
  int ForceEnd; // set to true to cause thread to return
  LinkList<TaskInfo> NewTasks;     // newly created tasks
  LinkList<TaskInfo> RunningTasks; // tasks that are running
  LinkList<TaskInfo> WaitingTasks; // tasks that are waiting on some message
  LinkList<TaskInfo> TimedWaitingTasks; // tasks that are waiting for a time
  u64 TimeOfNextTimedWaiting;  // time when the earliest TimedWaitingTask needs
                               // to execute
  ChannelManager *CManager; // object that holds the channels
  u8 ThreadNo;
  LinkList<TaskMsgEntry> OverflowQueue; // queue to place messages when send
                                        // queue is full
  TaskInfo *FixedTaskMap[NFIXEDTASKS];
  ImmediateFunc ImmediateFuncMap[NIMMEDIATEFUNCS];

  TaskMsg MessageBuf[DEFAULT_CHANNEL_SIZE*2];  // temporary buffer for storing
                              // incoming messages in processIncomingMessages()

  int Asleep;       // set to true if thread got idle and went to sleep
  int SleepEventFd; // eventfd for sleeping on when thread gets idle. If Asleep
                    // is true, thread needs to be waked up by wake()

  // methods related to overflowing message queues
  //int overflowRetry;
  //void addOverflowMsg(TaskMsg &msg){
  //  OverflowQueue.pushTail(tnew(TaskMsgEntry)(msg));
  //}
  //void retryOverflowMessages();

  int processIncomingMessages(); // handles incoming messages. Returns 1 if
                         // some message was processed, 0 if nothing happened

public:
  TaskScheduler(u8 tno, ChannelManager *cmanager);
  ~TaskScheduler(){ }
  void assignFixedTask(int n, TaskInfo *ti);
  TaskInfo *getFixedTask(int n);
  void assignImmediateFunc(int n, ImmediateFunc func);
  ImmediateFunc getImmediateFunc(int n);

  u8 getThreadNo(){ return ThreadNo; }
  ChannelManager *getCManager(){ return CManager; }
  int getForceEnd(){ return ForceEnd; }

  TaskInfo *createTask(ProgFunc f, void *taskdata); // creates a new task and
                                                  // returns its taskinfo
  void createTask(TaskInfo *ti);    // creates a new task given its taskinfo
  //int checkSendQueuesAlmostFull(void); // returns non-zero if some send queue
                                          // is almost full, zero othersize
  void setTaskState(TaskInfo *ti, int newstate);
  void wakeUpTask(TaskInfo *ti); // if task is waiting or timedwaiting, change
      // it to running. Should be called only for task in the current thread
  int runOnce(); // one iteration of loop that schedules task. Assumes that
    // tinit() has been previously called once by thread
    // return 1 if some task or immediate function was executed,
  // 0 if nothing happened
  void run(); // start scheduling tasks. Assumes that tinit() has been
              // previously executed once by thread
  void sendMessage(TaskMsg &msg){ 
    int retry=0;
    int dst;
    TaskScheduler *dstts;
    if (msg.flags & TMFLAG_FIXDEST)
      dst = TASKID_THREADNO((u32)(u64)msg.dest);
    else dst = msg.dest->getThreadNo();
    dstts = tgetThreadTaskScheduler(dst); assert(dstts);
    
    while (CManager->sendMessage(dst, msg)){
      ++retry;
      if (retry % CHANNEL_SENDMSGRETRY_REPORT_WAIT == 0){
        CManager->reportWait(dst); 
      }
      if (retry % CHANNEL_SENDMSGRETRY_PROCESS_INCOMING == 0){
        processIncomingMessages();
        //addOverflowMsg(msg);
      }
      if (retry % CHANNEL_SENDMSGRETRY_PRINTWAITING == 0){
        CManager->printWaiting();
      }
    }
    dstts->wake();
  }
  
  void exitThread(){ ForceEnd = 1; wake(); }

  // returns 0 if no incoming messages, != 0 otherwise
  // Useful to determine if thread can go to sleep or not
  int hasIncomingMessages(); 
  
  // wake up thread by writing to SleepEventFd. This will presumably be called
  // by another thread to wake up the scheduler loop of the current thread
  void wake();

  // returns the eventfd used for sleeping. This is intended to be used by
  // a tailored scheduler loop, who will then go to sleep on the SleepEventFd
  // (and possibly other fds).
  int getSleepEventFd(){ return SleepEventFd; }

  // calculate how many milliseconds we can sleep for. This will be zero if
  // there are incoming messages, ready tasks, or new tasks. If there are timed
  // waiting tasks, then returns the number of milliseconds until the next one.
  // Otherwise return -1, meaning we can sleep forever
  int findSleepTimeout();

  void setAsleep(int as){ Asleep = as; }   // sets asleep flag
  
  // Go to sleep until waken up. This is to be called by the scheduler loop of
  // the current thread.
  // This function is provided as an example of how the scheduler loop can
  // sleep.
  // We intend that a tailored scheduler loop will contain its own
  // implementation of sleep, say by calling poll/epoll on a list of fds that
  // include SleepEventFd.
  // Sleeping requires a careful discipline. One must first set Asleep=1.
  // Then one must calculate the sleep timeout, which could be 0 if there are
  // pending events such as incoming messages or ready tasks, or >0 if there
  // are timedwaiting tasks).
  // Then one should sleep for the calculated sleep timeout.
  void sleep();

};

// -------------------------------- free batches -------------------------------
// definitions for batches of free requests
struct FreeBatchLinkListNode { // link list node for a batch of free requests
  FreeBatchLinkListNode *next;
  int nbufs;  // number of entries in vector below that are valid
  void *bufs[TASKSCHEDULER_FREEBATCH_NODE_SIZE]; // pointers to bufs to be freed
  FreeBatchLinkListNode();
};

//-------------------------------- thread context -----------------------------

// information kept per thread
class ThreadContext {
private:
  friend class SchedulerLauncher;
  char *Name;
  int ThreadNo;
  TaskScheduler *TScheduler;
  void *SharedSpace[THREADCONTEXT_SHARED_SPACE_SIZE];

  static int PROGBatchFreeBufs(TaskInfo *ti);

public:
  ThreadContext(const char *name, int threadno);
  ~ThreadContext();
  int getThreadNo(){ return ThreadNo; }
  ChannelManager *getCManager(){
    if (TScheduler) return TScheduler->getCManager(); 
    else return 0; 
  }
  TaskScheduler *getTaskScheduler(){ return TScheduler; }
  void setTaskScheduler(TaskScheduler *ts){ TScheduler=ts; }
  void *getSharedSpace(int index){
    assert(0 <= index && index < THREADCONTEXT_SHARED_SPACE_SIZE); 
    return SharedSpace[index]; 
  }
  void setSharedSpace(int index, void *v){ 
    assert(0 <= index && index < THREADCONTEXT_SHARED_SPACE_SIZE); 
    SharedSpace[index] = v; 
  }
  char *getName(){ return Name; }
};

extern Tlocal ThreadContext *threadContext;

inline void tsetTaskScheduler(TaskScheduler *ts){
  threadContext->setTaskScheduler(ts);
}
inline TaskScheduler *tgetTaskScheduler(void){
  return threadContext->getTaskScheduler();
}
inline int tgetThreadNo(void){ return threadContext->getThreadNo(); }
inline char *tgetThreadName(void){ return threadContext->getName(); }
inline void *tgetSharedSpace(int index){
  return threadContext->getSharedSpace(index);
}
inline void tsetSharedSpace(int index, void *v){
  threadContext->setSharedSpace(index, v);
}
inline void tsendMessage(TaskMsg &msg){
  threadContext->getTaskScheduler()->sendMessage(msg);
}
inline void tsendWakeup(TaskInfo *ti){ // send a wake up message to a task
                                      // (this is handled by the task scheduler)
  TaskMsg msg;
  memset(&msg, 0, sizeof(msg));
  msg.flags = TMFLAG_SCHED;
  msg.dest = ti;
  tsendMessage(msg);
}

// sends a message to an immediate function at another thread
//    threadno: target thread
//    funcno: number of the immediate function
//    data: data to be passed to immediate function
//    len: length of data (at most TASKSCHEDULER_TASKMSGDATA_SIZE)
static inline void sendIFMsg(int threadno, int funcno, void *data, int len){
  assert(len <= TASKSCHEDULER_TASKMSGDATA_SIZE);
  TaskMsg msg;
  msg.dest = TASKID_CREATE(threadno, funcno);
  msg.flags = TMFLAG_IMMEDIATEFUNC | TMFLAG_FIXDEST;
  if (len < (int) sizeof(TaskMsgData))
    memset(&msg.data, 0, sizeof(TaskMsgData));
  memcpy(&msg.data, data, len);
  tsendMessage(msg);
}


// ------------------------ global context --------------------------------
// information kept for all threads

class GlobalContext {
private:
  int NThreads[TASKSCHEDULER_MAX_THREAD_CLASSES]; // number of threads for
                                                  // each class
  int *Threads[TASKSCHEDULER_MAX_THREAD_CLASSES];  // list of threads for
                                                   // each class

public:
  GlobalContext();
  ~GlobalContext();
  void setNThreads(int tclass, int n); // indicates that class tclass has n
                                        // threadnos
  void setThread(int tclass, int k, int threadno); // indicates that the k-th
                                               // thread of tclass is threadno

  // returns how many threads tclass has
  int getNThreads(int tclass){
    assert(0 <= tclass && tclass < TASKSCHEDULER_MAX_THREAD_CLASSES);
    return NThreads[tclass];
  }
  
  // returns the k-th thread of tclass
  int getThread(int tclass, int k){
    assert(0 <= tclass && tclass < TASKSCHEDULER_MAX_THREAD_CLASSES);
    assert(0 <= k && k < NThreads[tclass]);
    assert(Threads[tclass]);
    return Threads[tclass][k];
  }

  // returns one of the threads in tclass based on v mod the number of threads
  int hashThread(int tclass, int v){
    return getThread(tclass, v % getNThreads(tclass));
  }

  // ditto, except that returns the index within the class instead of the
  // actual thread number
  int hashThreadIndex(int tclass, int v){
    return v % getNThreads(tclass);
  }

  // returns the index of threadno within tclass
  // fails assert if threadno does not belong to class
  int indexWithinClass(int tclass, int threadno){
    int i = threadno - hashThread(tclass,0);
    assert(i < getNThreads(tclass));
    return i;
  }
};

// class that launches the schedulers at each thread
class SchedulerLauncher {
private:
  RWLock ProtectAll; // protects everything (big lock)
  TaskScheduler **Schedulers;
  ChannelManager CManager;
  OSThread_t *ThreadHandles;
  int Maxthreads;
  int NextThread; // next available thread number
  static OSTHREAD_FUNC createThreadAux(void *parm); // aux function for
                                       // creating thread

public:
  // Creates channels and launch the various schedulers.
  //   - maxthreads indicates the max of threads that can be created.
  SchedulerLauncher(int maxthreads);

  // initializes the context of a thread
  void initThreadContext(const char *threadname,
       int threadno, bool pinthread); // this version uses a given threadno
  int initThreadContext(const char *threadname,
      bool pinthread); // this version obtains a new threadno, which is returned

  // Create a thread that has a task scheduler and a thread context.
  // The run() method of the task scheduler is not called.
  // The thread should either call run() or periodically call runOnce().
  int createThread(const char *threadname, OSTHREAD_FUNC_PTR startroutine,
                   void *threaddata, bool pinthread);

  // waits for a single thread to finish
  unsigned long waitThread(int threadno);

  // wait for threads to finish
  void wait();

  TaskScheduler *getTaskScheduler(int threadno){
    if (Schedulers && 0 <= threadno && threadno < NextThread)
      return Schedulers[threadno];
    else return 0;
  }
};

extern GlobalContext gContext;
extern SchedulerLauncher *SLauncher;
void tinitScheduler(int initthread); // should be called once across all threads
     // parameter initthread indicates how to initialize the scheduler for
     // the calling thread:
     //  -1: do not initialize scheduler
     //   0: initialize scheduler without pinning thread to CPU
     //   1: initialize scheduler while pinning thread to CPU

TaskScheduler *tgetThreadTaskScheduler(int threadno);
inline void initThreadContext(const char *threadname, bool pinthread){
  SLauncher->initThreadContext(threadname, pinthread);
}

// --------------------------- event scheduler ------------------------------

typedef int (*TEventHandler)(void *);
struct TaskMsgDataAddEvent {
  TEventHandler handler;
  void *data;
  u32 type;
  u32 msFromNow;
};

class TEvent {
  public:
  u64 when;        // time of next call to handler
  TaskMsgDataAddEvent *ed;   // separating rest to make class small since it
                             // gets copied a lot
  TEvent(){ ed=0; }
};
bool operator < (const TEvent& x, const TEvent& y);

// data in thread shared space for event scheduler
struct ThreadSharedEventScheduler {
  priority_queue<TEvent> TEvents;
  TaskInfo *EventSchedulerTask;
};

// function to install a new task and immediate function for event scheduler
class TaskEventScheduler {
public:
  static void ImmediateFuncEventSchedulerAdd(TaskMsgData &msgdata,
      TaskScheduler *ts, int srcthread); // event scheduler immediate function
  static int PROGEventScheduler(TaskInfo *ti); // event scheduler program
  static void AddEvent(int threadno, TEventHandler handler, void *data,
                       int type, int msFromNow);
};

#endif
