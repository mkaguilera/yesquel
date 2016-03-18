//
// storageserver-rpc.cpp
//
// Stubs for RPCs at the storage server. These stubs implement tasks (see
// task.cpp) and then invokes the actual RPCs.
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
#include <ctype.h>
#include <stddef.h>

#include <map>
#include <list>
#include <set>

#include "tmalloc.h"
#include "os.h"
#include "debug.h"
#include "gaiarpcaux.h"

#include "task.h"

#include "storageserver.h"
#include "storageserverstate.h"
#include "storageserver-rpc.h"

//RPCServer *ServerPtr;

int nullRpcStub(RPCTaskInfo *rti){
  NullRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = nullRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int getstatusRpcStub(RPCTaskInfo *rti){
  GetStatusRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = getstatusRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int writeRpcStub(RPCTaskInfo *rti){
  WriteRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = writeRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int readRpcStub(RPCTaskInfo *rti){
  ReadRPCData d;
  Marshallable *resp;
  bool defer;
  defer = false;
  d.demarshall(rti->data);
  resp = readRpc(&d, (void*) rti, defer);
  if (defer) return SchedulerTaskStateWaiting;
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int fullwriteRpcStub(RPCTaskInfo *rti){
  FullWriteRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = fullwriteRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int fullreadRpcStub(RPCTaskInfo *rti){
  FullReadRPCData d;
  Marshallable *resp;
  bool defer;
  defer = false;
  d.demarshall(rti->data);
  resp = fullreadRpc(&d, (void*) rti, defer);
  if (defer) return SchedulerTaskStateWaiting;
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int listaddRpcStub(RPCTaskInfo *rti){
  ListAddRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = listaddRpc(&d, rti->State);
  if (!resp){ // no response
    assert(rti->State);
    int delay = (int) (long long)rti->State;
    //printf("Delaying LISTADD for %d ms\n", delay);
    rti->setWakeUpTime(Time::now() + delay);
    return SchedulerTaskStateTimedWaiting;
  } else {
    rti->setResp(resp);
    return SchedulerTaskStateEnding;
  }
}

int listdelrangeRpcStub(RPCTaskInfo *rti){
  ListDelRangeRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = listdelrangeRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int attrsetRpcStub(RPCTaskInfo *rti){
  AttrSetRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = attrsetRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int prepareRpcStub(RPCTaskInfo *rti){
  PrepareRPCData d;
  Marshallable *resp;
  int res;
  d.demarshall(rti->data);

  if (rti->State == 0){
    resp = prepareRpc(&d, rti->State, (void*) rti);
    if (!resp){ // no response yet
      assert(rti->State);
      return SchedulerTaskStateWaiting; // more work to do
    }
  } else {
    if (rti->hasMessage()){
      TaskMsgData msg;
      res = rti->getMessage(msg); assert(res == 0);
      assert(msg.data[0] == 0xb0); // this is just to check the response
                                   // (which has no relevant data)
    }
    resp = prepareRpc(&d, rti->State, (void*) rti);
    assert(resp);
    assert(rti->State == 0);
  }
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int commitRpcStub(RPCTaskInfo *rti){
  CommitRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = commitRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int subtransRpcStub(RPCTaskInfo *rti){
  SubtransRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = subtransRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int shutdownRpcStub(RPCTaskInfo *rti){
  ShutdownRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = shutdownRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int startsplitterRpcStub(RPCTaskInfo *rti){
  StartSplitterRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = startsplitterRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
int flushfileRpcStub(RPCTaskInfo *rti){
  FlushFileRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = flushfileRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
int loadfileRpcStub(RPCTaskInfo *rti){
  LoadFileRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = loadfileRpc(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

// Auxilliary function to be used by server implementation
// Wake up a task that was deferred, by sending a wake-up message to it
void serverAuxWakeDeferred(void *handle){
#ifndef LOCALSTORAGE
  // on storageserver, send wake up message
  tsendWakeup((TaskInfo*)handle);
#else
  // on localstorage, trigger event
  EventSync *readwait = (EventSync*) handle;
  readwait->set();
#endif
}

// handler that will exit the server
int exitHandler(void *p){
  exit(0);
}

// a function that schedules an exit to occur after a while (2 seconds)
void scheduleExit(){
  TaskEventScheduler::AddEvent(tgetThreadNo(), exitHandler, 0, 0, 2000);
}
