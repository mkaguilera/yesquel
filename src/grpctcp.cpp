//
// grpctcp.cpp
//
// TCP-based implementation of remote procedure calls. Runs on top of
// the tcpdatagram service.
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
#include <map>
#include <list>

#include "tmalloc.h"
#include "debug.h"
#include "grpctcp.h"
#include "scheduler.h"

RPCTcp::RPCTcp() :
  TCPDatagramCommunication(),
  OutstandingRequests(OUTSTANDINGREQUESTS_HASHTABLE_SIZE)
{
  refcount = 0;
  CurrXid=0;
  NextServer=0;
}

// these are intended to be overloaded by child classes
void RPCTcp::startupWorkerThread(){
}
void RPCTcp::finishWorkerThread(){
}

void RPCTcp::handleMsg(int handlerid, IPPort *dest, u32 req, u32 xid,
                       u32 flags, TaskMultiBuffer *tmb, char *data, int len){
  if (handlerid == -1){ // client stuff
    OutstandingRPC *orpc;
    orpc = RequestLookupAndDelete(xid);
    if (orpc && orpc->callback){
      orpc->callback(data, len, orpc->callbackdata);
    }
    if (orpc){
      delete orpc->dmsg.data;
      delete orpc;
    }

    freeMB(tmb);
  } else { // server stuff
    assert(0 <= handlerid && handlerid < NextServer);
    
    TaskScheduler *ts = tgetTaskScheduler();
    TaskInfo *ti = new RPCTaskInfo(handlerid, (ProgFunc) RPCStart, 0, dest,
                                   req, xid, flags, tmb, data, len);
    ti->setEndFunc((ProgFunc) RPCEnd); // set ending function
    ts->createTask(ti); // creates task
  }
  return;
}

//*********************************** CLIENT *********************************

OutstandingRPC *RPCTcp::RequestLookupAndDelete(u32 xid){
  U32 Xid(xid);
  int res;
  OutstandingRPC *it=0;
  res = OutstandingRequests.lookupRemove(Xid, 0, it);
  if (!res){ // found it
    it->done = true; // mark as done
  }
  return it;
}

void RPCTcp::asyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data,
                      RPCCallbackFunc callback, void *callbackdata){
  OutstandingRPC *orpc = new OutstandingRPC;
  orpc->dmsg.xid = AtomicInc32(&CurrXid);
  orpc->dmsg.flags = flags;
  orpc->dmsg.ipport = dest;
  orpc->dmsg.req = rpcno;
  orpc->dmsg.data = data;
  orpc->dmsg.freedata = 0;
  orpc->callback = callback;
  orpc->callbackdata = callbackdata;
  orpc->timestamp = (u64) Time::now();
  orpc->rpcc = this;
  orpc->done = 0;

  U32 Xid(orpc->dmsg.xid);
  OutstandingRequests.insert(Xid, orpc);

  sendMsg(&orpc->dmsg);
}

struct WaitCallbackData {
  EventSync *eventsync;
  char *retdata;
};

void RPCTcp::waitCallBack(char *data, int len, void *callbackdata){
  WaitCallbackData *wcbd = (WaitCallbackData *) callbackdata;

  wcbd->retdata = (char*) malloc(len);
  memcpy(wcbd->retdata, data, len);
  wcbd->eventsync->set();
  return;
}

char *RPCTcp::syncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data){
  EventSync es;
  WaitCallbackData wcbd;

  wcbd.eventsync = &es;
  asyncRPC(dest, rpcno, flags, data, &RPCTcp::waitCallBack, (void*) &wcbd);
  es.wait();
  return wcbd.retdata;
}

/******************************* SERVER **************************************/

int RPCTcp::RPCStart(RPCTaskInfo *rti){
  RPCTcp *rpctcp = (RPCTcp*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);

  assert((int)rti->req < rpctcp->Servers[rti->handlerid].nprocs); // ensure
                                                     // rpcno is within range

  rti->msgid.source = rti->src;
  rti->msgid.xid = rti->xid;

  rti->setFunc((ProgFunc)rpctcp->Servers[rti->handlerid].procs[rti->req]);
  return rpctcp->Servers[rti->handlerid].procs[rti->req](rti); // invoke
                                                               // procedure
}

int RPCTcp::RPCEnd(RPCTaskInfo *rti){
  //RPCCachedResult *it;
  RPCTcp *rpctcp = (RPCTcp*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  DatagramMsg dmsg;

  dmsg.data = rti->getResp();
  dmsg.ipport = rti->src;
  dmsg.req = rti->req;
  dmsg.xid = rti->xid;
  dmsg.flags = rti->flags; // or should we clear the flags?
  //dmsg.freedata = idempotent ? true : false;
  // // if not idempotent then do not free result after sending
  // //    since we are caching it
  // // if idempotent then we can free result
  dmsg.freedata = true;
  
  rpctcp->sendMsgFromWorker(&dmsg);
   
  freeMB(rti->tmb); // free incoming RPC data
  return SchedulerTaskStateEnding;
}

void RPCTcp::registerNewServer(RPCProc *procs, int nprocs, int portno){
  assert(NextServer < MAXRPCSERVERS);
  Servers[NextServer].procs = procs;
  Servers[NextServer].nprocs = nprocs;
  Servers[NextServer].portno = portno;
  Servers[NextServer].handlerid = NextServer;
  addServer(Servers[NextServer].handlerid, Servers[NextServer].portno);
  ++NextServer;
}



