//
// grpctcp.h
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

#ifndef _GRPCTCP_H
#define _GRPCTCP_H

#include <map>
using namespace std;

#include "tmalloc.h"
#include "os.h"
#include "options.h"
#include "util.h"
#include "scheduler.h"
#include "datastruct.h"
#include "datastructmt.h"
#include "task.h"
#include "tcpdatagram.h"

// This is the callback function passed on to an asynchronous RPC call.
// The callback is invoked when the RPC response arrives. Data has
// the unmarshalled response, and callbackdata is any data chosen
// by the entity which set up the callback function.
// The callback function should not free data, as it will be freed elsewhere.
// The place where it is freed depends on the return value. If the callback
// returns false, the RPC library frees data subsequently. If it returns
// true then it does not free the data and the application is supposed
// to do it by calling MsgBuffer::free().
typedef void (*RPCCallbackFunc)(char *data, int len, void *callbackdata);


// ***************************** CLIENT STUFF ********************************
class RPCTcp;
struct OutstandingRPC;

// This is the callback function passed on to an asynchronous RPC call.
// The callback is invoked when the RPC response arrives. Data has
// the unmarshalled response with length len, and callbackdata is any data
// chosen by the entity which set up the callback function.
// The callback function should not free data, as it will be freed by the
// RPC library.
typedef void (*RPCCallbackFunc)(char *data, int len, void *callbackdata);

// outstanding RPCs of a client
struct OutstandingRPC
{
  DatagramMsg dmsg;          // message headers and data
  RPCCallbackFunc callback;  // callback for reply
  void *callbackdata;        // data to be passed to callback
  u64 timestamp;             // when RPC call was made (used for retrying)
  Ptr<RPCTcp> rpcc;
  //int currtimeout;           // current timeout value
  //int nretransmit;           // number of retransmits
  int done;                  // whether reply has arrived already or not
                             // Invariant: done=true iff xid is not in
                             // OutstandingRequests
  // HashTable stuff
  OutstandingRPC *next, *prev, *snext, *sprev;
  int GetKey(){ return dmsg.xid; }
  static unsigned HashKey(int i){ return (unsigned)i; }
  static int CompareKey(int i1, int i2){
    if (i1<i2) return -1;
    else if (i1==i2) return 0;
    else return 1;
  }
};



// ******************************* SERVER STUFF ******************************

#define MAXRPCSERVERS 16


class RPCTaskInfo : public TaskInfo {
public:
 RPCTaskInfo(int hid, ProgFunc pf, void *taskdata, IPPort *s, u32 r, u32 x,
             u32 f, TaskMultiBuffer *t, char *d, int l)
   : TaskInfo(pf, taskdata)
  {
    handlerid = hid;
    src = *s;
    req = r;
    xid = x;
    flags = f;
    tmb = t;
    data = d;
    len = l;
    resp = 0;
    //seen = 0;
  }
  void setResp(Marshallable *r){ resp = r; }
  Marshallable *getResp(){ return resp; }

  int handlerid;
  
  // information coming from TCPDatagramCommunication
  IPPort src; 
  u32 req; 
  u32 xid; 
  u32 flags; 
  TaskMultiBuffer *tmb; 
  char *data;
  int len;

  // information used during the RPC processing
  MsgIdentifier msgid;
  bool seen; // whether the RPC was seen before or not

  // information to be returned
  Marshallable *resp;
};

typedef int (*RPCProc)(RPCTaskInfo *); // Parameter RPCTaskInfo includes
                                       // all information about the RPC

// information for an individual server
struct RPCServerInfo {
  RPCProc *procs; // handler of all procedure
  int nprocs;     // number of procedures
  int portno;
  int handlerid;  // id of the handler. This is currently just the index in
                  // the array.
};


// RPCTcp support smart pointers. Do not create regular pointers to
// instances of this class (or to instances of derived class). Use
// instead Ptr<RPCTcp>. Also, do not create instances on the stack.
class RPCTcp : private TCPDatagramCommunication
{
private:
  HashTableMT<U32,OutstandingRPC*> OutstandingRequests; // outstanding RPC's.
                                           // A map from xid to OutstandingRPC*
  Align4 u32 CurrXid;
  Align4 int refcount;
  friend class Ptr<RPCTcp>; // to support smart pointers to it
  
  // internal callback for synchronous RPCs
  static void waitCallBack(char *data, int len, void *callbackdata);

  RPCServerInfo Servers[MAXRPCSERVERS];
  int NextServer; // index of next server to be added

protected:
  OutstandingRPC *RequestLookupAndDelete(u32 xid);
  
  RPCProc *Procs;
  unsigned NProcs; // number of registered procedures, which will range
                   // from 0 to NFuncs-1

  static int RPCStart(RPCTaskInfo *rti);
  static int RPCEnd(RPCTaskInfo *rti);
  
  // handles a message from the TCP layer and dispatches RPCs
  void handleMsg(int handlerid, IPPort *dest, u32 req, u32 xid, u32 flags,
                 TaskMultiBuffer *tmb, char *data, int len);
  virtual void startupWorkerThread();  // workerThread calls this method
                                       // upon startup
  virtual void finishWorkerThread();   // workerThread calls this method
                                       // when ending
  
public:
  RPCTcp();
  ~RPCTcp(){
    exitThreads();
    mssleep(1000);
  }

  void launch(int nworkers){
    TCPDatagramCommunication::launch(nworkers,0);
  }

  // ---------------------------- Client methods ------------------------------

  // creates a thread that can make RPCs. Returns a local thread id
  // (not a pthread handle)
  int createThread(const char *threadname, OSTHREAD_FUNC_PTR startroutine,
                   void *threaddata, bool pinthread){
    return SLauncher->createThread(threadname, startroutine, threaddata,
                                   pinthread);
  }

  // wait for a thread to finish
  unsigned long waitThread(int threadno){
    return SLauncher->waitThread(threadno);
  }

  // initializes clients. Must be called once before clientconnect()
  void clientinit(){ TCPDatagramCommunication::clientinit(); }

  // connect as a client to a server. Needs to be called before
  // calling syncRPC or asyncRPC
  int clientconnect(IPPort dest){
    return TCPDatagramCommunication::clientconnect(dest);
  }
  int clientdisconnect(IPPort dest){
    return TCPDatagramCommunication::clientdisconnect(dest);
  }
    
  // callbackdata is data to be passed to the callback function
  // When RPC gets response, argument "data" will be deleted automatically,
  // so caller should not delete it again
  void asyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data,
                RPCCallbackFunc callback, void *callbackdata);

  // Returns a buffer that caller must later free using free().
  // Comment about parameter "data" for asyncRPC applies here too
  char *syncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data);
  
  // ---------------------------- Server methods ------------------------------
  void registerNewServer(RPCProc *procs, int nprocs, int portno);
  void waitServerEnd(void){ TCPDatagramCommunication::waitServerEnd(); }

  void exitThreads(void){ TCPDatagramCommunication::exitThreads(); }
};

#endif
