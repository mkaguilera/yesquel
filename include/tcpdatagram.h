//
// tcpdatagram.h
//
// Reliable datagram service based on TCP. RPCs are implemented
// on top of this service.
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

#ifndef _TCPDATAGRAM_H
#define _TCPDATAGRAM_H

#include <unistd.h>
#include <sys/eventfd.h>

#include "tmalloc.h"
#include "options.h"
#include "gaiatypes.h"
#include "util.h"
#include "datastruct.h"
#include "ipmisc.h"
#include "task.h"

// info about an RPC to be sent
struct DatagramMsg {
  Marshallable *data;
  IPPort ipport;
  u32 req;  // see wire format below for the meaning of req, xid, flags
  u32 xid;
  u32 flags;
  bool freedata;
  DatagramMsg(){}
  DatagramMsg(Marshallable *d, IPPort ip, u32 r, u32 x, u32 f, bool fd) : 
    data(d), ipport(ip), req(r), xid(x), flags(f), freedata(fd){}
};

#define FLAG_HID(hid) ((hid)<<16)//given hid, returns corresponding bits in flag
#define FLAG_GET_HID(flag) ((flag)>>16) // extract hid bits from flag

// wire format for RPC header
struct DatagramMsgHeader {
  u32 cookie; // cookie to identify beginning of header
  u32 flags;  // The 16 high bits of flags are a hash id, which determines which
              // server thread will handle the request. Requests with the same
              // hashid are guaranteed to be handled by the same server thread.
              // The 16 low bits of flags are currently unused flags
  u32 size; // size of payload (does not include header or footer)
  u32 req;  // request number (rpc number)
  u32 xid;  // unique per-sender identifier for request
};



#define REQ_HEADER_COOKIE 0xbebe    // cookie added to beginning of datagram
#define MAXIOVECSERIALIZE 32        // max # of iovec that an RPC may produce
#define SEND_IOVEC_QUEUESIZE 1024   // length of iovec send queue

#define DISABLE_NAGLE

#ifdef CONCURMARKER
#include <cvmarkersobj.h>
using namespace Concurrency::diagnostic;
#define CMARKER(x) TcpMarkerSeries.write_flag(x)
#else
#define CMARKER(x)
#endif
class MsgBuffer;

// This is a buffer that tracks a buffer and a refcount for it.
// When the refcount reaches zero, the buffer is freed with free().
// The buffer being tracked should be allocated with malloc()
class TaskMultiBuffer {
private:
  Align4 int refcount;
public:
  u8 threadno;
  char *base;

  TaskMultiBuffer(char *b, int count=0); // allocate b with malloc()
  ~TaskMultiBuffer();
  void decRef();
  void incRef();
};

class TCPDatagramCommunication {
  friend class MsgBuffer;
private:
  struct ReceiveState {
    char *Buf;  // beginning of buffer being filled
    int Buflen; // total allocated size
    char *Ptr;  // current position being filled
    int Filled; // offset of current position being filled (==Ptr-Buf)
    ReceiveState(){ Buf = Ptr = 0; }
  };
  struct SendQueueEntry {
  private:
    void marshallRPC();
  public:
    DatagramMsg dmsg;
    DatagramMsgHeader header; // space for wire RPC header,
                              // to be included in iovec to send
    iovec bufs[MAXIOVECSERIALIZE];
    int nbufs;
    int nbytes; // number of bytes in all iovecs
    SendQueueEntry *next;
    SendQueueEntry(DatagramMsg &dm){ nbufs=0; dmsg = dm; marshallRPC(); }
  };
  struct TCPStreamState {
    int fd;
    IPPort ipport;
    int handlerid;
    ReceiveState rstate; // current receive state
    SLinkList<SendQueueEntry> sendQueue; // send queue
    int sendQueueBytesSkip; // how many bytes to skip from send queue (because
       // they were sent in a previous partial writev). This should remain
       // inside first item in sendQueue; if it would include all
       // of this item, it would have been removed from sendQueue
    int sendeagain; // whether got EAGAIN the last time we
                    // tried to write to socket
    TCPStreamState(){ fd = -1; sendQueueBytesSkip = 0; sendeagain = 0; }
    ~TCPStreamState(){
      if (fd >= 0) close(fd);
      if (rstate.Buf) free(rstate.Buf);
      SendQueueEntry *sqe;
      while (!sendQueue.empty()){
        sqe = sendQueue.popHead();
        delete sqe;
      }
    }
  };
  struct TCPStreamStatePtr {
    TCPStreamState *tssptr;
    TCPStreamStatePtr(){ tssptr=0; }
    TCPStreamStatePtr(TCPStreamState *tp){ tssptr = tp; }
    static int cmp(TCPStreamStatePtr &left, TCPStreamStatePtr &right){
      long long l = (long long) left.tssptr;
      long long r = (long long) right.tssptr;
      if (l < r) return -1;
      if (l > r) return +1;
      return 0;
    }
  };
  
  SkipList<IPPort,TCPStreamState*> IPPortMap; // maps ip-port to TCPStreamState
  Set<TCPStreamStatePtr> *PendingSendsBeforeEpoll; // connections with pending
                             // data to be sent before epoll
  bool ForceEndThreads; // when set to true, threads will exit asap
  
  // entry in linked list
  struct RPCSendEntry {
    DatagramMsg dmsg;
    DatagramMsgHeader header; // space for RPC header, which will be included
                              // in actual iovec when sending
    RPCSendEntry *next;
  };
  class IPPortInfoTCP {
  public:
    iovec iovecbufs[SEND_IOVEC_QUEUESIZE];
    int nextiovec; // next available iovec buf so far
    u32 nbytes; // number of bytes so far
    SLinkList<RPCSendEntry> gcQueue;
    IPPortInfoTCP(){ nextiovec = 0;  nbytes = 0;}
    ~IPPortInfoTCP(){
      RPCSendEntry *rse;
      while (!gcQueue.empty()){
        rse = gcQueue.popHead();
        if (rse->dmsg.freedata) 
          delete rse->dmsg.data;
        delete rse;
      }
    }
  };

  // stuff for receiving
  void updateState(int handlerid, ReceiveState &s, IPPort src, int len);
  static OSTHREAD_FUNC receiveThread(void *parm);

  // worker thread
  Semaphore workerInitSync; // used to wait for all workers to start
  virtual void startupWorkerThread(); // workerThread calls this upon startup
  virtual void finishWorkerThread();   // workerThread calls this when ending
  static OSTHREAD_FUNC workerThread(void *parm);
  static void immediateFuncAddIPPortFd(TaskMsgData &msgdata, TaskScheduler *ts,
                                       int srcthread);

  // stuff for sending
  static int marshallRPC(iovec *iovecbuf, int bufsleft, RPCSendEntry *rse);
  static void immediateFuncSend(TaskMsgData &msgdata, TaskScheduler *ts,
                                int srcthread);
  void sendTss(TCPStreamState *tss);
  
  
  // called whenever a client connect()s or a server accept()s
  void startReceiving(IPPort ipport, int fd, int handlerid, int workerno);

  // Client-specific stuff
  int chooseWorkerForClient(IPPort client); // given client, returns which
                                            // worker thread should handle it

  // Server-specific stuff
  struct NewServer {
    int fd;         // fd where we are supposed to listen and accept
    int handlerid;  // id of handler for incoming server messages,
                    // passed to handleMsg()
  };
  OSThread_t ServerThr; // thread for listening for new connections
  static OSTHREAD_FUNC serverThread(void *parm);
  int ClientCount;
  int ServerEventFd;
  BoundedQueue<NewServer*> newServerQueue;

protected:
  // Specialize this function to provide handler of incoming messages.
  // handleMsg is given the handlerid (which identifies the port/server that
  // got the message), the address of the sender (src), req and xid of the RPC,
  // a TaskMultiBuffer (used to free the message buffer),
  // and a pointer to a buffer containing the rest of the UDP message.
  // handleMsg should free the buffer by calling freeMB with the
  // TaskMultiBuffer parameter.
  // It is not recommended the handleMsg holds on to the buffer for a
  // long time, since the buffer is shared with other requests received
  // together, so holding on to the buffer will
  // keep more memory allocated than needed. Thus, if handleMsg needs to keep
  // the data, it should make a private copy and then free the buffer.
  virtual void handleMsg(int handlerid, IPPort *src, u32 req, u32 xid,
                     u32 flags, TaskMultiBuffer *tmb, char *data, int len)=0;

  static void freeMB(TaskMultiBuffer *bufbase); // add an entry to the
                                               // batch of multibufs to free

public:
  TCPDatagramCommunication();
  virtual ~TCPDatagramCommunication();

  // Adds a server listening on given port.
  // handlerid is a value that gets passed to handleMsg for any messages
  // arriving for that server.
  int addServer(int handlerid, int port);

  // Client-specific stuff
  // initializes clients. Must be called once before clientconnect()
  void clientinit(){ initThreadContext("CLIENT",0); }

  // connects to a server. Must be called before the client can send to it
  int clientconnect(IPPort dest);

  // disconnects from server
  int clientdisconnect(IPPort dest);
  
  // launches the server and worker threads.
  // workerthreads indicate the number of threads for handling receipts
  // wait=true means to never return
  int launch(int workerthreads, int wait=1);
  void exitThreads(); // causes scheduler of threads to exit

  void sendMsgFromWorker(DatagramMsg *dmsg); // send a message from the worker
                                           // thread who received the message.
  // This is the thread associated with the fd. Intended to be used when server
  // replies to incoming message.
  
  void sendMsg(DatagramMsg *dmsg); // send an RPC message from any thread with
      // a thread context (e.g., obtained by calling initThreadContext()).
      // Intended to be used by a client

  // wait for server to end
  void waitServerEnd(void){ pthread_join(ServerThr, 0); }
};

#endif
