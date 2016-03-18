//
// tcpdatagram.cpp
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


#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <map>
#include <list>

#include "tmalloc.h"
#include "os.h"
#include "datastruct.h"
#include "tcpdatagram.h"
#include "util.h"

using namespace std;

//------------------------------------ RECEIVING -----------------------------

// returns 0 if ok, non-zero if send queue is full
void TCPDatagramCommunication::updateState(int handlerid, ReceiveState &s,
                                           IPPort src, int len){
  DatagramMsgHeader *header;
  int totalsize;
  char *newbuf;
  TaskMultiBuffer *tmb;
  //static int minsize=99999, maxsize=0;

  s.Ptr += len;
  s.Filled = (int)(s.Ptr - s.Buf);
  if (s.Filled < (int)sizeof(DatagramMsgHeader)) return;
  header = (DatagramMsgHeader*) s.Buf;
  assert(header->cookie == REQ_HEADER_COOKIE);
  // totalsize is how much we are supposed to receive
  totalsize = sizeof(DatagramMsgHeader) + header->size;
  if (s.Filled < totalsize){ // didn't fill everything yet
    if (totalsize > s.Buflen){ // current buffer is too small;
                               // copy to new buffer
      newbuf = (char*) malloc(totalsize); assert(newbuf);
      memcpy(newbuf, s.Buf, s.Filled);
      free(s.Buf);
      s.Buf = newbuf;
      s.Buflen = totalsize;
      s.Ptr = newbuf+s.Filled;
    }
    return;
  }

  if (s.Filled == totalsize){   // we filled everything exactly
    // call application handler
    tmb =  new TaskMultiBuffer(s.Buf, 1); // TaskMultiBuffer is used to
                                          // later free s.Buf
    handleMsg(handlerid, &src, header->req, header->xid, header->flags,
              tmb, s.Buf+sizeof(DatagramMsgHeader), header->size);
    
    s.Buf = (char*) malloc(TCP_RECLEN_DEFAULT); assert(s.Buf);
    s.Ptr = s.Buf;
    s.Buflen = TCP_RECLEN_DEFAULT;
    s.Filled = 0;
    return;
  }

  // Filled > totalsize
  unsigned extrasize, newlen;
  extrasize = s.Filled-totalsize;  // number of extra bytes we received
  char *extraptr = s.Buf + totalsize; // where the extra bytes are
  int sizenewreq;

#define MAXREQUESTSPERRECEIVE 10000
  char *bufs[MAXREQUESTSPERRECEIVE];
  int bufindex = 0;
  char *bufnewreq;

  bufs[bufindex] = s.Buf;
  ++bufindex; assert(bufindex < MAXREQUESTSPERRECEIVE);

  while (extrasize > sizeof(DatagramMsgHeader) &&
         extrasize >= sizeof(DatagramMsgHeader) +
         ((DatagramMsgHeader*)extraptr)->size){
    // we have a full request to process
    sizenewreq = sizeof(DatagramMsgHeader) +
      ((DatagramMsgHeader*)extraptr)->size;

    assert(((DatagramMsgHeader*) extraptr)->cookie == REQ_HEADER_COOKIE);
    bufs[bufindex] = extraptr;
    ++bufindex; assert(bufindex < MAXREQUESTSPERRECEIVE);
    extrasize -= sizenewreq;
    extraptr += sizenewreq;
  }

  // now extraptr and extrasize still refers to an incomplete chunk at the end
  tmb = new TaskMultiBuffer(s.Buf, bufindex); // TaskMultiBuffer is used to
                   // later free s.Buf. It will expect bufindex requests
                   // before freeing s.Buf
  if (extrasize <= TCP_RECLEN_DEFAULT){
    newlen = TCP_RECLEN_DEFAULT;
    newbuf = (char*) malloc(newlen); assert(newbuf);
  }
  else { // we have at least the header, so we know the size of the next request
    newlen = sizeof(DatagramMsgHeader) + ((DatagramMsgHeader*)extraptr)->size;
    newbuf = (char*) malloc(newlen); assert(newbuf);
  }
  memcpy(newbuf, extraptr, extrasize);

  // state, for next iteration of the loop
  s.Buf = newbuf;
  s.Ptr = newbuf + extrasize;
  s.Buflen = newlen;
  s.Filled = (int)(s.Ptr-s.Buf);

  // call application handler for all complete requests
  for (int i=0; i < bufindex; ++i){
    bufnewreq = bufs[i];
    header = (DatagramMsgHeader*) bufnewreq;
    assert(header->cookie == REQ_HEADER_COOKIE);
    sizenewreq = sizeof(DatagramMsgHeader) + header->size;

    // call application handler
    header = (DatagramMsgHeader*) bufnewreq;
    handleMsg(handlerid, &src, header->req, header->xid, header->flags,
              tmb, bufnewreq+sizeof(DatagramMsgHeader), header->size);

    //if ((int)header->size < minsize) minsize = header->size;
    //if ((int)header->size > maxsize) maxsize = header->size;
  }
}

struct TaskMsgDataAddIPPortFd {
  TaskMsgDataAddIPPortFd(IPPort i, i64 f, i64 h){
    ipport = i;
    fd = f;
    handlerid = h;
  }
  IPPort ipport;
  i64 fd;
  i64 handlerid;
};

// add fd to list of fds being monitored by one of the worker threads,
// so that worker can handle sending and receiving of data. To be called after
// a client connect()s or a server accept()s.
// workerno indicates which of the workers will handle this fd. That number
// will be mod'ed by the actual number of workers in the system.
void TCPDatagramCommunication::startReceiving(IPPort ipport, int fd,
                                              int handlerid, int workerno){
  // ask worker thread to start handling fd+ipport
  TaskMsgDataAddIPPortFd addmsg(ipport, fd, handlerid);
  sendIFMsg(gContext.hashThread(TCLASS_WORKER, workerno),
            IMMEDIATEFUNC_ADDIPPORTFD, (void*) &addmsg,
            sizeof(TaskMsgDataAddIPPortFd));
}


//------------------------------------ WORKER ----------------------------------


void TCPDatagramCommunication::immediateFuncAddIPPortFd(TaskMsgData &msgdata,
                                     TaskScheduler *ts, int srcthread){
  TaskMsgDataAddIPPortFd *addmsg = (TaskMsgDataAddIPPortFd*) &msgdata;
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*)
    tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  int epfd = (int) (long long)
    tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM_WORKER);
  int res;

  // initialize TCPSTreamState for this new connection
  TCPStreamState *tss = new TCPStreamState;
  tss->fd = addmsg->fd;
  tss->ipport = addmsg->ipport;
  tss->handlerid = addmsg->handlerid;
  tss->rstate.Buf = (char*) malloc(TCP_RECLEN_DEFAULT); assert(tss->rstate.Buf);
  tss->rstate.Buflen = TCP_RECLEN_DEFAULT;
  tss->rstate.Ptr = tss->rstate.Buf;
  tss->rstate.Filled = 0;
  tss->sendeagain = 0;

  tdc->IPPortMap.insert(addmsg->ipport, tss); // associate ip-port with
                                              // TCPStreamState just created
  
  // add fd to list of things being watched
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLHUP | EPOLLET;
  ev.data.ptr = (void*) tss;
  res = epoll_ctl(epfd, EPOLL_CTL_ADD, addmsg->fd, &ev); assert(res==0);
}

void TCPDatagramCommunication::sendTss(TCPStreamState *tss){
  int firstbuf, firstoff;
  int byteslefttoskip; // bytes left to skip
  SendQueueEntry *sqe;
  iovec *bufs;
  int nbytes;
  int currbuf;
  int nreqscombined;
  int nbufscombined;

  if (!tss->sendQueue.getFirst()) return;
  bufs = new iovec[SEND_IOVEC_QUEUESIZE];
  
  do {
    currbuf = 0;
    nreqscombined=0;
    nbufscombined=0;
    byteslefttoskip = tss->sendQueueBytesSkip;
    sqe = tss->sendQueue.getFirst();
    while (sqe){
      // skip the first few buffers based on byteslefttoskip
      firstbuf = 0;
      firstoff = 0;

      // if there is anything to skip, skip
      while (byteslefttoskip > 0){
        if ((int)sqe->bufs[firstbuf].iov_len <= byteslefttoskip){
          byteslefttoskip -= sqe->bufs[firstbuf].iov_len;
          ++firstbuf;
          assert(firstbuf < sqe->nbufs); // should not exhaust all bufs in sqe
        } else {
          firstoff = byteslefttoskip;
          byteslefttoskip = 0;
        }
      }

      if (currbuf + sqe->nbufs >= SEND_IOVEC_QUEUESIZE)
        break; // exhausted iobufs
        
      assert(sqe->nbufs < SEND_IOVEC_QUEUESIZE);
      ++nreqscombined;
      nbufscombined += sqe->nbufs;
      while (firstbuf < sqe->nbufs){
        bufs[currbuf] = sqe->bufs[firstbuf];
        if (firstoff){
          bufs[currbuf].iov_base = (char*) bufs[currbuf].iov_base + firstoff;
          bufs[currbuf].iov_len -= firstoff;
          firstoff = 0;
        }
        ++currbuf;
        ++firstbuf;
      } // while
      assert(firstoff==0);
      sqe = tss->sendQueue.getNext(sqe);
    } // while (sqe)

    // send iobufs
    assert(currbuf >= 1);
    tss->sendeagain = 0;

    nbytes = writev(tss->fd, bufs, currbuf);
    if (nbytes <= 0){ // could not write anything
      if (errno == EAGAIN) tss->sendeagain = 1;
      if (nbytes == 0 || errno == EAGAIN) break;
      goto end;
    }
    nbytes += tss->sendQueueBytesSkip;
  
    while ((sqe = tss->sendQueue.getFirst()) != 0){
      if (sqe->nbytes <= nbytes){ // entire entry was sent
        nbytes -= sqe->nbytes;
        // free entry and remove it from sendQueue
        if (sqe->dmsg.freedata) delete sqe->dmsg.data;
        tss->sendQueue.popHead();
        delete sqe;
      } else { // nbytes < sqe->nbytes
        tss->sendQueueBytesSkip = nbytes;
        break;
      }
    }
    if (!sqe){ // common case
      assert(nbytes==0);
      tss->sendQueueBytesSkip = 0;
    }
  } while (!tss->sendQueue.empty());
 end:
  delete [] bufs;
}

// these are intended to be overloaded by child classes
void TCPDatagramCommunication::startupWorkerThread(){
}
void TCPDatagramCommunication::finishWorkerThread(){
}


#define MAX_EPOLL_EVENTS 256
OSTHREAD_FUNC TCPDatagramCommunication::workerThread(void *parm){
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication *) parm;
  TaskScheduler *ts = tgetTaskScheduler();
  int epfd = epoll_create1(0); assert(epfd != -1);
  eventfd_t eventdummy;
  int myworkerno = gContext.indexWithinClass(TCLASS_WORKER, tgetThreadNo());
  
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM, tdc);
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM_WORKER,
                  (void*)(long long) epfd);
  
  ts->assignImmediateFunc(IMMEDIATEFUNC_ADDIPPORTFD, immediateFuncAddIPPortFd);
  ts->assignImmediateFunc(IMMEDIATEFUNC_SEND, immediateFuncSend);

  tdc->startupWorkerThread(); // invokes startup code

  int i, n, res;
  struct epoll_event *epevents=0;
  TCPStreamState *tss=0;
  TCPStreamState sleepeventtss;
  epevents = new epoll_event[MAX_EPOLL_EVENTS];
  int nread=0;

  int sleepeventfd = ts->getSleepEventFd();
  struct epoll_event ev;
  sleepeventtss.fd = sleepeventfd;
  ev.events = EPOLLIN;
  ev.data.ptr = (void*) &sleepeventtss;
  res = epoll_ctl(epfd, EPOLL_CTL_ADD, sleepeventfd, &ev); assert(res==0);
  
  int something;
  int timeout=0;

  tdc->workerInitSync.signal(); // indicate that we have initialized
  
  while (!tdc->ForceEndThreads){
    something = ts->runOnce(); // run event schedule loop once
                  // this will handle immediate functions to add new
                  // things to the epoll set:
                  //     after adding, must receive anything that already exists

    // go through pendingsends and send anything for which we did not get
    // EAGAIN before
    if (!tdc->PendingSendsBeforeEpoll[myworkerno].empty()){
      SetNode<TCPStreamStatePtr> *it;
      TCPStreamState *tssptr;
      for (it = tdc->PendingSendsBeforeEpoll[myworkerno].getFirst();
           it != tdc->PendingSendsBeforeEpoll[myworkerno].getLast();
           it = tdc->PendingSendsBeforeEpoll[myworkerno].getNext(it)){
        tssptr = it->key.tssptr;
        assert(!tssptr->sendeagain);
        tdc->sendTss(tssptr);
      }
      tdc->PendingSendsBeforeEpoll[myworkerno].clear();
    }
    
    if (!something){ // start sleep cycle
      ts->setAsleep(1);
      timeout = ts->findSleepTimeout();
      //printf("Going to sleep for %d\n", timeout);
    } else timeout = 0;
    
    n = epoll_wait(epfd, epevents, MAX_EPOLL_EVENTS, timeout);
    if (!something) ts->setAsleep(0);
      
    for (i=0; i < n; ++i){
      tss = (TCPStreamState*) epevents[i].data.ptr;
      if (tss->fd == sleepeventfd){ // used just to wake us up
        //printf("Woken from sleep\n");
        eventfd_read(sleepeventfd, &eventdummy);
        continue;
      }
          
      if (epevents[i].events & EPOLLIN){ // read available
        //if (ts->checkSendQueuesAlmostFull()){
          // **!** if internal send queues are almost full, do not receive
          // TCP packets
          // do something here
        //}
        
        while (1){
          nread = read(tss->fd, tss->rstate.Ptr,
                       tss->rstate.Buflen - tss->rstate.Filled);
          if (nread < 0){
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
          } else if (nread == 0) break;
          else {
            // update the state of this stream (and if entire message received,
            // invoke handler)
            tdc->updateState(tss->handlerid, tss->rstate, tss->ipport, nread);
          }
        }
      }
      if (epevents[i].events & EPOLLOUT){ // write available
        tdc->sendTss(tss);
      }
      if (epevents[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)){
        // problem with fd
        close(tss->fd);
      }
    }
  }

  tdc->finishWorkerThread(); // invokes cleaning up code

  delete [] epevents;
  return 0;
}

//------------------------------------ SENDING -------------------------------

void TCPDatagramCommunication::SendQueueEntry::marshallRPC(){
  int bufsleft = MAXIOVECSERIALIZE;
  int size;
  
  // fill RPC header
  header.cookie = REQ_HEADER_COOKIE;
  // header->size is set below
  header.req = dmsg.req;
  header.xid = dmsg.xid;
  header.flags = dmsg.flags;
  // include it in buffer to send
  bufs[0].iov_base = (char*) &header;
  bufs[0].iov_len = sizeof(DatagramMsgHeader);
  nbufs = 1;
  --bufsleft;

  // marshall RPC body
  nbufs += dmsg.data->marshall(bufs+1, bufsleft);

  // calculate length of marshalled data and store it in header
  size=0;
  for (int i=1; i < nbufs; ++i) size += bufs[i].iov_len;
  header.size = size;
  nbytes = size + bufs[0].iov_len; // include header size in nbytes
}
 


void TCPDatagramCommunication::immediateFuncSend(TaskMsgData &msgdata,
                                                 TaskScheduler *ts, int srcthread){
  DatagramMsg *dmsg = (DatagramMsg*) &msgdata;
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*)
    tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  tdc->sendMsgFromWorker(dmsg);
}

// chooses which worker thread will handle a given client
int TCPDatagramCommunication::chooseWorkerForClient(IPPort client){
  return client.ip;
}

void TCPDatagramCommunication::sendMsgFromWorker(DatagramMsg *dmsg){
  int res;
  TCPStreamState *tss, **rettss = 0;
  int myworkerno = gContext.indexWithinClass(TCLASS_WORKER, tgetThreadNo());
  res = IPPortMap.lookup(dmsg->ipport, rettss); assert(res==0);
  tss = *rettss;
  SendQueueEntry *sqe = new SendQueueEntry(*dmsg);
  tss->sendQueue.pushTail(sqe);
  if (!tss->sendeagain){ // if didn't get EAGAIN, then must send before epoll
    TCPStreamStatePtr tssp(tss);
    PendingSendsBeforeEpoll[myworkerno].insert(tssp);
  }
}

void TCPDatagramCommunication::sendMsg(DatagramMsg *dmsg){
  // otherwise, must send a message to one of the workers. This is the case at
  // the client.
  assert(sizeof(DatagramMsg) <= TASKSCHEDULER_TASKMSGDATA_SIZE);
  TaskMsg msg;
  int workerthread; // which of the worker threads to make the request to

  *(DatagramMsg*)&msg.data = *dmsg; // copy dmsg

  workerthread = gContext.hashThread(TCLASS_WORKER,
                                     chooseWorkerForClient(dmsg->ipport));
  msg.dest = TASKID_CREATE(workerthread, IMMEDIATEFUNC_SEND);
  msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
  tgetTaskScheduler()->sendMessage(msg);    
}
 

//------------------------------- INIT + LISTENING ---------------------------

TCPDatagramCommunication::TCPDatagramCommunication() : newServerQueue(1024)
{
  ServerThr = 0;
  ClientCount = 0;
  ServerEventFd = eventfd(0, EFD_NONBLOCK); assert(ServerEventFd != -1);
  ForceEndThreads = false;
  PendingSendsBeforeEpoll = 0;
}

TCPDatagramCommunication::~TCPDatagramCommunication(){
  if (!ForceEndThreads) // exitThreads sets ForceEndThreads, so this tests
    // ensures exitThreads is not called twice
    exitThreads();
  if (PendingSendsBeforeEpoll) delete [] PendingSendsBeforeEpoll;
}

static void setnonblock(int fd){
  int fl, res;
  fl = fcntl(fd, F_GETFL, 0); assert(fl != -1);
  fl |= O_NONBLOCK;
  res = fcntl(fd, F_SETFL, fl); assert(res != -1);
}

int TCPDatagramCommunication::addServer(int handlerid, int port){
  int fdlisten;
  sockaddr_in sin_server;
  int res;
  
  fdlisten = socket(AF_INET, SOCK_STREAM, 0); 
  if (fdlisten == -1){
    printf("socket() failed: %d\n", errno);
    return -1;
  }
  setnonblock(fdlisten);
#ifdef DISABLE_NAGLE
  int value = 1;
  res = setsockopt(fdlisten, IPPROTO_TCP, TCP_NODELAY, (char*) &value,
                   sizeof(int));
  if (res)
    printf("%016llx setsockopt on TCP_NODELAY of listen socket: error %d\n",
           (long long) Time::now(), errno);
#endif
  
  memset(&sin_server, 0, sizeof(sockaddr_in));
  sin_server.sin_family = AF_INET;
  sin_server.sin_addr.s_addr = htonl(INADDR_ANY);
  sin_server.sin_port = htons(port);
  res = ::bind(fdlisten, (sockaddr*) &sin_server, sizeof(sin_server));
  if (res == -1){
    printf("bind() failed: %d\n", errno);
    return -1;
  }

  // pass to server: fdlisten, handlerid
  NewServer *ns = new NewServer;
  ns->fd = fdlisten;
  ns->handlerid = handlerid;
  newServerQueue.enqueue(ns);

  // unblock server from epoll
  res = eventfd_write(ServerEventFd, 1); assert(res==0);
  return 0;
}

OSTHREAD_FUNC TCPDatagramCommunication::serverThread(void *parm){
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*) parm;
  int fdaccept;
  int res=0;
  TaskScheduler *ts;
  NewServer *ns, *epollptr;
  NewServer ServerEventFdNs; // epoll data for the ServerEventFd
  int workerno;

  SLauncher->initThreadContext("SERVER", 0); // allows this thread to send
                                             // messages to others
  ts = tgetTaskScheduler();
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM, tdc);

  // *!* increase thread priority above normal?

  //WARNING_INIT(); // initializes warning task

  int i, n, epfd;
  struct epoll_event ev;
  struct epoll_event *epevents=0;
  eventfd_t eventdummy;
  epevents = new epoll_event[MAX_EPOLL_EVENTS];

  ServerEventFdNs.fd = tdc->ServerEventFd;
  ServerEventFdNs.handlerid = -1;
  epfd = epoll_create1(0); assert(epfd != -1);
  ev.events = EPOLLIN;
  ev.data.ptr = (void*) &ServerEventFdNs;
  res = epoll_ctl(epfd, EPOLL_CTL_ADD, tdc->ServerEventFd, &ev);
  assert(res==0);  

  UDPDest ud;
  while (!tdc->ForceEndThreads){
    ts->runOnce();
    
    n = epoll_wait(epfd, epevents, MAX_EPOLL_EVENTS, -1);
      
    for (i=0; i < n; ++i){
      epollptr = (NewServer*) epevents[i].data.ptr;
      if (epollptr->fd == tdc->ServerEventFd){ // used just to wake us up
        eventfd_read(tdc->ServerEventFd, &eventdummy);
        while (!tdc->newServerQueue.empty()){
          ns = tdc->newServerQueue.dequeue();
          ev.events = EPOLLIN;
          ev.data.ptr = (void*) ns;
          res = epoll_ctl(epfd, EPOLL_CTL_ADD, ns->fd, &ev); assert(res==0);
          res = listen(ns->fd, SOMAXCONN);
          if (res == -1) {
            printf("listen() failed: %d\n", errno);
            continue;
          }
        }
        continue;
      }

      if (epevents[i].events & EPOLLIN){
        // read available, new connection to accept
        ud.sockaddr_len = sizeof(sockaddr_in);
        fdaccept = accept(epollptr->fd, (sockaddr*) &ud.destaddr,
                          &ud.sockaddr_len);
        if (fdaccept == -1){
          int err = errno;
          if (err != 0){
            printf("%016llx accept:socket_error %d from %08x\n",
                 (long long) Time::now(), errno, *(int*)&ud.destaddr.sin_addr);
          }
          continue;
        }
        //printf("accept:connection from %08x\n", *(int*)&ud.destaddr.sin_addr);

        setnonblock(fdaccept);
    
#ifdef DISABLE_NAGLE
        int value = 1;
        res = setsockopt(fdaccept, IPPROTO_TCP, TCP_NODELAY, (char*) &value,
                         sizeof(int));
        if (res)
          printf("setsockopt on TCP_NODELAY of accept socket: error %d\n",
                 errno);
#endif
        
        // determines which worker will handle this fd
        workerno = tdc->ClientCount++;
        //printf("Fd %d going to worker %d\n", fdaccept, workerno);
        tdc->startReceiving(ud.getIPPort(), fdaccept, epollptr->handlerid,
                            workerno);
      } // if

      if (epevents[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)){
        // problem with fd
        if (epollptr->fd != tdc->ServerEventFd){
          printf("Problem with fd, closing it\n");
          close(epollptr->fd);
          delete epollptr;
        }
      } // if
    } // for
  }
  delete [] epevents;
  return 0;
}

// should be called at the beginning by a single thread.
// This is because there are no locks protecting IPPortMap
int TCPDatagramCommunication::clientconnect(IPPort dest) {
  int fd;
  int res;
  UDPDest udpdest(dest);

  fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1){
    printf("socket() failed: %d\n", errno);
    return -1;
  }
  
  do {
    res = connect(fd, (sockaddr*) &udpdest.destaddr,
                  (int) udpdest.sockaddr_len);
    if (res == -1) {
      printf("connect failed: %d\n", errno);
      mssleep(1000);
    }
  } while (res == -1);

  setnonblock(fd);
#ifdef DISABLE_NAGLE
  int value = 1;
  res = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*) &value, sizeof(int));
  if (res) printf("setsockopt on connect socket: error %d\n", errno);
#endif  

  startReceiving(dest, fd, -1, chooseWorkerForClient(dest));
  return 0;
}

int TCPDatagramCommunication::clientdisconnect(IPPort dest) {
  // lookup dest in IPPortMap
  TCPStreamState **tss;
  int res;
  res = IPPortMap.lookup(dest, tss);
  if (res) return -1; // no such client
  if (*tss){
    delete *tss; // FIXME: potential race: deleting tss will free
                 // (*tss)->rstate.Buf and (*tss)->sendQueue, but worker thread
                 // may be using this if it is receiving data on this
                 // connection. The fix is to wait for worker to be done
                 // receiving anything before deleting *tss
    *tss = 0; // associate empty TCPStreamState with dest in IPPortMap
  }
  return 0;
}

TaskMultiBuffer::TaskMultiBuffer(char *b, int count){
  refcount = count;
  base = b;
  threadno = tgetThreadNo(); // get this from thread context
}
TaskMultiBuffer::~TaskMultiBuffer(){
  free(base);
}
void TaskMultiBuffer::decRef(){
  if (AtomicDec32(&refcount) <= 0) delete this;
}
void TaskMultiBuffer::incRef(){ AtomicInc32(&refcount); }
// sends message to free a TaskMultiBuffer

void TCPDatagramCommunication::freeMB(TaskMultiBuffer *bufbase){
  bufbase->decRef();
}

int TCPDatagramCommunication::launch(int workerthreads, int wait){
  int i;
  int threadno;
  int res;
  assert(!ForceEndThreads);
  int nWorkerThreads;

  if (!SLauncher){
    printf("Scheduler not initialized. Must call tinitScheduler()\n");
    exit(1);
  }

  // server has thread that listens for connections
  res = OSCreateThread(&ServerThr, serverThread, (void*) this); assert(res==0);
  nWorkerThreads = workerthreads;

  // allocate array of Set<TCPStreamStatePtr>
  PendingSendsBeforeEpoll = new Set<TCPStreamStatePtr>[workerthreads];
  
  // **!** need gContext.setNThreads and setThread for TCLASS_SERVER?
  // This is so that threads can communicate with it for disk I/O later

  gContext.setNThreads(TCLASS_WORKER, nWorkerThreads);

  for (i = 0; i < nWorkerThreads; ++i){
    threadno = SLauncher->createThread("TCPWORKER", workerThread,
                                       (void*) this, true);
    gContext.setThread(TCLASS_WORKER, i, threadno);
  }

  for (i = 0; i < nWorkerThreads; ++i){
    workerInitSync.wait(INFINITE);
  }

  if (!wait) return 0;
  pthread_join(ServerThr, 0);
  SLauncher->wait();

  return -1;
}

void TCPDatagramCommunication::exitThreads(){
  int i;
  int nthreads;
  int threadno;
  TaskMsg msg;
  // send message asking scheduler of worker threads to exit
  nthreads = gContext.getNThreads(TCLASS_WORKER);
  for (i=0; i < nthreads; ++i){
    threadno = gContext.getThread(TCLASS_WORKER, i);
    msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_EXIT);
    msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
    tgetTaskScheduler()->sendMessage(msg);
  }
  ForceEndThreads = true; // causes receive thread and thread that accepts
                          // connections to exit
}

