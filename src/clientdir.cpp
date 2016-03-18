//
// clientdir.cpp
//
// Directory for clients to find servers, based on a configuration file

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
#include <list>
#include <map>
#include <set>

#include "tmalloc.h"
#include "debug.h"
#include "clientdir.h"
#include "gaiarpcaux.h"

StorageConfig::StorageConfig(const char *configfile) {
  Rpcc = new RPCTcp();
  Rpcc->launch(CLIENT_WORKERTHREADS);
  
  CS = ConfigState::ParseConfig(configfile);
  if (!CS) exit(1); // cannot read config file

  u32 myip = IPMisc::getMyIP(parser_cs->PreferredIP,
                             parser_cs->PreferredIPMask);
  UniqueId::init(myip);
  Rpcc->clientinit();

  CS->connectHosts(Rpcc);
  Od = new ObjectDirectory(CS);
#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  CCache = new ClientCache(CS->Nservers);
#else
  CCache = 0;
#endif
}

StorageConfig::StorageConfig(const char *configfile, Ptr<RPCTcp> rpcc) {
  Rpcc = rpcc;
  
  CS = ConfigState::ParseConfig(configfile);
  if (!CS) exit(1); // cannot read config file

  u32 myip = IPMisc::getMyIP(parser_cs->PreferredIP,
                             parser_cs->PreferredIPMask);
  UniqueId::init(myip);

  CS->connectHosts(Rpcc);
  Od = new ObjectDirectory(CS);
#ifdef GAIA_CLIENT_CONSISTENT_CACHE
  CCache = new ClientCache(CS->Nservers);
#else
  CCache = 0;
#endif
}

struct pingCallbackData {
  Semaphore *sem;
};

void StorageConfig::pingCallback(char *data, int len, void *callbackdata){
  NullRPCRespData resp;
  pingCallbackData *pcd = (pingCallbackData *) callbackdata;
  if (data){ // RPC got response
    dprintf(2, "Ping: got a response");
    resp.demarshall(data); // now resp->data has return results of RPC
  }
  pcd->sem->signal();
  delete pcd;
  return; // free return results of RPC
}

// ping and wait for response once to each server
// (eg, to make sure they are all up)
void StorageConfig::pingServers(void){
  NullRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;
  pingCallbackData *pcd;

  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast();
       hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Pinging server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new NullRPCData;
    parm->data = new NullRPCParm;
    parm->data->reserved = 0;
    parm->freedata = true;
    pcd = new pingCallbackData;
    pcd->sem = &sem;
    Rpcc->asyncRPC(hc->ipport, NULL_RPCNO, 0, parm, pingCallback, (void *) pcd);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

struct GetStatusCallbackData {
  Semaphore *sem;
  GetStatusRPCRespData resp; // this field gets filled by getStatusCallback
                             // with server response
};

void StorageConfig::getStatusCallback(char *data, int len, void *callbackdata){
  GetStatusRPCRespData resp;
  GetStatusCallbackData *gscd = (GetStatusCallbackData *) callbackdata;
  if (data){ // RPC got response
    dprintf(2, "Ping: got a response");
    resp.demarshall(data); // now resp->data has return results of RPC
    gscd->resp = resp;
  }
  gscd->sem->signal();
  return; // free return results of RPC
}


void StorageConfig::getStatusServers(void){
  GetStatusRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;
  GetStatusCallbackData *gscd;
  list<GetStatusCallbackData*> responses;

  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast();
       hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("GetStatus server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new GetStatusRPCData;
    parm->data = new GetStatusRPCParm;
    parm->data->reserved = 0;
    parm->freedata = true;
    gscd = new GetStatusCallbackData;
    gscd->sem = &sem;
    responses.push_back(gscd);
    Rpcc->asyncRPC(hc->ipport, GETSTATUS_RPCNO, 0, parm, getStatusCallback,
                   (void *) gscd);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
  for (list<GetStatusCallbackData*>::iterator it = responses.begin();
       it != responses.end(); ++it){
    gscd = *it;
    // **!** do something with gscd->resp
    delete gscd;
  }
}

struct ShutdownCallbackData {
  Semaphore *sem;
  HostConfig *hc;
  SimpleLinkList<HostConfig*> *toshutdown;
};

void StorageConfig::shutdownCallback(char *data, int len, void *callbackdata){
  ShutdownRPCRespData resp;
  ShutdownCallbackData *scd = (ShutdownCallbackData*) callbackdata;
  assert(scd);
  dprintf(2, "Shutdown: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  if (resp.data->status){ // failed, so add hc to toshutdown, to retry below
    printf("Retrying shutdown\n");
    scd->toshutdown->pushTail(scd->hc);
  }
  scd->sem->signal();
  delete scd;
  return;
}

void StorageConfig::shutdownServers(int level){
  ShutdownRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;

  SimpleLinkList<HostConfig*> toshutdown;
  ShutdownCallbackData *scd;
  
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast();
       hc = CS->Hosts.getNext(hc)){
    printf("Shutting down server %08x port %d\n", hc->ipport.ip,
           hc->ipport.port);
    toshutdown.pushTail(hc);
  }

  while (!toshutdown.empty()){
    // issue RPCs to each server in toshutdown
    count = 0;
    while (!toshutdown.empty()){
      hc = toshutdown.popHead();
      ++count;
      parm = new ShutdownRPCData;
      parm->data = new ShutdownRPCParm;
      parm->data->reserved = 0;
      parm->data->level = level;
      parm->freedata = true;
      scd = new ShutdownCallbackData; // data for callback
      scd->sem = &sem;
      scd->hc = hc;
      scd->toshutdown = &toshutdown;
      Rpcc->asyncRPC(hc->ipport, SHUTDOWN_RPCNO, 0, parm, shutdownCallback,
                     (void *) scd);
    }
    
    // wait to receive replies
    // callback will add server back to toshutdown if RPC fails
    for (i=0; i < count; ++i){
      // wait for responses for all issued RPCs
      sem.wait(INFINITE);
    }

    if (!toshutdown.empty()){ mssleep(1000); }
  }
  printf("Shutdown done\n");
}

//----------------------------------------------------------------------------
void StorageConfig::startsplitterServersCallback(char *data, int len,
                                                 void *callbackdata){
  StartSplitterRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Start splitter: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::startsplitterServers(void){
  StartSplitterRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast();
       hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Starting splitter on server %08x port %d\n", hc->ipport.ip,
           hc->ipport.port);
    parm = new StartSplitterRPCData;
    parm->data = new StartSplitterRPCParm;
    parm->data->reserved = 0;
    parm->freedata = true;
    Rpcc->asyncRPC(hc->ipport, STARTSPLITTER_RPCNO, 0, parm,
                   startsplitterServersCallback, (void *) &sem);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

void StorageConfig::flushServersCallback(char *data, int len,
                                         void *callbackdata){
  FlushFileRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Save server: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  if (resp.data->status != 0)
    printf("Got error from server\n");
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::flushServers(char *filename){
  FlushFileRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count, len;

  if (!filename) filename = (char*)"";
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast();
       hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Saving server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new FlushFileRPCData;
    parm->data = new FlushFileRPCParm;
    len = (int)strlen(filename);
    parm->data->filename = new char[len+1];
    strcpy(parm->data->filename, filename);
    parm->data->filenamelen = len+1;
    parm->freedata = true;
    parm->freefilenamebuf = parm->data->filename;

    Rpcc->asyncRPC(hc->ipport, FLUSHFILE_RPCNO, 0, parm,
                   flushServersCallback, (void *) &sem);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

void StorageConfig::loadServersCallback(char *data, int len,
                                        void *callbackdata){
  LoadFileRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Load server: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  if (resp.data->status != 0)
    printf("Got error from server\n");
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::loadServers(char *filename){
  LoadFileRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count, len;

  if (!filename) filename = (char*)"";
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast();
       hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Loading server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new LoadFileRPCData;
    parm->data = new LoadFileRPCParm;
    len = (int)strlen(filename);
    parm->data->filename = new char[len+1];
    strcpy(parm->data->filename, filename);
    parm->data->filenamelen = len+1;
    parm->freedata = true;
    parm->freefilenamebuf = parm->data->filename;
    Rpcc->asyncRPC(hc->ipport, LOADFILE_RPCNO, 0, parm, loadServersCallback,
                   (void *) &sem);
  }
  dprintf(1, "Waiting for responses");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

//-------------------------------------------------------------------------

static inline unsigned GetServerNumber(const COid &coid, int nservers,
                                       int method, int parm){
  switch(method){
  case 0:
    // parm is not used for method 0
    return (coid.oid & 0xffff)%nservers;
  default: assert(0);
  }
  return 0;
}

void ObjectDirectory::GetServerId(const COid& coid, IPPortServerno &ipps){
  unsigned serverno;

  // get server number
  serverno = GetServerNumber(coid, Config->Nservers, Config->StripeMethod,
                             Config->StripeParm);
  ipps.ipport = Config->Servers[serverno]->ipport;
  ipps.serverno = serverno;
}

// void ObjectDirectory::GetServerId(const COid& coid, IPPort &ipport){
//   unsigned serverno;

//   // get server number
//   serverno = GetServerNumber(coid, Config->Nservers, Config->StripeMethod,
//                              Config->StripeParm);
//   ipport = Config->Servers[serverno]->ipport;
// }

