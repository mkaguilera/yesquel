//
// newconfig.cpp
//
// Data structures for configuration and functions to read
// configuration file, based on flex/bison parser.
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
#include "newconfig.h"
#include "util.h"
#include "ipmisc.h"
#include "grpctcp.h"

void ConfigState::addServer(int server, char *hostname, int port, u32 preferip,
                            u32 prefermask){
  u32 chosenip;
  IPPort ipport;

  if (Servers.lookup(server)){ // repeated server declaration
    errRepeatedServer.push_front(server);
    ++nerrors;
  } else {
    chosenip = IPMisc::resolveName(hostname, preferip, prefermask);
    if (!chosenip)
      fprintf(stderr, "Config error: cannot resolve '%s' for server %d\n",
              hostname, server);
    else {
      ipport.set(chosenip, htons(port));
      Servers.insert(new ServerHT(server, ipport));
    }
  }
}

void ConfigState::addHost(HostConfig *toadd)
{
  IPPort ipport;
  u32 ip;

  ip = IPMisc::resolveName(toadd->hostname, PreferredIP, PreferredIPMask);
  if (!ip) return; // hostname cannot be resolved
  ipport.set(ip, htons(toadd->port));
  toadd->ipport = ipport;
  if (toadd->port == 0) toadd->port = DEFAULT_PORT;

  if (Hosts.lookup(&ipport) != 0){
    // repeated ip-port pair
    errRepeatedIPPort.push_front(pair<IPPort,char*>(ipport, toadd->hostname));
    ++nerrors;
  }
  else Hosts.insert(toadd);
}

void ConfigState::setPreferredIP(char *ip){
  int b1, b2, b3, b4;
  sscanf(ip, "%d.%d.%d.%d", &b1, &b2, &b3, &b4);
  PreferredIP = b1 + (b2 << 8) + (b3 << 16)+ (b4 << 24);
}

void ConfigState::setPreferredIPMask(char *ip){
  int b1, b2, b3, b4;
  sscanf(ip, "%d.%d.%d.%d", &b1, &b2, &b3, &b4);
  PreferredIPMask = b1 + (b2 << 8) + (b3 << 16)+ (b4 << 24);
}

int ConfigState::check(void){
  int retval;
  retval=0;

  if (StripeMethod < 0){
    fprintf(stderr, "Config error: missing stripe_method indication\n");
    ++retval;
  }
  if (StripeParm < 0){
    fprintf(stderr, "Config error: missing stripe_parm indication\n");
    ++retval;
  }
  if (Nservers < 0){
    fprintf(stderr, "Config error: missing nservers indication\n");
    ++retval;
  }

  // check for repeated host definitions
  for (list<pair<IPPort,char*>>::iterator it = errRepeatedIPPort.begin();
       it != errRepeatedIPPort.end();
       ++it)
  {
    fprintf(stderr,
         "Config error: repeated host-port entry for host %s ip %x port %d\n",
            it->second, it->first.ip, it->first.port);
    ++retval;
  }

  // check for repeated server definitions
  for (list<int>::iterator it = errRepeatedServer.begin();
       it != errRepeatedServer.end();
       ++it)
  {
    fprintf(stderr, "Config error: repeated server entry for server %d\n", *it);
    ++retval;
  }
  

  // check that all servers have been defined
  int server;
  for (server=0; server < Nservers; ++server){
    if (Servers.lookup(server) == 0){
      fprintf(stderr, "Config error: missing information for server %d\n",
              server);
      ++retval;
    }
  }
  return retval;
}

ConfigState *ConfigState::ParseConfig(const char *configfilename){
  extern FILE *yyin;
  extern int yyparse(void);
  FILE *f;
  int res;
  ConfigState *CS;

  CS = new ConfigState();

  // read and parse configuration
  f = fopen(configfilename, "r");
  if (!f){ 
    fprintf(stderr, "Config error: cannot open file %s\n", configfilename);
    return 0;
  }
  yyin = f;
  parser_cs = CS;
  res = yyparse();
  fclose(f);
  if (res || CS->check()){
    fprintf(stderr, "Config error: problems reading configuration file %s\n",
            configfilename);
    return 0;
  }
  return CS;
}

// call clientconnect() on all hosts
int ConfigState::connectHosts(Ptr<RPCTcp> rpcc){
  int res;
  HostConfig *hc;
  int err;

  err=0;
  //HashTableBK<IPPort,HostConfig> Hosts;
  for (hc = Hosts.getFirst(); hc != Hosts.getLast(); hc = Hosts.getNext(hc)){
    res=rpcc->clientconnect(hc->ipport);
    if (res){
      err=1;
      printf("Config error: cannot connect to server at %s\n",
             IPMisc::ipToStr(hc->ipport.ip)); 
    }
  }
  return err;
}

int ConfigState::disconnectHosts(Ptr<RPCTcp> rpcc){
  int res;
  HostConfig *hc;
  int err;

  err=0;
  //HashTableBK<IPPort,HostConfig> Hosts;
  for (hc = Hosts.getFirst(); hc != Hosts.getLast(); hc = Hosts.getNext(hc)){
    res=rpcc->clientdisconnect(hc->ipport);
    if (res){
      err=1;
      printf("Cannot disconnect server at %08x\n", hc->ipport.ip); 
    }
  }
  return err;
}
