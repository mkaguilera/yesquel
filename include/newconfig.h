//
// newconfig.h
//
// Data structures for configuration and functions to read
// configuration file, based on flex/bison parser
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

#ifndef _NEWCONFIG_H
#define _NEWCONFIG_H

#define DEFAULT_PORT 12121

#include <list>
#include "inttypes.h"
#include "ipmisc.h"
#include "datastruct.h"
#include "grpctcp.h"

using namespace std;

struct ServerHT {
  int id;
  IPPort ipport;
  ServerHT(int i, IPPort ipp){ id=i; ipport=ipp; }
  ServerHT(){}
  // stuff for HashTable
  ServerHT *prev, *next, *sprev, *snext;
  int GetKey(){ return id; }
  static unsigned HashKey(int i){ return (unsigned) i; }
  static int CompareKey(int i1, int i2){ if (i1<i2) return -1; else if (i1==i2) return 0; else return 1; }
};

#define SERVER_HASHTABLE_SIZE 64
#define SERVERCONFIG_HASHTABLE_SIZE 64
#define HOSTSCONFIG_HASHTABLE_SIZE 64

class HostConfig {
public:
  IPPort ipport;   // IP and port of host
  char *hostname;  // name of host
  int port;        // port number
  char *logfile;   // name of log file
  char *storedir;  // name of directory where objects are stored.
                   // Should end with '/'

  // HashTable stuff
  HostConfig *next, *prev, *snext, *sprev;
  IPPort *GetKeyPtr(){ return &ipport; }
  static unsigned HashKey(IPPort *i){
    return (unsigned)(*(u32*)i ^ *((u32*)i+1));
  }
  static int CompareKey(IPPort *i1, IPPort *i2){
    return memcmp((void*)i1, (void*)i2, sizeof(IPPort));
  }
};

class ConfigState {
private:
  int nerrors;
  list<int> errRepeatedGroups;
  list<pair<IPPort,char*>> errRepeatedIPPort;
  list<int> errRepeatedServer;
  
public:
  HashTableBK<IPPort,HostConfig> Hosts;
  HashTable<int,ServerHT> Servers;
  int Nservers;     // number of servers
  
  unsigned PreferredIP;
  unsigned PreferredIPMask;
  int Ngroups;
  int StripeMethod; // method used for striping
  int StripeParm;   // parameter for method used for striping
  
  void addHost(HostConfig *toadd);
  void addServer(int server, char *hostname, int port, u32 preferip,
                 u32 prefermask);
  
  void setNgroups(int ngroups){ Ngroups = ngroups; }
  void setStripeMethod(int value){ StripeMethod = value; }
  void setStripeParm(int value){ StripeParm = value; }
  void setPreferredIP(char *ip);
  void setPreferredIPMask(char *ip);
  int check(void); // checks for configuration problems. Print errors on stderr
                   // Return 0 for ok, 1 for error
  int connectHosts(Ptr<RPCTcp> rpcc); // call clientconnect() on all hosts
  int disconnectHosts(Ptr<RPCTcp> rpcc); // call clientdisconnect() on all hosts
  ConfigState() : Hosts(HOSTSCONFIG_HASHTABLE_SIZE),
                  Servers(SERVERCONFIG_HASHTABLE_SIZE)
  {
    StripeMethod = StripeParm = Nservers = -1;
    PreferredIP = 0;
    PreferredIPMask = 0;
    nerrors = 0;
  }

  static ConfigState *ParseConfig(const char *configfilename);
}; 

extern ConfigState *parser_cs;

#endif
