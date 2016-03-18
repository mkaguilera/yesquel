//
// clientdir.h
//
// Directory for clients to find servers, based on a configuration file
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

#ifndef _CLIENTDIR_H
#define _CLIENTDIR_H

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

#include "options.h"
#include "os.h"
#include "gaiatypes.h"
#include "newconfig.h"

#include "tcpdatagram.h"
#include "grpctcp.h"

#include "datastruct.h"
#include "ccache.h"

struct ServerInfo;

// maps object id's to servers
class ObjectDirectory {
private:
  ConfigState *Config;

public:
  // get server IPPort and server number (optionally) of a given object id,
  // for the local site.
  // Returns IPPort. If retserverno!=0, sets *retserverno to server number.
  void GetServerId(const COid& coid, IPPortServerno &ipps);
  //void GetServerId(const COid& coid, IPPort &ipport);

  ObjectDirectory(ConfigState *cs){ Config = cs; }
};

// stores a storage configuration, indicating names of storage servers, etc
// It also includes the ObjectDirectory and the RPCTcp class to communicate with servers
class StorageConfig {
private:
  // aux function: callback for shutdown rpc
  static void pingCallback(char *data, int len, void *callbackdata);
  // aux function: callback for getstatus rpc
  static void getStatusCallback(char *data, int len, void *callbackdata);
  // aux function: callback for shutdown rpc
  static void shutdownCallback(char *data, int len, void *callbackdata);
  static void startsplitterServersCallback(char *data, int len,
                                           void *callbackdata);
  static void flushServersCallback(char *data, int len, void *callbackdata);
  static void loadServersCallback(char *data, int len, void *callbackdata);

public:
  ConfigState *CS;
  ObjectDirectory *Od;
  Ptr<RPCTcp> Rpcc;
  ClientCache *CCache;

  // ping and wait for response once to each server (eg, to make sure
  // they are all up)
  void pingServers(void);

  void getStatusServers(void);

  // the functions below invoke various RPCs on all servers to ask them
  // to do various things
  void shutdownServers(int level);  // shutdown servers (0=splitter only,
                                    // 1=fullserver)
  void startsplitterServers(void);  // start splitter
  void flushServers(char *filename=0); // flush storage contents to a given
                                       // filename or the default filename
  void loadServers(char *filename=0);  // load storage contents from a given
                                        // filename or the default filename

  StorageConfig(const char *configfile); // this constructor builds the RPCTcp
         // object. Intended to be used at the client
                 
  StorageConfig(const char *configfile, Ptr<RPCTcp> rpcc); // this constructor
     // uses a given RPCTcp object. Intended to be used at the server (who
     // wishes to make RPC calls to other servers)
  ~StorageConfig(){
    if (CS && Rpcc.isset()) CS->disconnectHosts(Rpcc); // disconnect clients
    if (Od){ delete Od; Od=0; }
    if (CS){ delete CS; CS=0; }
  }
};

#endif
