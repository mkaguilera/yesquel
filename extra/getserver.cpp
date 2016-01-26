//
// getserver.cpp
//
// Returns the ip and port of the server responsible for a given coid.
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

#include "clientdir.h"
#include "newconfig.h"

int main(int argc, char **argv){
  u64 cid, oid;
  COid coid;
  IPPortServerno server;

  tinitScheduler(0);
  
  if (argc != 4){
    fprintf(stderr, "usage %s configfile cid oid\n", argv[0]);
    fprintf(stderr, "  where cid and oid are in hex\n");
    exit(1);
  }

  sscanf(argv[2], "%lx", &cid);
  sscanf(argv[3], "%lx", &oid);

  coid.cid = cid;
  coid.oid = oid;

  ConfigState *cs = ConfigState::ParseConfig(argv[1]);
  
  ObjectDirectory od(cs);

  od.GetServerId(coid, server);

  printf("coid %016lx:%016lx serverno %d ip %08x port %d\n", coid.cid, coid.oid, server.serverno, server.ipport.ip, server.ipport.port);
  exit(0);
}
