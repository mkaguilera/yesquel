//
// callserver.cpp
//
// Invokes all storage servers (according to configuration file) to
// perform certain actions such as shutdown, start splitter, etc
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
#include <values.h>

#include <map>
#include <list>

#include "tmalloc.h"
#include "options.h"
#include "os.h"
#include "util.h"
#include "gaiatypes.h"
#include "clientdir.h"
#include "clientlib.h"

#ifdef DEBUG
extern int DebugLevel;
#endif

struct {
  char *cmdname;
  int cmdnumber;
} Cmds[] = {
  {"load",0},
  {"save",1},
  {"ping",2},
  {"shutdown-splitter", 3},
  {"shutdown",4},
  {"splitter",5},
  {0,-1} // to indicate end
};

int main(int argc, char **argv)
{
  char *Configfile=0;
  int badargs, c;
  char *command, *commandarg;
  int cmd=-1;
  int i;
  int debuglevel=MININT;

  tinitScheduler(-1);

  // remove path from argv[0]
  char *argv0 = strrchr(argv[0], '\\');
  if (argv0) ++argv0; else argv0 = argv[0];

  badargs=0;
  while ((c = getopt(argc,argv, "o:d:")) != -1){
    switch(c){
    case 'd':
      debuglevel = atoi(optarg);
      break;
    case 'o':
      Configfile = (char*) malloc(strlen(optarg)+1);
      strcpy(Configfile, optarg);
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments
  argc -= optind;

  // parse arguments
  switch(argc){
  case 1:
    command = argv[optind];
    commandarg = 0;
    break;
  case 2:
    command = argv[optind];
    commandarg = argv[optind+1];
    break;
  default:
    fprintf(stderr, "usage: %s [-o config] [-d debuglevel] command [parm]\n",
            argv0);
    fprintf(stderr, "existing commands:\n");
    fprintf(stderr, "  load [filename]\n");
    fprintf(stderr, "  save [filename]\n");
    fprintf(stderr, "  ping\n");
    fprintf(stderr, "  shutdown-splitter\n");
    fprintf(stderr, "  shutdown\n");
    fprintf(stderr, "  splitter\n");
    exit(1);
  }

  if (!Configfile){
    Configfile = getenv(GAIACONFIG_ENV);
    if (!Configfile) Configfile = GAIA_DEFAULT_CONFIG_FILENAME;
  }

  i=0;
  while (Cmds[i].cmdname){
    if (!strcmp(command, Cmds[i].cmdname)) break;
    ++i;
  }
  cmd = Cmds[i].cmdnumber;
  if (cmd == -1){
    printf("Invalid command %s\n", command);
    printf("Valid commands are the following:");
    i=0;
    while (Cmds[i].cmdname){
      printf(" %s", Cmds[i].cmdname);
      ++i;
    }
    putchar('\n');
    exit(1);
  }

  printf("Executing %s command", command);
  if (commandarg) printf(" with parameter %s", commandarg);
  putchar('\n');

  UniqueId::init();

  StorageConfig sc(Configfile);

#ifdef DEBUG
  if (debuglevel != MININT) DebugLevel = debuglevel;
#endif

  switch(cmd){
  case 0: // load
    sc.loadServers(commandarg);
    break;
  case 1: // save
    sc.flushServers(commandarg);
    break;
  case 2: // ping
    sc.pingServers();
    break;
  case 3: // shutdown splitter
    sc.shutdownServers(0);
    break;
  case 4: // shutdown
    sc.shutdownServers(1);
    break;
  case 5: // splitter
    sc.startsplitterServers();
    break;
  default: assert(0);
  }

  printf("Done\n");

  exit(0);
}
