//
// main.cpp
//
// This is the main() for the storage server.
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
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <ctype.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>

#include <map>
#include <list>
#include <set>

//#define VALG_LEAK   // if set, include console command to perform
                       // valgrind leak check
#define DEBUG_LEVEL_DEFAULT 0
#define DEBUG_LEVEL_WHEN_LOGFILE 2 // debug level when -g option (use logfile)
                                   // is chosen

//#define PIDFILE "/tmp/yesquel-storageserver.pid"

#ifdef VALG_LEAK
#include <valgrind/memcheck.h>
#endif

#include "tmalloc.h"
#include "os.h"
#include "options.h"
#include "options.h"
#include "debug.h"

#include "tcpdatagram.h"
#include "grpctcp.h"

#include "task.h"

#include "gaiarpcaux.h"
#include "warning.h"

#include "storageserver.h"
#include "storageserver-rpc.h"
#include "storageserverstate.h"
#include "kvinterface.h"

#include "storageserver-splitter.h"
#include "splitter-client.h"
#include "clientdir.h"
extern StorageConfig *SC;

#include "util-more.h"

extern StorageServerState *S; // defined in storageserver.c

char *Configfile=0;


RPCProc RPCProcs[] = {  nullRpcStub,         // RPC 0
                        getstatusRpcStub,    // RPC 1
                        writeRpcStub,        // RPC 2
                        readRpcStub,         // RPC 3
                        fullwriteRpcStub,    // RPC 4
                        fullreadRpcStub,     // RPC 5
                        listaddRpcStub,      // RPC 6
                        listdelrangeRpcStub, // RPC 7
                        attrsetRpcStub,      // RPC 8
                        prepareRpcStub,      // RPC 9
                        commitRpcStub,       // RPC 10
                        subtransRpcStub,     // RPC 11
                        shutdownRpcStub,     // RPC 12
                        startsplitterRpcStub,// RPC 13
                        flushfileRpcStub,    // RPC 14
                        loadfileRpcStub      // RPC 15

#ifdef STORAGESERVER_SPLITTER
                        ,
                        ss_getrowidRpcStub   // RPC 16
#endif
                     };
  
struct ConsoleCmdMap {
  const char *cmd;
  const char *helpmsg;
  int  (*func)(char *parm, StorageServerState *sss);
};

int cmd_help(char *parm, StorageServerState *sss);
int cmd_flush(char *parm, StorageServerState *sss);
int cmd_load(char *parm, StorageServerState *sss);
int cmd_flushfile(char *parm, StorageServerState *sss);
int cmd_loadfile(char *parm, StorageServerState *sss);
int cmd_print(char *parm, StorageServerState *sss);
int cmd_print_detail(char *parm, StorageServerState *sss);
int cmd_splitter(char *parm, StorageServerState *sss);
int cmd_quit(char *parm, StorageServerState *sss);
int cmd_debug(char *parm, StorageServerState *sss);
int cmd_leakcheck(char *parm, StorageServerState *sss);


ConsoleCmdMap ConsoleCmds[] = {
  {"debug", " n:         set debug level to n", cmd_debug},
  {"help", ":            show this message", cmd_help},
  {"load_individual", ": load contents from disk", cmd_load},
  {"load", " filename:   load contents from file", cmd_loadfile},
  {"print", ":           print contents of storage", cmd_print},
  {"printdetail", ":     print contents of storage in detail",cmd_print_detail},
  {"save_individual", ": flush contents to disk", cmd_flush},
  {"save", " filename:   flush contents to file", cmd_flushfile}, 
  {"splitter", ":        start splitter", cmd_splitter},
  {"quit", ":            quit server", cmd_quit},
#ifdef VALG_LEAK
  {"vchk", ":            run valgrind's leak check", cmd_leakcheck},
#endif  
};

#define NConsoleCmds (sizeof(ConsoleCmds)/sizeof(ConsoleCmdMap))

int cmd_help(char *parm, StorageServerState *S){
  int i;
  putchar('\n');
  for (i = 0; i < (int)NConsoleCmds; ++i){
    printf("%s%s\n", ConsoleCmds[i].cmd, ConsoleCmds[i].helpmsg);
  }
  putchar('\n');
  return 0;
}

int cmd_load(char *parm, StorageServerState *S){
  printf("Loading from disk...");
  S->cLogInMemory.loadFromDisk();
  printf(" Done!\n");
  return 0;
}

int cmd_flush(char *parm, StorageServerState *S){
  Timestamp ts;
  ts.setNew();
  mssleep(1000); // wait for a second

  printf("Warning: using \"flush\" will make the contents visible "
         "automatically to future runs of storageserver\n");
  printf("This is not true for \"flush2\"\n");

  printf("Flushing to disk...");
  S->cLogInMemory.flushToDisk(ts);
  printf(" Done!\n");
  
  return 0;
}

int cmd_loadfile(char *parm, StorageServerState *S){
  int res;
  if (!parm || strlen(parm)==0){
    printf("Missing filename\n");
    return 0;
  }
  printf("Loading from file...");
  res = S->cLogInMemory.loadFromFile(parm);
  if (res) printf("Error. Probably file does not exist\n");
  else printf(" Done!\n");
  return 0;
}

int cmd_flushfile(char *parm, StorageServerState *S){
  if (!parm || strlen(parm)==0){
    printf("Missing filename\n");
    return 0;
  }
  Timestamp ts;
  int res;
  ts.setNew();
  mssleep(1000); // wait for a second

  printf("Flushing to file...");
  res = S->cLogInMemory.flushToFile(ts, parm);
  if (res) printf("Error. Cannot write file for some reason\n");
  else printf(" Done!\n");
  
  return 0;
}

int cmd_print(char *parm, StorageServerState *S){
  S->cLogInMemory.printAllLooim();
  return 0;
}

int cmd_print_detail(char *parm, StorageServerState *S){
  S->cLogInMemory.printAllLooimDetailed();
  return 0;
}

int startSplitter(void);
int cmd_splitter(char *parm, StorageServerState *S){
#ifdef STORAGESERVER_SPLITTER
  if (!SC){
    printf("Starting splitter...\n");
    startSplitter();
    //SC = new StorageConfig((const char*) Configfile);
    //printf("Done\n");
  } else printf("Splitter already running\n");
#else
  printf("This storageserver does not have a splitter\n");
#endif

  return 0;
}

int cmd_quit(char *parm, StorageServerState *S){
  return 1;
}

int cmd_debug(char *parm, StorageServerState *S){
  if (parm) SetDebugLevel(atoi(parm));
  else printf("Debug requires a numerical parameter\n");
  return 0;
}

int cmd_leakcheck(char *parm, StorageServerState *S){
#ifdef VALG_LEAK
  VALGRIND_DO_ADDED_LEAK_CHECK;
#endif  
  return 0;
}

// convert str to lower case
void strlower(char *str){
  while (*str){
    *str = tolower(*str);
    ++str;
  }
}

void console(void){
  char line[256];
  char *cmd;
  char *parm;
  int i;
  int done=0;
  while (!done && !feof(stdin)){
    fgets(line, sizeof(line), stdin);
    line[sizeof(line)-1]=0;
    cmd = strtok(line, " \t\n");
    if (!cmd || !*cmd) continue;
    strlower(cmd);
    
    parm = strtok(0, " \t\n");
    for (i = 0; i < (int)NConsoleCmds; ++i){
      if (strcmp(cmd, ConsoleCmds[i].cmd)==0){
        done = ConsoleCmds[i].func(parm, S);
        break;
      }
    }
    if (i == NConsoleCmds)
      printf("Unrecognized command %s. Try \"help\".\n", cmd); 
  }
}

void printsizes(void){
  printf("LogOneObjectInMemory %d\n", (int)sizeof(LogOneObjectInMemory));
  printf("SingleLogEntryInMemory %d\n", (int)sizeof(SingleLogEntryInMemory));
  printf("LogInMemory %d\n", (int)sizeof(LogInMemory));
  printf("WriteRPCParm %d\n", (int)sizeof(WriteRPCParm));
  printf("WriteRPCData %d\n", (int)sizeof(WriteRPCData));
  printf("WriteRPCRespData %d\n", (int)sizeof(WriteRPCRespData));
  printf("ReadRPCParm %d\n", (int)sizeof(ReadRPCParm));
  printf("ReadRPCData %d\n", (int)sizeof(ReadRPCData));
  printf("ReadRPCRespData %d\n", (int)sizeof(ReadRPCRespData));
  printf("PrepareRPCParm %d\n", (int)sizeof(PrepareRPCParm));
  printf("PrepareRPCData %d\n", (int)sizeof(PrepareRPCData));
  printf("TxWriteItem %d\n", (int)sizeof(TxWriteItem));
  printf("TxReadItem %d\n", (int)sizeof(TxReadItem));
  printf("PendingTxInfo %d\n", (int)sizeof(PendingTxInfo));
  printf("PendingTx %d\n", (int)sizeof(PendingTx));
  printf("OutstandingRPC %d\n", (int)sizeof(OutstandingRPC));
  //printf("RPCCachedResult %d\n", (int)sizeof(RPCCachedResult));
}

#ifdef NDEBUG
#define COMPILECONFIG "Production"
#else
#define COMPILECONFIG "Debug"
#endif


class RPCServerGaia : public RPCTcp {
protected:
  void startupWorkerThread(){
    RPCTcp::startupWorkerThread();
#ifdef STORAGESERVER_SPLITTER
    initServerTask(tgetTaskScheduler());
#endif
  }
  
public:
  RPCServerGaia(RPCProc *procs, int nprocs, int portno) : RPCTcp() {
    launch(SERVER_WORKERTHREADS);
    registerNewServer(procs, nprocs, portno);
  }
};

Ptr<RPCTcp> RPCServer;

// Start splitter within server. Read config file, connect to other servers.
// Returns 0 if ok, !=0 if splitter already started

int startSplitter(void){
  if (SC) return -1;
  SC = new StorageConfig((const char*) Configfile, RPCServer);
  KVInterfaceInit();
  return 0;
}

int main(int argc, char **argv)
{
  u32 myip;
  u16 myport;
  IPPort myipport;
  int badargs, c;
  int useconsole=0;
  int loadfile=0;
  int uselogfile=0;
  int setdebug=0;
  int skipsplitter=0;
  char *loadfilename=0;
  char *logfilename=0;

  badargs=0;
  while ((c = getopt(argc,argv, "cd:g:l:o:s")) != -1){
    switch(c){
    case 'c':
      useconsole = 1;
      break;
    case 'd':
      setdebug = 1;
      SetDebugLevel(atoi(optarg));
      break;
    case 'g':
      uselogfile = 1;
      logfilename = (char*) malloc(strlen(optarg)+1);
      strcpy(logfilename, optarg);
      break;
    case 'l':
      loadfile = 1;
      loadfilename = (char*) malloc(strlen(optarg)+1);
      strcpy(loadfilename, optarg);
      break;
    case 'o':
      Configfile = (char*) malloc(strlen(optarg)+1);
      strcpy(Configfile, optarg);
      break;
    case 's':
      skipsplitter = 1;
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments
  argc -= optind;

  tinitScheduler(0);

  // parse arguments
  switch(argc){
  case 0:
    myport = SERVER_DEFAULT_PORT; // use default port
    break;
  case 1:
    myport = atoi(argv[optind]);
    break;
  default:
    fprintf(stderr, "usage: %s [-cgs] [-d debuglevel] [-l filename] "
                        "[-o configfile] [-g logfile] [portno]\n", argv[0]);
    fprintf(stderr, "   -c  enable console\n");
    fprintf(stderr, "   -d  set debuglevel to given value\n");
    fprintf(stderr, "   -g  use log file\n");
    fprintf(stderr, "   -l  load state from given file\n");
    fprintf(stderr, "   -o  use given configuration file\n");
    fprintf(stderr, "   -s  do not start splitter (storageserver-splitter version)\n");
    fprintf(stderr, "       This is useful with more than one server, in which case it may be better\n");
    fprintf(stderr, "       to start the splitter remotely after all servers have started already,\n");
    fprintf(stderr, "       otherwise the servers that start first may not be able to start their\n");
    fprintf(stderr, "       splitters since they cannot communicate with the other servers\n");
    exit(1);
  }

  if (!Configfile){
    Configfile = getenv(GAIACONFIG_ENV);
    if (!Configfile) Configfile = GAIA_DEFAULT_CONFIG_FILENAME;
  }
  printf("Config file is %s\n", Configfile);

  // What follows is code for handling the config file
  ConfigState *cs;

  cs = ConfigState::ParseConfig(Configfile);
  if (!cs) exit(1); // cannot read configuration file

  myip = IPMisc::getMyIP(parser_cs->PreferredIP, parser_cs->PreferredIPMask);
  myipport.set(myip, htons(myport));

  // try to find ourselves in host list of configuration file
  HostConfig *hc;
  hc = cs->Hosts.lookup(&myipport);
  if (!hc){
    myip = htonl(0x7f000001); // try 127.0.0.1 address
    myipport.set(myip, htons(myport));
    hc = cs->Hosts.lookup(&myipport);
    if (!hc){
      fprintf(stderr, "Cannot find my IP %s and port %d in config file\n",
        IPMisc::ipToStr(myipport.ip), ntohs(myipport.port));
      exit(1);
    }
  }

  UniqueId::init(myip);

  
  printf("Compilation time %s %s configuration %s\n",
         __DATE__, __TIME__, COMPILECONFIG);
  printf("Configuration file %s debuglog %s\n", Configfile,
         uselogfile ? "yes" : "no");
  printf("Host %s IP %s port %d log %s store %s\n", hc->hostname,
         IPMisc::ipToStr(myip), hc->port, hc->logfile, hc->storedir);
  printf("Server_workers %d\n", SERVER_WORKERTHREADS);

  // debugging information stuff
#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
  if (!setdebug){ // if setdebug, override defaults
    if (!uselogfile)  SetDebugLevel(DEBUG_LEVEL_DEFAULT);
    else SetDebugLevel(DEBUG_LEVEL_WHEN_LOGFILE);
  }

  if (!uselogfile) DebugInit(false);
  else DebugInit(true, logfilename);

#endif

  initStorageServer(hc);
  int myrealport = hc->port; assert(myrealport != 0);

  RPCServer = new RPCServerGaia(RPCProcs, sizeof(RPCProcs)/sizeof(RPCProc),
                                myrealport);

  if (loadfile){
    printf("Load state from file %s...", loadfilename); fflush(stdout);
    S->cLogInMemory.loadFromFile(loadfilename);
    putchar('\n'); fflush(stdout);
  }

  //RPCServer.launch(0); 
  mssleep(1000);

#if defined(STORAGESERVER_SPLITTER) && DTREE_SPLIT_LOCATION >= 2
  if (!skipsplitter){
    startSplitter();
  }
#endif

#ifdef PIDFILE
  int pidfd = open(PIDFILE, O_CREAT | O_TRUNC | O_WRONLY, 0644);
  char pidstr[100];
  if (pidfd < 0){
    printf("Cannot write pid file %s errno %d\n", PIDFILE, errno);
    exit(1);
  }
  sprintf(pidstr, "%d", getpid());
  write(pidfd, pidstr, strlen(pidstr)+1);
#endif

  if (useconsole){
    console();
  } else {
    RPCServer->waitServerEnd(); // should never return
  }
#if defined(STORAGESERVER_SPLITTER) && !defined(LOCALSTORAGE)

  if (SC){
    delete SC;   // closes splitter connections
    SC = 0;
  }
#endif
  
  RPCServer->exitThreads();
  mssleep(500);

  RPCServer = 0;

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
  DebugUninit();
#endif
}
