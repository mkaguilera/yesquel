//
// shelldt.cpp
//
// An interactive shell to insert, delete, and scan data on a distributed
// B-tree.
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
#include <stdlib.h>
#include <assert.h>
#include <stdarg.h>
#include <string.h>
#include <malloc.h>
#include <time.h>

#include "options.h"
#include "tmalloc.h"
#include "os.h"
#include "treedirect.h"

//#define STRESSTEST

DdConnection *conn;
DdTable *table;

struct ConsoleCmdMap {
  char *cmd;
  char *helpmsg;
  int  (*func)(char *parm);
};

int cmd_help(char *parm);
int cmd_debug(char *parm);
int cmd_quit(char *parm);

int cmd_insert(char *parm);
int cmd_delete(char *parm);
int cmd_lookup(char *parm);
int cmd_scan(char *parm);

ConsoleCmdMap ConsoleCmds[] = {
  {"debug", " n: set debug level to n", cmd_debug},
  {"d", " k:     delete key k", cmd_delete}, 
  {"help", ":    show this message", cmd_help},
  {"i", " k:     insert key k value Dk", cmd_insert},
  {"l", " k:     lookup key k", cmd_lookup},
  {"s", " k:     scan key k for 20 keys", cmd_scan},
  {"quit", ":    quit server", cmd_quit},
};

#define NConsoleCmds (sizeof(ConsoleCmds)/sizeof(ConsoleCmdMap))

int cmd_help(char *parm){
  int i;
  putchar('\n');
  for (i = 0; i < (int)NConsoleCmds; ++i){
    printf("%s%s\n", ConsoleCmds[i].cmd, ConsoleCmds[i].helpmsg);
  }
  putchar('\n');
  return 0;
}
int cmd_quit(char *parm){
  return 1;
}

int cmd_debug(char *parm){
  if (parm) SetDebugLevel(atoi(parm));
  else printf("Debug requires a numerical parameter\n");
  return 0;
}

// convert str to lower case
void strlower(char *str){
  while (*str){
    *str = tolower(*str);
    ++str;
  }
}

int cmd_insert(char *parm){
  i64 key;
  int res;
  char str[256];
  if (!parm){ printf("Insert requires a numerical parameter\n"); return 0; }
  key = (i64) atoll(parm);
  sprintf(str, "D%lld", (long long)key+1000);
  DdStartTx(conn);
  res = DdInsert(table, key, str, (int)strlen(str)+1);
  if (res){ printf("Insert failed: %d\n", res); }
  res = DdCommitTx(conn);
  if (res){ printf("Insert commit failed: %d\n", res); }
  return 0;
}


int cmd_delete(char *parm){
  i64 key;
  int res;

  if (!parm){ printf("Delete requires a numerical parameter\n"); return 0; }
  key = (i64) atoll(parm);
  DdStartTx(conn);
  res = DdDelete(table, key);
  if (res){ printf("Delete failed: %d\n", res); }
  res = DdCommitTx(conn);
  if (res){ printf("Delete commit failed: %d\n", res); }
  return 0;
}

int cmd_lookup(char *parm){
  char str[256];
  i64 key;
  int valuelen;
  int res;

  if (!parm){ printf("Lookup requires a numerical parameter\n"); return 0; }
  key = (i64) atoll(parm);
  DdStartTx(conn);
  res = DdLookup(table, key, str, sizeof(str)-1, &valuelen);
  if (res){ printf("Lookup failed: %d\n", res); return 0; }
  res = DdCommitTx(conn);
  if (res){ printf("Lookup commit failed: %d\n", res); }
  if (valuelen==0){ printf("Not found\n"); return 0; }
  str[valuelen] = 0;
  printf("%s\n", str);
  return 0;
}

void scancallback(i64 key, const char *data, int len, int i, bool eof, void *parm){
  char *ptr = (char*) data;
  if (eof){ printf("eof\n"); return; }
  ptr[len] = 0;
  printf("key %llx sequence %d len %d data %s\n", (long long)key, i, len, data);
}

int cmd_scan(char *parm){
  i64 key;
  int res;

  if (!parm){ printf("Scan requires a numerical parameter\n"); return 0; }
  key = (i64)atoll(parm);
  DdStartTx(conn);
  res = DdScan(table, key, 20, scancallback, 0); 
  if (res){ printf("Scan failed: %d\n", res); return 0; }
  res = DdCommitTx(conn);
  if (res){ printf("Scan commit failed: %d\n", res); }
  return 0;
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
        done = ConsoleCmds[i].func(parm);
        break;
      }
    }
    if (i == (int)NConsoleCmds) printf("Unrecognized command %s. Try \"help\".\n", cmd); 
  }
}

#ifndef STRESSTEST

int main(int argc, char **argv)
{
  char *dbname;
  int badargs=0;
  u64 itable;
  int res;
  int OptCreate = 0;
  int c;

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  badargs=0;
  while ((c = getopt(argc,argv, "c")) != -1){
    switch(c){
    case 'c':
      OptCreate = 1;
      break;
    default:
      ++badargs;
    }
  }
  argc = argc - optind;
  
  if (argc != 2 || badargs){
    fprintf(stderr, "usage: %s [-c] dbname containerid  (containerid in hex)\n", argv[0]);
    fprintf(stderr, "  -c create the table\n");
    exit(1);
  }

  dbname = argv[optind];
  sscanf(argv[optind+1], "%llx", (long long*)&itable);

  DdInit();
  res = DdInitConnection(dbname, conn);
  if (res){ fprintf(stderr, "Error connecting to %s: %d\n", dbname, res); exit(1); }

  if (OptCreate) res = DdCreateTable(conn, itable, table);
  else res = DdOpenTable(conn, itable, table);
  if (res){ fprintf(stderr, "Error %s table %llx: %d\n", OptCreate ? "creating" : "opening", (long long)itable, res); exit(1); }

  console();

  DdCloseTable(table);
  DdCloseConnection(conn);
  DdUninit();

  exit(0);
}

#else

#include "shelldt-rnd.h"

#define NTHREADS 2

int dowork(int id){
  int v, res;
  RandomUnique ru(100,20000);
  Prng p;
  char buf[3];
  memcpy(buf, "Ok", 3);
  int i;

  // phase 1
  for (i=0; i < 1500; ++i){
    if ((unsigned) p.next() % 3 == 0){ // with probability 1/3, remove
      v = ru.removeRandom();
      if (v==-1) continue;
      v = v * NTHREADS + id;
      res = do_delete(v);
      if (res){ putchar('R'); fflush(stdout); }
      else if (i%100==0) { putchar(','); fflush(stdout); }
    } else {  // with probability 2/3, add
      v = ru.newRandom();
      if (v==-1) continue;
      v = v * NTHREADS + id;
      res = do_insert(v, buf, sizeof(buf));
      if (res){ putchar('A'); fflush(stdout); }
      else if (i%100==0) { putchar('.'); fflush(stdout); }
    }
  }

  // phase 2
  for (i=0; i < 1500; ++i){
    if ((unsigned) p.next() % 3 > 0){ // with probability 2/3, remove
      v = ru.removeRandom();
      if (v==-1) continue;
      v = v * NTHREADS + id;
      res = do_delete(v);
      if (res){ putchar('R'); fflush(stdout); }
      else if (i%100==0){ putchar(','); fflush(stdout); }
    } else { // with probability 1/3, add
      v = ru.newRandom();
      if (v==-1) continue;
      v = v * NTHREADS + id;
      res = do_insert(v, buf, sizeof(buf));
      if (res){ putchar('A'); fflush(stdout); }
      else if (i%100==0) { putchar('.'); fflush(stdout); }
    }
  }
  putchar('\n');
  return 0;
}

int main(int argc, char **argv)
{
  char *argv0;
  int badargs=0;
  int index;
  //int c;

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  // remove path from argv[0]
  argv0 = strrchr(argv[0], '\\');
  if (!argv0) argv0 = argv[0];
  else ++argv0;

  //badargs=0;
  //while ((c = getopt(argc,argv, "r")) != -1){
  //  switch(c){
  //  case 'r':
  //    OptShowReal = 1;
  //    break;
  //  default:
  //    ++badargs;
  //  }
  //}
  optind=1;
  argc = argc - optind;
  
  if (argc != 2 || badargs){
    fprintf(stderr, "usage: %s containerid index\n", argv0);
    fprintf(stderr, "      (containerid parameter in hex)\n");
    exit(1);
  }

  sscanf(argv[optind], "%llx", (long long*)&Rootdtree);
  index = atoi(argv[optind+1]);

  //RandomUnique ru(100,20000);
  //for (int i=0; i < 10000; ++i){
  //  printf("%d\n", ru.newRandom());
  //}

  //printf("\n");
  //for (int i=0; i < 10000; ++i){
  //  printf("%d\n", ru.removeRandom());
  //}

  SetDebugLevel(1);

  //InitGaia();
  InitDt();

  dowork(index);

  UninitDt();

  exit(0);
}

#endif
