//
// test-sql-simple.cpp
//
// Issues a simple SQL query to Yesquel.
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

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <assert.h>
#include <stdarg.h>
#include <values.h>
#include <stdint.h>

#include "sqlite3.h"

void printres(int res)
{
  switch(res){
  case SQLITE_BUSY: printf(" busy\n"); break;
  case SQLITE_DONE: printf(" done\n"); break;
  case SQLITE_ROW: printf(" row\n"); break;
  case SQLITE_ERROR: printf(" error\n"); break;
  case SQLITE_MISUSE: printf(" misuse\n"); break;
  }
}

#define REPS 10000

int main(int argc, char **argv)
{
  sqlite3 *db;
  int res, i;
  sqlite3_stmt *stmt;

  res=sqlite3_open("TEST", &db); assert(res==0);

  char *s1 = "create table t1 (a integer primary key, b int);";
  printf("%s\n", s1);
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("Error creating table (table already exists?)\n");
    exit(1);
  }
  res=sqlite3_step(stmt);
  printres(res);
  sqlite3_finalize(stmt);

  s1 = "insert into t1 values (1,2);";
  printf("%s\n", s1);
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  printres(res);
  sqlite3_finalize(stmt);

  s1 = "select * from t1 where a=1;";
  printf("Executing '%s' %d times\n", s1, REPS);

  for (i=0; i < REPS; ++i){
    res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
    res=sqlite3_step(stmt);
    res=sqlite3_finalize(stmt); assert(res==0);
  }
  printf(" success\n");

  res=sqlite3_close(db); assert(res==0);
  return 0;
}
