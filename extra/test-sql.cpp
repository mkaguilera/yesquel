//
// test-sql.cpp
//
// SQL tests for Yesquel.
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
#include <unistd.h>

#include "os.h"
#include "gaiatypes.h"
#include "util.h"
#include "sqlite3.h"
#include "prng.h"

void printres(int res)
{
  switch(res){
  case SQLITE_BUSY: printf("  busy\n"); break;
  case SQLITE_DONE: printf("  done\n"); break;
  case SQLITE_ROW: printf("  row\n"); break;
  case SQLITE_ERROR: printf("  error\n"); break;
  case SQLITE_MISUSE: printf("  misuse\n"); break;
  }
}

#define TEST1_REPS 10000

// test1: basic test that creates a table, inserts a value, and
// queries for that value repeatedly.
void test1(){
  sqlite3 *db;
  int res, i;
  sqlite3_stmt *stmt;

  res=sqlite3_open("TEST1", &db); assert(res==0);

  const char *s1 = "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INT);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  if (res != SQLITE_DONE){
    printres(res);
    return;
  }
  sqlite3_finalize(stmt);

  s1 = "INSERT INTO t1 VALUES (1,2);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  if (res != SQLITE_DONE){
    printres(res);
    return;
  }
  sqlite3_finalize(stmt);

  s1 = "SELECT * FROM t1 WHERE a=1;";

  for (i=0; i < TEST1_REPS; ++i){
    res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
    res=sqlite3_step(stmt);
    res=sqlite3_finalize(stmt); assert(res==0);
  }

  res=sqlite3_close(db); assert(res==0);
  return;
}

// test2: First insert ROWS rows. Then, launch several threads, each
// inserting a disjoint set of ROWS new rows.
// At the end, check that all rows are in the table.

#define TEST2_ROWS 10000 // number of rows to insert in each phase
#define TEST2_THREADS 1  // number of threads

OSTHREAD_FUNC test2_thread(void *parm){
  int i, res, start;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  char str[256];

  start = (int) (long long) parm * TEST2_ROWS;

  res=sqlite3_open("TEST2", &db); assert(res==0);
  for (i=start; i < start+TEST2_ROWS; ++i){
    sprintf(str, "INSERT INTO t1 VALUES (%d,%d);", i, i);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt); 
    } while (res==SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
  }
  res=sqlite3_close(db); assert(res==0);
  return 0;
}

void test2(){
  sqlite3 *db;
  int res, i, j, k;
  int middle;
  sqlite3_stmt *stmt;
  char str[256];
  OSThread_t thr[TEST2_THREADS+1];
  void *retthread;

  res=sqlite3_open("TEST2", &db); assert(res==0);

  const char *s1 = "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table (table already exists?)\n");
    return;
  }
  
  res=sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  // insert elements in the middle of ranges
  middle = TEST2_THREADS / 2;
  for (i=middle*TEST2_ROWS; i < (middle+1)*TEST2_ROWS; ++i){
    sprintf(str, "INSERT INTO t1 VALUES (%d,%d);", i, i);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    res=sqlite3_step(stmt); assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
  }

  // create TEST2_THREADS threads
  for (i = 0; i <= TEST2_THREADS; ++i){
    if (i == middle) continue; // skip middle part
    res = OSCreateThread(&thr[i], test2_thread, (void*)(long long) i);
    assert(res==0);
  }

  // wait for them to finish
  for (i = 0; i <= TEST2_THREADS; ++i){
    if (i == middle) continue;
    OSWaitThread(thr[i], &retthread);
  }

  s1 = "SELECT * FROM t1 ORDER BY a;";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  k=0;
  do {
    if (res==SQLITE_ROW){
      res = sqlite3_column_count(stmt);
      assert(res==2);
      i=sqlite3_column_int(stmt, 0);
      j=sqlite3_column_int(stmt, 1);
      assert(i==k && j==k);
    }
    res=sqlite3_step(stmt);
    ++k;
  } while (res == SQLITE_ROW);
  
  res=sqlite3_finalize(stmt); assert(res==0);
  assert(k==(TEST2_THREADS+1)*TEST2_ROWS);

  res=sqlite3_close(db); assert(res==0);
}

// test3: Let n = THREADS+1
// First insert rows 0, n, 2n, ..., (ROWS-1)*n. The launch THREADS threads
// i=1,...,THREADS to insert rows i, i+n, i+2n, ..., i+ROWS*n.
// At the end, check that all rows are in the table.

#define TEST3_ROWS 10000 // number of rows to insert in each phase
#define TEST3_THREADS 5  // number of threads

OSTHREAD_FUNC test3_thread(void *parm){
  int i, res, offset;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  char str[256];

  offset = (int) (long long) parm;

  res=sqlite3_open("TEST3", &db); assert(res==0);
  for (i=0; i < TEST3_ROWS; ++i){
    sprintf(str, "INSERT INTO t1 VALUES (%d,%d);",
            i*(TEST3_THREADS+1)+offset,
            i*(TEST3_THREADS+1)+offset);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt);
    } while (res == SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
  }
  res=sqlite3_close(db); assert(res==0);
  return 0;
}

void test3(){
  sqlite3 *db;
  int res, i, j, k;
  sqlite3_stmt *stmt;
  char str[256];
  OSThread_t thr[TEST3_THREADS];
  void *retthread;

  res=sqlite3_open("TEST3", &db); assert(res==0);

  const char *s1 = "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  // insert elements 0, N, 2N, ..., (ROWS-1)N
  for (i=0; i < TEST3_ROWS; ++i){
    sprintf(str, "INSERT INTO t1 VALUES (%d,%d);", i*(TEST3_THREADS+1),
            i*(TEST3_THREADS+1));
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt);
    } while (res == SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
  }

  // create THREADS threads
  for (i=0; i < TEST3_THREADS; ++i){
    res = OSCreateThread(&thr[i], test3_thread, (void*) (long long)(i+1));
    assert(res==0);
  }

  // wait for threads to finish
  for (i=0; i < TEST3_THREADS; ++i){
    OSWaitThread(thr[i], &retthread);
  }

  s1 = "SELECT * FROM t1 ORDER BY a;";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  k=0;
  do {
    if (res==SQLITE_ROW){
      res = sqlite3_column_count(stmt);
      assert(res==2);
      i=sqlite3_column_int(stmt, 0);
      j=sqlite3_column_int(stmt, 1);
      assert(i==k && j==k);
    }
    res=sqlite3_step(stmt);
    ++k;
  } while (res == SQLITE_ROW);
  
  res=sqlite3_finalize(stmt); assert(res==0);
  assert(k==(TEST3_THREADS+1)*TEST3_ROWS);

  res=sqlite3_close(db); assert(res==0);
}

// test4: basic test for JOIN
// number of rows in first table
#define TEST4_NROWS 2000
// number of joined rows in second table per row of first table
#define TEST4_NJOINS  4

// common function for test4 and test5
void test4and5_common(char *dbname, bool indexfirst){
  sqlite3 *db;
  int res, i, j, a, b, c;
  sqlite3_stmt *stmt;
  char str[256];

  res=sqlite3_open(dbname, &db); assert(res==0);

  // create first table
  char *s1 = "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INT);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table t1 (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  if (res != SQLITE_DONE){
    printres(res);
    return;
  }
  sqlite3_finalize(stmt);

  // create second table
  s1 = "CREATE TABLE t2 (c INT, d INT);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table t2 (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  if (res != SQLITE_DONE){
    printres(res);
    return;
  }
  sqlite3_finalize(stmt);

  if (indexfirst){
    // create index
    s1 = "CREATE INDEX i1 ON t2(c);";
    res=sqlite3_prepare(db, s1, -1, &stmt, 0);
    if (res){
      printf("  Error creating table t2 (table already exists?)\n");
      return;
    }
    res=sqlite3_step(stmt);
    if (res != SQLITE_DONE){
      printres(res);
      return;
    }
    sqlite3_finalize(stmt);
  }

  // insert into first table
  for (i=0; i < TEST4_NROWS; ++i){
    sprintf(str, "INSERT INTO t1 VALUES (%d,%d);", i, i);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt); 
    } while (res==SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
  }

  // insert into second table
  for (i=0; i < TEST4_NROWS; ++i){
    for (j=0; j < TEST4_NJOINS; ++j){
      sprintf(str, "INSERT INTO t2 VALUES (%d,%d);", i, j);
      res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
      do {
        res=sqlite3_step(stmt); 
      } while (res==SQLITE_BUSY);
      assert(res==SQLITE_DONE);
      sqlite3_finalize(stmt);
    }
  }

  if (!indexfirst){
    // create index
    s1 = "CREATE INDEX i1 ON t2(c);";
    res=sqlite3_prepare(db, s1, -1, &stmt, 0);
    if (res){
      printf("  Error creating table t2 (table already exists?)\n");
      return;
    }
    res=sqlite3_step(stmt);
    if (res != SQLITE_DONE){
      printres(res);
      return;
    }
    sqlite3_finalize(stmt);
  }

  s1 = "SELECT * FROM t1 JOIN t2 on t1.b=t2.c;";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  i=0;
  do {
    if (res==SQLITE_ROW){
      res = sqlite3_column_count(stmt);
      assert(res==4);
      a=sqlite3_column_int(stmt, 0);
      b=sqlite3_column_int(stmt, 1);
      c=sqlite3_column_int(stmt, 2);
      // d=sqlite3_column_int(stmt, 3);
      assert(a==b);
      assert(b==c);
    }
    res=sqlite3_step(stmt);
    ++i;
  } while (res == SQLITE_ROW);
  
  res=sqlite3_finalize(stmt); assert(res==0);
  assert(i==TEST4_NROWS * TEST4_NJOINS);

  res=sqlite3_close(db); assert(res==0);
  return;
}

void test4(){
  test4and5_common("TEST4", true);
}

// test5: as test4, except that index is created after tables are populated
void test5(){
  test4and5_common("TEST5", false);
}

// test6: basic test for ORDER BY
#define TEST6_NROWS 5000

void test6(){
  sqlite3 *db;
  int res, i, a, b;
  sqlite3_stmt *stmt;
  char str[256];

  res=sqlite3_open("TEST6", &db); assert(res==0);

  // create first table
  char *s1 = "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INT);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table t1 (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  if (res != SQLITE_DONE){
    printres(res);
    return;
  }
  sqlite3_finalize(stmt);

  // insert into first table
  for (i=0; i < TEST6_NROWS; ++i){
    sprintf(str, "INSERT INTO t1 VALUES (%d,%d);", i, TEST6_NROWS-1-i);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt); 
    } while (res==SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
  }

  s1 = "SELECT * FROM t1 ORDER BY b;";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  i=0;
  do {
    if (res==SQLITE_ROW){
      res = sqlite3_column_count(stmt);
      assert(res==2);
      a=sqlite3_column_int(stmt, 0);
      b=sqlite3_column_int(stmt, 1);
      assert(a==TEST6_NROWS-1-i);
      assert(b==i);
    }
    res=sqlite3_step(stmt);
    ++i;
  } while (res == SQLITE_ROW);
  
  res=sqlite3_finalize(stmt); assert(res==0);
  assert(i==TEST6_NROWS);

  res=sqlite3_close(db); assert(res==0);
  return;
}

// test7: basic test for transactions
#define TEST7_TXS 5000 // number of transactions to run
#define TEST7_OPS   10 // number of operations per transaction

// common function for test7 and test5
void test7(){
  sqlite3 *db;
  int res, i, j, a, b;
  sqlite3_stmt *stmt;
  char str[256];

  res=sqlite3_open("TEST7", &db); assert(res==0);

  // create table
  char *s1 = "CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INT);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table t1 (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  if (res != SQLITE_DONE){
    printres(res);
    return;
  }
  sqlite3_finalize(stmt);

  // run transactions
  for (i=0; i < TEST7_TXS; ++i){
    do {
      s1 = "BEGIN TRANSACTION;";
      res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(!res);
      res=sqlite3_step(stmt); assert(res==SQLITE_DONE);
      sqlite3_finalize(stmt);

      for (j=0; j < TEST7_OPS; ++j){
        sprintf(str, "INSERT INTO t1 VALUES (%d,%d);", i*TEST7_OPS+j,
                i*TEST7_OPS+j);
        res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
        do {
          res=sqlite3_step(stmt);
          if (res==SQLITE_BUSY) putchar('B');
        } while (res==SQLITE_BUSY);
        assert(res==SQLITE_DONE);
        sqlite3_finalize(stmt);
      }

      s1 = "COMMIT TRANSACTION;";
      res=sqlite3_prepare(db, s1, -1, &stmt, 0);
      if (res == SQLITE_BUSY){
        putchar('C');
        goto test7_busy;
      }
      assert(!res);
      res=sqlite3_step(stmt);
      if (res == SQLITE_BUSY){
        putchar('D');
        goto test7_busy;
      }
      assert(res==SQLITE_DONE);
     test7_busy:
      sqlite3_finalize(stmt);
    } while (res == SQLITE_BUSY);
  }
    

  s1 = "SELECT * FROM t1 ORDER BY a;";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  i=0;
  do {
    if (res==SQLITE_ROW){
      res = sqlite3_column_count(stmt);
      assert(res==2);
      a=sqlite3_column_int(stmt, 0);
      b=sqlite3_column_int(stmt, 1);
      assert(a==i);
      assert(b==i);
    }
    res=sqlite3_step(stmt);
    ++i;
  } while (res == SQLITE_ROW);
  
  res=sqlite3_finalize(stmt); assert(res==0);
  assert(i==TEST7_TXS * TEST7_OPS);

  res=sqlite3_close(db); assert(res==0);
  return;
}


// test8: concurrent test of transactions

#define TEST8_THREADS 2    // number of threads
#define TEST8_NOPS 5000    // number of operations per thread
#define TEST8_INITIAL 1000 // initial money per account type

OSTHREAD_FUNC test8_thread(void *parm){
  SimplePrng prng;
  int i, res, threadno;
  sqlite3 *db;
  sqlite3_stmt *stmt;
  char str[256];
  int tomove;

  threadno = (int) (long long) parm;
  prng.SetSeed(threadno);

  res=sqlite3_open("TEST8", &db); assert(res==0);
  for (i=0; i < TEST8_NOPS; ++i){
    sprintf(str, "BEGIN TRANSACTION;");
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt);
    } while (res == SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);

    tomove = prng.next() % TEST8_INITIAL - TEST8_INITIAL/2;

    sprintf(str,
            "UPDATE t1 SET balance = balance + (%d) WHERE accttype='CHECKING';",
            tomove);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt);
    } while (res == SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);

    sprintf(str,
            "UPDATE t1 SET balance = balance - (%d) WHERE accttype='SAVINGS';",
            tomove);
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt);
    } while (res == SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);

    sprintf(str, "COMMIT TRANSACTION;");
    res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
    do {
      res=sqlite3_step(stmt);
    } while (res == SQLITE_BUSY);
    assert(res==SQLITE_DONE);
    sqlite3_finalize(stmt);
    
  }
  res=sqlite3_close(db); assert(res==0);
  return 0;
}

void test8(){
  sqlite3 *db;
  int res, i, k, val, total;
  sqlite3_stmt *stmt;
  char str[256];
  OSThread_t thr[TEST8_THREADS];
  void *retthread;

  res=sqlite3_open("TEST8", &db); assert(res==0);

  const char *s1 =
    "CREATE TABLE t1 (accttype VARCHAR(10) PRIMARY KEY, balance INTEGER);";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0);
  if (res){
    printf("  Error creating table (table already exists?)\n");
    return;
  }
  res=sqlite3_step(stmt);
  sqlite3_finalize(stmt);

  // insert two elements into t1, one representing a checking account,
  // one representing a saveings account, each with initial balance of 1000
  sprintf(str, "INSERT INTO t1 VALUES ('CHECKING', %d);", TEST8_INITIAL);
  res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
  do {
    res=sqlite3_step(stmt);
  } while (res == SQLITE_BUSY);
  assert(res==SQLITE_DONE);
  sqlite3_finalize(stmt);

  sprintf(str, "INSERT INTO t1 VALUES ('SAVINGS', %d);", TEST8_INITIAL);
  res=sqlite3_prepare(db, str, -1, &stmt, 0); assert(res==0);
  do {
    res=sqlite3_step(stmt);
  } while (res == SQLITE_BUSY);
  assert(res==SQLITE_DONE);
  sqlite3_finalize(stmt);
  
  // create THREADS threads
  for (i=0; i < TEST8_THREADS; ++i){
    res = OSCreateThread(&thr[i], test8_thread, (void*) (long long)i);
    assert(res==0);
  }

  // wait for threads to finish
  for (i=0; i < TEST8_THREADS; ++i){
    OSWaitThread(thr[i], &retthread);
  }

  total = 0;
  s1 = "SELECT * FROM t1;";
  res=sqlite3_prepare(db, s1, -1, &stmt, 0); assert(res==0);
  res=sqlite3_step(stmt);
  k=0;
  do {
    if (res==SQLITE_ROW){
      res = sqlite3_column_count(stmt);
      assert(res==2);
      //const unsigned char *accttype=sqlite3_column_text(stmt, 0);
      val=sqlite3_column_int(stmt, 1);
      //printf("%s %d\n", accttype, val);
      total += val;
    }
    res=sqlite3_step(stmt);
    ++k;
  } while (res == SQLITE_ROW);
  
  res=sqlite3_finalize(stmt); assert(res==0);
  assert(k==2);
  assert(total == TEST8_INITIAL * 2);

  res=sqlite3_close(db); assert(res==0);
}

int main(){
  printf("Test1\n");
  test1();
  printf("Test2\n");
  test2();
  printf("Test3\n");
  test3();
  printf("Test4\n");
  test4();
  printf("Test5\n");
  test5();
  printf("Test6\n");
  test6();
  printf("Test7\n");
  test7();
  printf("Test8\n");
  test8();
  return 0;
}
