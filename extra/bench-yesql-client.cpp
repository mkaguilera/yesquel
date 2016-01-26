/*
  Original code: Copyright (c) 2014 Microsoft Corporation
  Modified code: Copyright (c) 2015-2016 VMware, Inc
  All rights reserved. 

  Written by Joshua B. Leners

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
#include <float.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bench-yesql-client.h"
#include "bench-log.h"
#include "bench-sql.h"

uint64_t MurmurHash64A (const void * key, int len);

int CreateYesqlClient(std::string dbname, BenchmarkClient** clp, bool create_table){
    *clp = new YesqlClient(dbname, create_table);
    return (*clp)->Init();
}

int YesqlClient::Init(){
  int ret = -1;
  int retries = 0;
  do {
    ret = sqlite3_open(_dbname.c_str(), &_dbhandle);
    if (ret != SQLITE_OK){
      ++retries;
      usleep(1000000);
    }
  } while (ret != SQLITE_OK && retries < 10);
    
  if (ret == SQLITE_OK && _create){
    LOG("Succesfully opened db, about to create a table:\n%s\n", BENCHMARK_TABLE_STMT);
    char* err_str = nullptr;
    retry1:
    ret = sqlite3_exec(_dbhandle, BENCHMARK_TABLE_STMT, nullptr, nullptr, &err_str);
    if (ret == SQLITE_BUSY){ usleep(1000); goto retry1; }
    if (ret != SQLITE_OK){
      LOG("SQL error %s\n", err_str);
      sqlite3_free(err_str);
      sqlite3_close(_dbhandle);
    }
    else {
      retry2:
      ret = sqlite3_exec(_dbhandle, SYNC_TABLE_STMT, nullptr, nullptr, &err_str);
      if (ret == SQLITE_BUSY){ usleep(1000); goto retry2; }
      if (ret != SQLITE_OK){
        LOG("SQL error %s\n", err_str);
        sqlite3_free(err_str);
        sqlite3_close(_dbhandle);
      }
    }
  } else if (ret != SQLITE_OK){
    LOG("Can't open database %s (%d): %s\n", _dbname.c_str(),
         ret, sqlite3_errmsg(_dbhandle));
    sqlite3_close(_dbhandle);
  }
  return ret;
}

YesqlClient::~YesqlClient(){
  if(_dbhandle){
    sqlite3_close(_dbhandle);
  }
}

YesqlClient::YesqlClient(const char *database, bool create) :
  _dbname(database), _create(create), _dbhandle(nullptr){
}

YesqlClient::YesqlClient(const std::string& database, bool create) :
  _dbname(database), _create(create), _dbhandle(nullptr){
}

inline int bind_key(sqlite3_stmt* stmt, const Key& key){
  return sqlite3_bind_int64(stmt, 1, MurmurHash64A(key.c_str(), (int) key.length()));
  //return sqlite3_bind_text(stmt, 1, key.c_str(), (int) key.length(), SQLITE_STATIC);
}

thread_local sqlite3_stmt **cache_stmt=0;
thread_local char **cache_sql=0;
#define STMT_CACHE_SIZE 30

// cache sqlite3_stmt so that we don't have to prepare them every time
// Cache has multiple indexed entries, each with a sql query and an stmt.
// Upon calling with an index, the function checks if the sql query
// matches what's the cache at given index. If so, the cached stmt
// is returned. Otherwise, a new stmt is prepared, which replaces
// the cached entry at the given index. The intention is that
// the function be called with the same sql queries for a given index
// most of the time.
int YesqlClient::stmt_cache(int index, const char *sql, sqlite3_stmt **ret){
  assert(0 <= index && index < STMT_CACHE_SIZE);
  int res;
  if (!cache_stmt){
    int j;
    cache_stmt = new sqlite3_stmt*[STMT_CACHE_SIZE];
    for (j=0; j < STMT_CACHE_SIZE; ++j) cache_stmt[j] = 0;
    
    cache_sql = new char*[STMT_CACHE_SIZE];
    for (j=0; j < STMT_CACHE_SIZE; ++j) cache_sql[j] = 0;
  }

  if (cache_sql[index] && strcmp(cache_sql[index], sql) == 0){
    res = sqlite3_clear_bindings(cache_stmt[index]);
    if (res != SQLITE_OK) goto replace_cache;
    *ret = cache_stmt[index];
    return SQLITE_OK;
  }

 replace_cache:
  int rc = sqlite3_prepare_v2(_dbhandle, sql, (int) strlen(sql)+1, ret, nullptr);
  if (rc == SQLITE_OK){
    if (cache_stmt[index]){
      sqlite3_finalize(cache_stmt[index]);
      free(cache_sql[index]);
    }
    cache_stmt[index] = *ret;
    cache_sql[index] = strdup(sql);
  } else {
    sqlite3_finalize(*ret);
  }
  return rc;
}

#define STMT_INDEX_READ 0
#define STMT_INDEX_UPDATE 1
#define STMT_INDEX_INSERT 2
#define STMT_INDEX_REMOVE 3
#define STMT_INDEX_SCAN 4



sqlite3_stmt *YesqlClient::construct_sql_read(const TableId& table, const Key& key, const FieldList& fields){
  sqlite3_stmt* ret = nullptr;
  bool prepared = false;
  int rc=0;
  std::string sql;

  sql.append("SELECT ");
  for (auto& it : fields){
    sql.append(it);
    sql.append(",");
  }
  sql.back() = ' ';
  sql.append(" FROM ");
  sql.append(table);
  sql.append(" WHERE ");
  sql.append(KEYNAME);
  sql.append("=?");

  rc = stmt_cache(STMT_INDEX_READ, sql.c_str(), &ret);
  
  if (rc == SQLITE_OK){
    prepared = true;
    rc = bind_key(ret, key);
  }
  if (rc != SQLITE_OK){
    if (sql == "") sql.append("(previously-prepared-query)");
    LOG("Error %s read: %s\n\tSQL was: %s\n", (prepared) ? "binding" : "preparing",
	            sqlite3_errmsg(_dbhandle), sql.c_str());
    //sqlite3_finalize(ret);
    ret = nullptr;
  }
  return ret;
}


int YesqlClient::read_result_row(sqlite3_stmt* stmt, const FieldList& fields, ValueMap& result){
  int rc = -1;
  rc = sqlite3_step(stmt);
  if (rc == SQLITE_ROW){
    int count = 0;
    for (auto& i: fields){
      const char* char_str = (const char *)sqlite3_column_text(stmt, count++);
      result[i] = std::string((char_str) ? char_str : "(nil)");
    }
  } else if (rc != SQLITE_DONE){
    LOG("Error reading result row (%d): %s\n", rc, sqlite3_errmsg(_dbhandle));
  }
  return rc;
}

int YesqlClient::read(const TableId& table, const Key& key, const FieldList& fields,
		  ValueMap& result){
  sqlite3_stmt* stmt = construct_sql_read(table, key, fields);
  int rc = -1;
  if (stmt){
    rc = read_result_row(stmt, fields, result);
    sqlite3_reset(stmt);
  }
  return (rc == SQLITE_DONE || rc == SQLITE_ROW) ? 0 : -1;
}

sqlite3_stmt *YesqlClient::construct_sql_scan(const TableId& table, const Key& start_key, int count,
				const FieldList& fields){
  sqlite3_stmt* ret = nullptr;
  std::string sql;
  bool prepared = false;
  sql.append("SELECT ");
  for (auto& it : fields){
    sql.append(it);
    sql.append(",");
  }
  sql.back() = ' ';
  sql.append(" FROM ");
  sql.append(table);
  sql.append(" WHERE ");
  sql.append(KEYNAME);
  sql.append(">=?");
  sql.append(" LIMIT ");
  char num_buf[10];
  snprintf(num_buf, 10, "%d", count);
  sql.append(num_buf);

  int rc = stmt_cache(STMT_INDEX_SCAN, sql.c_str(), &ret);
  if (rc == SQLITE_OK){
    prepared = true;
    rc = bind_key(ret, start_key);
  }
  if (rc != SQLITE_OK){
    LOG("Error %s scan: %s\n\tSQL was: %s\n", (prepared) ? "binding" : "preparing",
	            sqlite3_errmsg(_dbhandle), sql.c_str());
    //sqlite3_finalize(ret);
    ret = nullptr;
  }
  return ret;
}

int YesqlClient::scan(const TableId& table, const Key& start_key, int count, const FieldList& fields,
		  std::vector<ValueMap>& result){
  sqlite3_stmt* stmt = construct_sql_scan(table, start_key, count, fields);
  int rc = -1;
  if (stmt){
    do {
      result.push_back(ValueMap());
      rc = read_result_row(stmt, fields, result.back());
      count--;
    } while (count > 0 && rc == SQLITE_ROW);
    sqlite3_reset(stmt);
  }
  return (rc == SQLITE_DONE || rc == SQLITE_ROW) ? 0 : -1;
}

sqlite3_stmt *YesqlClient::construct_sql_update(const TableId& table, const Key& key, const ValueMap& values){
  sqlite3_stmt* ret = nullptr;
  bool prepared = false;
  std::string sql;
  sql.append("UPDATE OR FAIL ");
  sql.append(table);
  sql.append(" SET ");
  for (auto& it : values){
    sql.append(it.first);
    sql.append("=upper(");
    sql.append(it.first);
    sql.append("),");
  }
  sql.back() = ' ';
  sql.append("WHERE ");
  sql.append(KEYNAME);
  sql.append("=?");
  
  int rc = stmt_cache(STMT_INDEX_UPDATE, sql.c_str(), &ret);
  
  if (rc == SQLITE_OK){
    int count = 1;
    prepared = true;
    //for (auto& it : values){
    //  rc = sqlite3_bind_text(ret, count++, it.second.c_str(), (int) it.second.length(), SQLITE_STATIC);
    //  if (rc != SQLITE_OK) break;
    //}
    rc = sqlite3_bind_int64(ret, count, MurmurHash64A(key.c_str(), (int) key.length()));
    //rc = sqlite3_bind_text(ret, count, key.c_str(), (int) key.length(), SQLITE_STATIC);
  }
  if (rc != SQLITE_OK){
    LOG("Error %s update: %s\n\tSQL was: %s\n", (prepared) ? "binding" : "preparing",
	            sqlite3_errmsg(_dbhandle), sql.c_str());
    //sqlite3_finalize(ret);
    ret = nullptr;
  }
  return ret;
}

inline int YesqlClient::execute_sql(sqlite3_stmt* stmt, const char* caller){
  int rc = -1;
  if (stmt){
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE){
      LOG("Error executing %s: (%d) %s\n", caller, rc, sqlite3_errmsg(_dbhandle));
    }
    sqlite3_reset(stmt);
  }
  return (rc == SQLITE_DONE) ? 0 : -1;
}

int YesqlClient::update(const TableId& table, const Key& key, const ValueMap& values){
  sqlite3_stmt* stmt = construct_sql_update(table, key, values);
  return execute_sql(stmt, __FUNCTION__);
}

sqlite3_stmt *YesqlClient::construct_sql_insert(const TableId& table, const Key& key, const ValueMap& values){
  sqlite3_stmt* ret = nullptr;
  bool prepared = false;
  std::string sql;
  sql.append("INSERT OR REPLACE INTO ");
  sql.append(table);
  sql.append(" (");
  sql.append(KEYNAME);
  sql.append(",");
  for (auto& it: values){
    sql.append(it.first);
    sql.append(",");
  }
  sql.back() = ')';
  sql.append(" VALUES (");
  sql.append("?,");
  for (auto& it : values){
    sql.append("?,");
  }
  sql.back() = ')';

  int rc = stmt_cache(STMT_INDEX_INSERT, sql.c_str(), &ret);
  
  if (rc == SQLITE_OK){
    int count = 1;
    rc = sqlite3_bind_int64(ret, count++, MurmurHash64A(key.c_str(), (int) key.length()));
    //rc = sqlite3_bind_text(ret, count++, key.c_str(), (int) key.length(), SQLITE_STATIC);
    
    for (auto& it : values){
      if (rc != SQLITE_OK) break;
      rc = sqlite3_bind_text(ret, count++, it.second.c_str(), (int) it.second.length(), SQLITE_STATIC);
    }
  }
  if (rc != SQLITE_OK){
    LOG("Error %s insert: %s\n\tSQL was: %s\n", (prepared) ? "binding" : "preparing",
	            sqlite3_errmsg(_dbhandle), sql.c_str());
    //sqlite3_finalize(ret);
    ret = nullptr;
  }
  return ret;
}

int YesqlClient::insert(const TableId& table, const Key& key, const ValueMap& values){
  sqlite3_stmt* stmt = construct_sql_insert(table, key, values);
  return execute_sql(stmt, __FUNCTION__);
}

int YesqlClient::begin(void){
  char* err;
  return sqlite3_exec(_dbhandle, "BEGIN;", NULL, NULL, &err);
}

int YesqlClient::complete(void){
  char* err;
  if (!_should_abort){
    int rc = sqlite3_exec(_dbhandle, "COMMIT;", NULL, NULL, &err);
    if (rc == 0) return 0;
  }
  return sqlite3_exec(_dbhandle, "ROLLBACK;", NULL, NULL, &err);
}

int YesqlClient::BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values){
  char* err = nullptr;
  int rc = -1;
  int retries = 100;
  while (rc && retries > 0){
    retries--;
    rc = 0;
    if (retries < 99){
      LOG("Retrying in BULK_INSERT\n");
    }
    rc = sqlite3_exec(_dbhandle, "BEGIN;", NULL, NULL, &err);
    if (rc){
      LOG("Got error BEGIN: %s\n", sqlite3_errmsg(_dbhandle));
      sqlite3_exec(_dbhandle, "ROLLBACK;", NULL, NULL, &err);
      continue;
    }
    assert(keys.size() == values.size());
    for (auto i = 0; i < (int)keys.size(); ++i){
      int count = 15;
     // LOG("Doing insert\n");
      while (0 != insert(table, keys[i], values[i])){
        if (count-- == 0){
          LOG("Died at %d\n", i);
          exit(1);
          LOG("YOU SHOULD NEVER SEE THIS!\n");
        }
        LOG("Retrying in Bulk Insert: %d\n", count);
      }
    }
    rc = sqlite3_exec(_dbhandle, "COMMIT;", NULL, NULL, &err);
    if (rc){
      LOG("Got error COMMIT: %s\n", sqlite3_errmsg(_dbhandle));
      sqlite3_exec(_dbhandle, "ROLLBACK;", NULL, NULL, &err);
      continue;
    }
  }
  return rc;
}

sqlite3_stmt *YesqlClient::construct_sql_remove(const TableId& table, const Key& key){
  sqlite3_stmt* ret;
  bool prepared = false;
  std::string sql;
  sql.append("DELETE FROM ");
  sql.append(table);
  sql.append(" WHERE ");
  sql.append(KEYNAME);
  sql.append("=?");
  
  int rc = stmt_cache(STMT_INDEX_REMOVE, sql.c_str(), &ret);
  
  if (rc == SQLITE_OK){
    rc = bind_key(ret, key);
  }
  if (rc != SQLITE_OK){
    LOG("Error %s delete: %s\n\tSQL was: %s\n", (prepared) ? "binding" : "preparing",
	            sqlite3_errmsg(_dbhandle), sql.c_str());
    //sqlite3_finalize(ret);
    ret = nullptr;
  }
  return ret;
}

int YesqlClient::remove(const TableId& table, const Key& key){
  sqlite3_stmt* stmt = construct_sql_remove(table, key);
  int rc = execute_sql(stmt, __FUNCTION__);
  if (rc) _should_abort = true;
  return rc;
}
