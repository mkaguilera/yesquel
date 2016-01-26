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
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <sstream>
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

#include "bench-mysql-client.h"
#include "bench-log.h"
#include "bench-sql.h"

uint64_t MurmurHash64A ( const void * key, int len);

inline void bind_string(const std::string& to_bind, MYSQL_BIND* bind){
  memset(bind, 0, sizeof(*bind));
  bind->buffer_type = MYSQL_TYPE_BLOB;
  // FML, I hope this is const
  bind->buffer = const_cast<void *>((const void *) to_bind.c_str());
  bind->buffer_length = (unsigned long) to_bind.length();
  bind->length_value = (unsigned long) to_bind.length();
}

inline void bind_uint64(uint64_t *data, MYSQL_BIND* bind){
  memset(bind, 0, sizeof(*bind));
  bind->buffer_type = MYSQL_TYPE_LONGLONG;
  bind->buffer = (void*) data;
  bind->buffer_length = 8;
  bind->length_value = 8;
}

int CreateMysqlClient(std::string dbname, BenchmarkClient** clp, bool create_table){
  *clp = new MysqlClient(dbname, create_table);
  return (*clp)->Init();
}

// CAUTION: this function has been hacked so that it uses INNODB 
// if the database host is non-local
int MysqlClient::Init(){
  int ret = -1;
  std::string dbname, host;
  try {
    size_t pos = _dbname.find(":");
    dbname = _dbname.substr(pos + 1);
    host = _dbname.substr(0, pos);
    assert(pos > 0);
    std::string user;
    char* passwd;
    LOG("_dbname %s host %s dbname %s\n", _dbname.c_str(), host.c_str(), dbname.c_str());
    //if (host == std::string("127.0.0.1")){
    user = DBUSER;
    passwd = DBPASSWD;
    //} else {
    //  user = std::string("msrgst35");
    //  passwd = strdup("galaxysmasher");
    //}
    
    _dbconn = mysql_init(nullptr);
    if (mysql_real_connect(_dbconn, host.c_str(), user.c_str(), passwd, 
                           dbname.c_str(), 0, nullptr, 0)){
      ret = 0;
      free(passwd);
    } else {
      LOG( "Couldn't connect to database (%s): Error: %s\n",
              _dbname.c_str(), mysql_error(_dbconn));
      exit(1);
      sleep(1);
      mysql_close(_dbconn);
      return Init();
    }
  } catch (const std::out_of_range){
    LOG( "Connection string %s is invalid (no colon)\n\
         format is hostname:dbname\n", _dbname.c_str());
  }
  if (_create){
    ret = mysql_query(_dbconn, "START");
    ret = mysql_query(_dbconn, "DROP TABLE IF EXISTS " TABLENAME);
    ret = mysql_query(_dbconn, "DROP TABLE IF EXISTS synctable");
    if (ret){
      LOG("Error dropping table: %s\n", mysql_error(_dbconn));
    } else {
      if (host == std::string("127.0.0.1")){
        LOG("creating NDBCLUSTER tables");
        ret = mysql_query(_dbconn, BENCHMARK_TABLE_STMT_MYSQL "ENGINE=NDBCLUSTER");
        ret = mysql_query(_dbconn, SYNC_TABLE_STMT_MYSQL "ENGINE=NDBCLUSTER");
      } else {
        LOG("creating MEMORY tables");
        ret = mysql_query(_dbconn, MEMORY_SYNC_TABLE);
        ret = mysql_query(_dbconn, MEMORY_BENCH_TABLE);
      }
    }
    ret = mysql_query(_dbconn, "COMMIT");
  }
  return ret;
}

MysqlClient::~MysqlClient(){
  if (_dbconn) mysql_close(_dbconn);
}

MysqlClient::MysqlClient(const char *database, bool create) :
  _dbname(database), _create(create), _dbconn(nullptr){
}

MysqlClient::MysqlClient(const std::string& database, bool create) :
  _dbname(database), _create(create), _dbconn(nullptr){
}

struct BindReadHelper {
  BindReadHelper(const FieldList& fl) : fields(fl){
    bind = new MYSQL_BIND[fields.size()];
    item_lengths = new unsigned long [fields.size()];
  }

  ~BindReadHelper(){ 
    delete [] bind;
    delete [] item_lengths;
  }

  int FetchRow(MYSQL_STMT* stmt, ValueMap& result);

  const FieldList& fields;
  MYSQL_BIND* bind;
  unsigned long* item_lengths;

private:
  BindReadHelper();
  BindReadHelper(const BindReadHelper&);
  BindReadHelper& operator=(const BindReadHelper &);
};


// This function is probably not as efficient as it could be. Unfortunately,
// the MySQL API and documentation are terrible, so we'll leave it like
// this for now.
int BindReadHelper::FetchRow(MYSQL_STMT* stmt, ValueMap& result){
  // Initialize the bind space
  for (int i = 0; i < (int)fields.size(); ++i){
    memset(&bind[i], 0, sizeof(MYSQL_BIND));
    bind[i].buffer_type = MYSQL_TYPE_BLOB;
    bind[i].length = &item_lengths[i];
    bind[i].buffer_length = 256;
    bind[i].buffer = malloc(256);
  }
  if (0 != mysql_stmt_bind_result(stmt, bind)){
    LOG( "Error fetching row: %s\n", mysql_stmt_error(stmt));
    return -1;
  }

  int rc = mysql_stmt_fetch(stmt);
  if (rc == 0){
    int count = 0;
    for (auto& it : fields){
      // bind[count].buffer = malloc(item_lengths[count]);
      // bind[count].buffer_length = item_lengths[count];
      if (mysql_stmt_fetch_column(stmt, &bind[count], count, 0)){
        LOG( "Error fetching column: %s\n", mysql_stmt_error(stmt));
      } else {
        result[it] = std::string((const char *)bind[count].buffer);
      }
      count++;
   }
  }
  for (auto i = 0; i < (int)fields.size(); ++i){
    free(bind[i].buffer);
  }
  return rc;
}

MYSQL_STMT *MysqlClient::construct_sql_read(const TableId& table, const Key& key,
                                const FieldList& fields){
  MYSQL_STMT* ret = mysql_stmt_init(_dbconn);
  bool prepared = false;
  std::string sql;
  static uint64_t hash;
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

  int rc = mysql_stmt_prepare(ret, sql.c_str(), (unsigned long) sql.length());
  if (rc == 0){
    prepared = true;
    MYSQL_BIND *bind = new MYSQL_BIND [1];
    hash = MurmurHash64A(key.c_str(), (int)key.length());
    bind_uint64(&hash, &bind[0]);
    rc = mysql_stmt_bind_param(ret, bind);
    delete [] bind;
  } 
  if (rc != 0){
    LOG( "read %s failed: %s\n", (prepared) ? "binding" : "preparing",
      mysql_stmt_error(ret));
    mysql_stmt_close(ret);
    ret = nullptr;
  } 
  return ret;
}

int MysqlClient::read(const TableId& table, const Key& key, const FieldList& fields,
      ValueMap& result){
  MYSQL_STMT* stmt = construct_sql_read(table, key, fields);
  int rc = -1;
  if (stmt){
    if (0 == (rc = mysql_stmt_execute(stmt))){
      BindReadHelper helper(fields);
      rc = helper.FetchRow(stmt, result);
    }
    if (rc == 1 || rc == MYSQL_DATA_TRUNCATED){
      LOG( "%s: %s\n", (rc == 1) ? "Error fetching rows" : "Rows were truncated",
              mysql_stmt_error(stmt));
    } else {
       rc = 0;  // No rows, but that's okay.
    }
    mysql_stmt_close(stmt);
  }
  return rc;
}

MYSQL_STMT *MysqlClient::construct_sql_scan(const TableId& table, const Key& start_key, int count,
        const FieldList& fields){
  MYSQL_STMT* ret = mysql_stmt_init(_dbconn);
  std::string sql;
  bool prepared = false;
  static uint64_t hash;
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

  int rc = mysql_stmt_prepare(ret, sql.c_str(), (unsigned long) sql.length());
  if (rc == 0){
    prepared = true;
    MYSQL_BIND *bind = new MYSQL_BIND [1];
    hash = MurmurHash64A(start_key.c_str(), (int)start_key.length());
    bind_uint64(&hash, &bind[0]);
    rc = mysql_stmt_bind_param(ret, bind);
    delete [] bind;
  } 
  if (rc != 0){
    LOG( "scan %s failed: %s\n", (prepared) ? "binding" : "preparing",
      mysql_stmt_error(ret));
    mysql_stmt_close(ret);
    ret = nullptr;
  } 
  return ret;
}

int MysqlClient::scan(const TableId& table, const Key& start_key, int count, const FieldList& fields,
      std::vector<ValueMap>& result){
  MYSQL_STMT* stmt = construct_sql_scan(table, start_key, count, fields);
  int rc = -1;
  if (stmt){
    if (0 == (rc = mysql_stmt_execute(stmt))){
      BindReadHelper helper(fields);
      do {
        result.push_back(ValueMap());
        rc = helper.FetchRow(stmt, result.back());
      } while(--count > 0 && rc == 0);
    }
    if (rc == 1 || rc == MYSQL_DATA_TRUNCATED){
      LOG( "Error fetching rows or rows were truncated: %s\n",
        mysql_stmt_error(stmt));
    } else {
      rc = 0;
    }
    mysql_stmt_close(stmt);
  }
  return rc;
}

MYSQL_STMT *MysqlClient::construct_sql_update(const TableId& table, const Key& key, const ValueMap& values){
  MYSQL_STMT* ret = mysql_stmt_init(_dbconn);
  bool prepared = false;
  static uint64_t hash;
  std::string sql;
  sql.append("UPDATE ");
  sql.append(table);
  sql.append(" SET ");
  for (auto& it : values){
    sql.append(it.first);
    sql.append("=UPPER(");
    sql.append(it.first);
    sql.append("),");
  }
  sql.back() = ' ';
  sql.append("WHERE ");
  sql.append(KEYNAME);
  sql.append("=?");

  int rc = mysql_stmt_prepare(ret, sql.c_str(), (unsigned long) sql.length());
  if (rc == 0){
    int count = 0;
    prepared = true;
    //MYSQL_BIND *bind = new MYSQL_BIND [values.size() + 1];
    //for (auto& it : values){
    //  bind_string(it.second, &bind[count++]);
    //}
    MYSQL_BIND *bind = new MYSQL_BIND [1];
    hash = MurmurHash64A(key.c_str(), (int)key.length());
    bind_uint64(&hash, &bind[count++]);
    rc = mysql_stmt_bind_param(ret, bind);
    delete [] bind;
  } 
  if (rc != 0){
    LOG( "insert %s failed: %s\n", (prepared) ? "binding" : "preparing",
      mysql_stmt_error(ret));
    mysql_stmt_close(ret);
    ret = nullptr;
  } 
  return ret;
}

inline int MysqlClient::execute_sql(MYSQL_STMT* stmt, const char* caller){
  int rc = -1;
  if (stmt){
    rc = mysql_stmt_execute(stmt);
    if (rc != 0){
      LOG( "Error executing %s: %s\n", caller, mysql_stmt_error(stmt));
    }
    mysql_stmt_close(stmt);
  }
  if (rc) _should_abort = true;
  return rc;
}

int MysqlClient::begin(void){
  _should_abort = false;
  return mysql_query(_dbconn, "START TRANSACTION");
}

int MysqlClient::complete(void){
  if (_should_abort){
    return mysql_query(_dbconn, "ROLLBACK");
  } else {
    return mysql_query(_dbconn, "COMMIT");
  }
}

int MysqlClient::BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values){
  char* err = nullptr;
  int rc = begin();
  if (!rc){
    assert(keys.size() == values.size());
    for (int i = 0; i < (int)keys.size(); ++i){
      if(0 != insert(table, keys[i], values[i])){
        complete();
        LOG("ERROR INSERTING IN BULK INSERT\n");
        sleep(1);
        return BulkInsert(table, keys, values);
      }
    }
    rc = complete();
  }
  err = const_cast<char*>(mysql_error(_dbconn));
  if (rc){
    LOG( "Error: %s\n", err);
    return BulkInsert(table, keys, values);
  }
  return rc;
}

int MysqlClient::update(const TableId& table, const Key& key, const ValueMap& values){
  MYSQL_STMT* stmt = construct_sql_update(table, key, values);
  return execute_sql(stmt, __FUNCTION__);
}

MYSQL_STMT *MysqlClient::construct_sql_insert(const TableId& table, const Key& key, const ValueMap& values){
  MYSQL_STMT* ret = mysql_stmt_init(_dbconn);
  bool prepared = false;
  std::string sql;
  static uint64_t hash;
  sql.append("INSERT INTO ");
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
  sql.append(" ON DUPLICATE KEY UPDATE ");
  for (auto& it: values){
    sql.append(it.first);
    sql.append("=?,");
  }
  sql.back() = ' ';

  int rc = mysql_stmt_prepare(ret, sql.c_str(), (unsigned long) sql.length());
  if (rc == 0){
    int count = 0;
    prepared = true;
    MYSQL_BIND *bind = new MYSQL_BIND [2 * values.size() + 1];
    hash = MurmurHash64A(key.c_str(), (int)key.length());
    bind_uint64(&hash, &bind[count++]);
    // First time bind values for the insert
    for (auto& it : values){
      bind_string(it.second, &bind[count++]);
    }
    // Second time bind values for the update
    for (auto& it : values){
      bind_string(it.second, &bind[count++]);
    }
    rc = mysql_stmt_bind_param(ret, bind);
    delete [] bind;
  } 
  if (rc != 0){
    LOG( "insert %s failed: %s\n", (prepared) ? "binding" : "preparing",
            mysql_stmt_error(ret));
    mysql_stmt_close(ret);
    ret = nullptr;
  }
  return ret;
}

int MysqlClient::insert(const TableId& table, const Key& key, const ValueMap& values){
  MYSQL_STMT* stmt = construct_sql_insert(table, key, values);
  return execute_sql(stmt, __FUNCTION__);
}


MYSQL_STMT *MysqlClient::construct_sql_remove(const TableId& table, const Key& key){
  MYSQL_STMT* ret = mysql_stmt_init(_dbconn);
  bool prepared = false;
  std::string sql;
  static uint64_t hash;
  sql.append("DELETE FROM ");
  sql.append(table);
  sql.append(" WHERE ");
  sql.append(KEYNAME);
  sql.append("=?");

  int rc = mysql_stmt_prepare(ret, sql.c_str(), (unsigned long) sql.length());
  if (rc == 0){
    prepared = true;
    MYSQL_BIND bind[1];
    hash = MurmurHash64A(key.c_str(), (int)key.length());
    bind_uint64(&hash, &bind[0]);
    rc = mysql_stmt_bind_param(ret, bind);
  } 
  if (rc != 0){
    LOG( "remove %s failed: %s\n", (prepared) ? "binding" : "preparing",
      mysql_stmt_error(ret));
    mysql_stmt_close(ret);
    ret = nullptr;
  }
  return ret;
}

int MysqlClient::remove(const TableId& table, const Key& key){
  MYSQL_STMT* stmt = construct_sql_remove(table, key);
  return execute_sql(stmt, __FUNCTION__);
}
