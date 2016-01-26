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
#include <stdlib.h>
#include <malloc.h>
#include <unistd.h>

#include <assert.h>
#include <stdarg.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>
#include <errno.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>
#include <string>

#include "bench-dtree-client.h"
#include "bench-sql.h"

#define USERTABLE_NO 66
#define SYNCTABLE_NO 67

static std::mutex init_lock;

uint64_t MurmurHash64A (const void * key, int len);
static const unsigned int _hash_seed = 0xBADCAB1E;

int DtreeClient::begin(){
    _txcount++;
    if (_txcount == 1){
      return DdStartTx(_dbhandle);
    } else {
      return 0;
    }
}

int DtreeClient::end(){
  _txcount--; 
  if(_txcount == 0){
    return DdCommitTx(_dbhandle);
  } else {
    return 0;
  }
}

DtreeClient::DtreeClient(const std::string& database, bool create) :
  _dbname(database), _create(create), _txcount(0){}

DtreeClient::DtreeClient(const char* database, bool create) :
  _dbname(database), _create(create), _txcount(0){}

int CreateDtreeClient(const std::string dbname, BenchmarkClient** clp, bool create_table){
  *clp = new DtreeClient(dbname, create_table);
  return (*clp)->Init();
}

void init_functions(){
      DdInit();
}

static std::once_flag flag;

int DtreeClient::Init(){
  DdTable* table, *synctable;
  std::call_once(flag, std::bind(init_functions));
  int res = DdInitConnection(const_cast<char *>(_dbname.c_str()), _dbhandle);
  assert(!res);

  if (_create){
    res = DdCreateTable(_dbhandle, USERTABLE_NO, table); assert(!res);
    res = DdCreateTable(_dbhandle, SYNCTABLE_NO, synctable); assert(!res);
    _table_map[TABLENAME] = table;
    _table_map["synctable"] = synctable;
  } else {
    // Better to use a condition variable, but threads might wait on
    // another machine
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    int res = DdOpenTable(_dbhandle, USERTABLE_NO, table); assert(!res);
    res = DdOpenTable(_dbhandle, SYNCTABLE_NO, synctable); assert(!res);
    _table_map[TABLENAME] = table;
    _table_map["synctable"] = synctable;
  }
  return res;
}

DtreeClient::~DtreeClient(){ 
  for (auto &i : _table_map)
    DdCloseTable(i.second);
  DdCloseConnection(_dbhandle);
}

int DtreeClient::copy_to_scratch(const void* ptr, i32 size){
  assert(_curr - _scratch + size < (int)sizeof(_scratch));
  memcpy(_curr, ptr, size);
  _curr+=size;
  return 0;
}

int DtreeClient::serialize(const ValueMap& values){
  reset_scratch();
  i32 nFields = htonl(i32(values.size()));
  copy_to_scratch(&nFields, sizeof(nFields));
  for (auto &i : values){
    i32 len_name = htonl(i32(i.first.size()));
    copy_to_scratch(&len_name, sizeof(len_name));
    copy_to_scratch(i.first.c_str(), (int) i.first.size());

    i32 len_value = htonl(i32(i.second.size()));
    copy_to_scratch(&len_value, sizeof(len_value));
    copy_to_scratch(i.second.c_str(), (int) i.second.size());
  }
  return (int) (_curr - _scratch);
}

int DtreeClient::read_int(){
  int ret = ntohl(*((i32*) _curr));
  _curr += sizeof(i32);
  return ret;
}

const std::string DtreeClient::read_string(){
  int len = read_int();
  std::string ret(_curr, len);
  _curr += len;
  return ret;
}

int DtreeClient::deserialize(ValueMap& values){
  reset_scratch();
  int nFields = read_int();
  for (auto i = 0; i < nFields; ++i){
    const std::string k = read_string();
    values[k] = read_string();
  }
  return 0;
}

int DtreeClient::read(const TableId& table, const Key& key, const FieldList& fields,
                  ValueMap& result){
  int res = begin(); assert(!res);
  i64 ikey = MurmurHash64A(key.c_str(), (int)key.size());  
  int len = 0;
  res = DdLookup(_table_map[table], ikey, _scratch, sizeof(_scratch), &len);
  if (len != 0){
    deserialize(result);
  }
  int res2 = end();
  if (!res) res = res2;
  return res;
}

struct scan_helper_args {
  scan_helper_args(DtreeClient* _clp, std::vector<ValueMap>& _res) :
    clp(_clp), result(_res){}
  DtreeClient* clp;
  std::vector<ValueMap>& result;
};

void scan_helper(i64 key, const char *data, int len, int n, bool eof, void* arg){
  scan_helper_args* parm = (scan_helper_args *) arg;
  if (eof) return;
  ValueMap res;
  memcpy(parm->clp->_scratch, data, len);
  parm->clp->deserialize(res);
  parm->result.push_back(res);
}

int DtreeClient::scan(const TableId& table, const Key& start_key, int count,
     const FieldList& fields, std::vector<ValueMap>& result){
  int res = begin(); assert(!res);
  i64 ikey = MurmurHash64A(start_key.c_str(), (int)start_key.size());  
  scan_helper_args parm(this, result);
  DdScan(_table_map[table], ikey, count, scan_helper, &parm);
  return end();
}

int DtreeClient::scanNodata(const TableId& table, const Key& start_key, int count,
     const FieldList& fields, std::vector<ValueMap>& result){
  int res = begin(); assert(!res);
  i64 ikey = MurmurHash64A(start_key.c_str(), (int)start_key.size());  
  scan_helper_args parm(this, result);
  DdScan(_table_map[table], ikey, count, scan_helper, &parm, false);
  return end();
}

int updatecallback(char *buf, int len, void *arg){
  DtreeClient *clp = (DtreeClient*) arg;
  ValueMap values;
  clp->deserialize(values);
  for (auto& it : values){
    for (auto &c : it.second){
      c = toupper(c);
    }
  }
  
  len = clp->serialize(values);
  return len;
}

int DtreeClient::update(const TableId& table, const Key& key, const ValueMap& values){
  int res = begin(); assert(!res);
  i64 ikey = MurmurHash64A(key.c_str(), (int)key.size());

  res = DdUpdate(_table_map[table], ikey, _scratch, sizeof(_scratch),
                 updatecallback, (void*) this);
  int res2 = end();
  if (!res) res = res2;
  return res;
}

int DtreeClient::insert(const TableId& table, const Key& key, const ValueMap& values){
  int res = begin(); assert(!res);
  i64 ikey = MurmurHash64A(key.c_str(), (int)key.size());  
  int size = serialize(values);
  res = DdInsert(_table_map[table], ikey, _scratch, size);
  
  int res2 = end();
  if (!res) res = res2;
  return res;
}

int DtreeClient::remove(const TableId& table, const Key& key){
  int res = begin(); assert(!res);
  i64 ikey = MurmurHash64A(key.c_str(), (int)key.size());  
  res = DdDelete(_table_map[table], ikey);
  int res2 = end();
  if (!res) res = res2;
  return res;
}

int DtreeClient::BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values){
  int idx = 0;
  int res = begin();
  for (auto &key : keys){
    int retries = 10;
    do {
      res = insert(table, key, values[idx]);
      if (res) std::this_thread::sleep_for(std::chrono::microseconds(500));
      retries--;
    } while (retries > 0 && res != 0);
    idx++;
  }
  assert(!res);
  res = end();
  if (res) return BulkInsert(table, keys, values);
  return 0;
}

int DtreeClient::GetMonotonicInt(int& monot_int, const int hint){
  extern i64 GetRowidFromServer(Cid cid, i64 hint);
  i64 rowid;
  Cid cid = 0;
  i64 hint64 = (i64) hint;
  if (hint64 == 0) hint64 = 1;

  rowid = GetRowidFromServer(cid, hint64);
  if (rowid==0) return -1;
  else {
    monot_int = (int) rowid;
    return 0;
  }
}

int DtreeClient::InsertInt(const TableId& table, int i, const ValueMap& values){
  int res = begin(); assert(!res);
  i64 ikey = (i64) i;  
  int size = serialize(values);
  res = DdInsert(_table_map[table], ikey, _scratch, size);
  
  int res2 = end();
  if (!res) res = res2;
  return res;
}
