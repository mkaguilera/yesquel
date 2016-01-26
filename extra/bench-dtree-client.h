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

#pragma once

#include "bench-client.h"
#include "treedirect.h"

#include <unordered_map>

class DtreeClient : public virtual BenchmarkClient {
public:
  DtreeClient(const char *database, bool create=false);
  DtreeClient(const std::string& database, bool create=false);
  int Init();
  ~DtreeClient();
  
  int read(const TableId& table, const Key& key, const FieldList& fields,
	   ValueMap& result);
  int scan(const TableId& table, const Key& start_key, int count,
	   const FieldList& fields, std::vector<ValueMap>& result);
  int scanNodata(const TableId& table, const Key& start_key, int count,
                 const FieldList& fields, std::vector<ValueMap>& result);
  int update(const TableId& table, const Key& key, const ValueMap& values);
  int insert(const TableId& table, const Key& key, const ValueMap& values);
  int remove(const TableId& table, const Key& key);
  int begin();
  int complete() {return end();}
  int BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values);
  int GetMonotonicInt(int& monot_int, const int hint);
  int InsertInt(const TableId& table, int i, const ValueMap& values);
  

private:
  int serialize(const ValueMap& values);
  int deserialize(ValueMap& values);

  int end();

  // Serliaze and deserialze ValueMap to/from scratch space)
  int copy_to_scratch(const void* ptr, i32 size);
  void reset_scratch() {_curr = _scratch;}
  int read_int();
  const std::string read_string();

  friend void scan_helper(i64 key, const char *data, int len, int n, bool eof, void* arg);
  friend int updatecallback(char *buf, int len, void *arg);


  const std::string _dbname;
  const bool _create;
  DdConnection* _dbhandle;
  char* _curr;
  char _scratch[4096 * 4];

  int _txcount;
  bool _should_abort;

  std::unordered_map<TableId,DdTable*> _table_map;
};

int CreateDtreeClient(const std::string dbname, BenchmarkClient** clp, bool create_table);
