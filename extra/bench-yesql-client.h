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

#include <string>

#include "bench-client.h"
#include "sqlite3.h"

class YesqlClient : public virtual BenchmarkClient {
public:
  YesqlClient(const char *database, bool create=false);
  YesqlClient(const std::string& database, bool create=false);
  int Init();
  ~YesqlClient();
  
  int read(const TableId& table, const Key& key, const FieldList& fields,
	   ValueMap& result);
  int scan(const TableId& table, const Key& start_key, int count,
	   const FieldList& fields, std::vector<ValueMap>& result);
  int scanNodata(const TableId& table, const Key& start_key, int count,
                 const FieldList& fields, std::vector<ValueMap>& result){return -1;}
  int update(const TableId& table, const Key& key, const ValueMap& values);
  int insert(const TableId& table, const Key& key, const ValueMap& values);
  int remove(const TableId& table, const Key& key);
  int begin(void);
  int complete(void);
  int BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values);

private:
  // SQL constructors
  sqlite3_stmt* construct_sql_read(const TableId& table, const Key& key, const FieldList& fields);
  sqlite3_stmt* construct_sql_update(const TableId& table, const Key& key, const ValueMap& values);
  sqlite3_stmt* construct_sql_insert(const TableId& table, const Key& key, const ValueMap& values);
  sqlite3_stmt* construct_sql_scan(const TableId& table, const Key& start_key, int count, const FieldList& fields);
  sqlite3_stmt* construct_sql_remove(const TableId& table, const Key& key);

  // Other helper functions
  int stmt_cache(int index, const char *sql, sqlite3_stmt **ret);
  int read_result_row(sqlite3_stmt* stmt, const FieldList& fields, ValueMap& result);
  int execute_sql(sqlite3_stmt* stmt, const char* caller);

  const std::string _dbname;
  const bool _create;
  sqlite3 *_dbhandle;
  bool _should_abort;
};

int CreateYesqlClient(const std::string dbname, BenchmarkClient** clp, bool create_table);
