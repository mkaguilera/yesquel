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

#include <stdio.h>
#include <assert.h>
#include <stdio.h>
#include <stdarg.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <my_global.h>
#include <mysql.h>

#include "bench-client.h"
#include "bench-config.h"

#define DBNAME "wikiloadtest"
#define DBUSER "root"
#define DBPASSWD NULL

class WikiMysqlClient : public virtual BenchmarkClient {
public:
  WikiMysqlClient(const char *database, const char *confdir, bool _leader);
  WikiMysqlClient(const std::string& database, const std::string& confdir, bool _leader);
  // REQUIRED YCSB functions
  int Init();
  int read(const TableId& table, const Key& key,
	           const FieldList& fields, ValueMap& result);
  int scan(const TableId& table, const Key& start_key, int count,
	           const FieldList& fields, std::vector<ValueMap>& result){return -1;}
  int scanNodata(const TableId& table, const Key& start_key, int count,
                 const FieldList& fields, std::vector<ValueMap>& result){return -1;}
  int update(const TableId& table, const Key& key, const ValueMap& values){return -1;}
  int insert(const TableId& table, const Key& key, const ValueMap& values);
  int remove(const TableId& table, const Key& key){return -1;}

  ~WikiMysqlClient ();
  int BulkInsert(const TableId& table, const std::vector<Key>& keys,
    const std::vector<ValueMap>& values) {return 0;}
  int browser_cache_read(int seed);
  int memcache_read(int seed);
  int database_read(int seed);

  MYSQL_STMT*	getStatement(const char* query_str) {return _stmt_map[query_str];}
  void		setStatement(const char* query_str, MYSQL_STMT* stmt) {_stmt_map[query_str] = stmt;}
private:
  int execute_sql(MYSQL_STMT* stmt, const char* caller);
  MYSQL_STMT* construct_sql_insert(const TableId& table, const Key& key, const ValueMap& values);
  int do_query(const char* query_str, int nCols, const std::vector<std::string>& params,
               const std::set<int>& fetch_cols, std::vector<std::string>& cols);
  const std::string _dbname;
  const std::string _confdir;
  MYSQL* _dbconn;
  std::map<const std::string, MYSQL_STMT*> _stmt_map;

  // Data management
  static const std::set<std::string> threadSafeGetCategories(const std::string& page_title);
  static const std::set<std::string> threadSafeGetImages(const std::string& page_title);
  static const std::set<std::string> threadSafeGetStubs(const std::string& page_title);
  static const std::set<std::string> threadSafeGetLinks(const std::string& page_title);
  
  void load_data();

  static std::unordered_map<std::string, std::set<std::string> > _category_map;
  static std::unordered_map<std::string, std::set<std::string> > _link_map;
  static std::unordered_map<std::string, std::set<std::string> > _stub_map;
  static std::unordered_map<std::string, std::set<std::string> > _image_map;
  static std::mutex _category_load_lock;
  static std::vector<std::string> _page_titles;
  const bool _leader;
};

int CreateWikiMysqlClient(const char* conf_str, const char* confdir, BenchmarkClient** clp, bool leader);
