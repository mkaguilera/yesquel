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

#include <hiredis/hiredis.h>
#include "bench-client.h"

class RedisClient : public virtual BenchmarkClient {
public:
  RedisClient(const char *database);
  RedisClient(const std::string& database);
  int Init();
  ~RedisClient();
  
  int read(const TableId& table, const Key& key, const FieldList& fields,
	   ValueMap& result);
  int scan(const TableId& table, const Key& start_key, int count,
	   const FieldList& fields, std::vector<ValueMap>& result);
  int scanNodata(const TableId& table, const Key& start_key, int count,
                 const FieldList& fields, std::vector<ValueMap>& result){return -1;}
  int update(const TableId& table, const Key& key, const ValueMap& values);
  int insert(const TableId& table, const Key& key, const ValueMap& values);
  int remove(const TableId& table, const Key& key);
  int BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values);

private:
  const std::string _dbname;
  redisContext* _dbconn;
  int auxupdate(const TableId& table, const Key& key, const ValueMap& values);
};

int CreateRedisClient(const std::string dbname, BenchmarkClient** clp, bool create_table);
