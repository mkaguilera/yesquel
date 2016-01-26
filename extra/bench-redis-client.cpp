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
#include <ctype.h>
#include <math.h>
#include <assert.h>
#include <float.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <algorithm>
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

#include "bench-redis-client.h"
#include "bench-log.h"

static const char* _idx_name = "ycsb-index";
uint64_t MurmurHash64A (const void * key, int len);

inline double create_valid_double(const uint64_t* x){
  if (std::isfinite(*(const double *)x) && std::isnormal(*(const double *)x)){
   return *(reinterpret_cast<const double *>(x));
  } else {
   return 0.0;
  }
}

int CreateRedisClient(const std::string dbname, BenchmarkClient **clp, bool create_table){
  *clp = new RedisClient(dbname);
  return (*clp)->Init();
}

RedisClient::RedisClient(const std::string& dbname) :
    _dbname(dbname), _dbconn(nullptr){}
RedisClient::RedisClient(const char* dbname) :
    _dbname(dbname), _dbconn(nullptr){}

int RedisClient::Init(){
  static const int std_redis_port = 6379;
  std::string host;
  int portno;
  try {
    int pos = _dbname.find(":");
    if (pos >= 0){
      host = _dbname.substr(0, pos);
      std::string portstr = _dbname.substr(pos + 1);
      portno = atoi(portstr.c_str());
    } else {
      host = _dbname;
      portno = std_redis_port;
    }
  } catch (const std::out_of_range){
    printf("out of range\n");
    host = _dbname;
    portno = std_redis_port;
  }
  printf("Redis connecting to server %s port %d\n", host.c_str(), portno);
  
  _dbconn = redisConnect(host.c_str(), portno);
  return (_dbconn == nullptr) ? 1 : 0;
}

RedisClient::~RedisClient(){
  if (_dbconn) redisFree(_dbconn);
}


int RedisClient::read(const TableId& table, const Key& key, const FieldList& fields,
	   ValueMap& result){
  int rc = -1;
  size_t nCmds = fields.size() + 2;
  size_t* size_array = new size_t[nCmds];
  char** command_array = new char*[nCmds];
  int idx = 0;

  command_array[idx] = const_cast<char*>("HMGET"); size_array[idx++] = 5;

  uint64_t hash = MurmurHash64A(key.c_str(), (int) key.length());
  command_array[idx] = (char*) &hash;
  size_array[idx++] = 8;
  
  for (auto& it : fields){
    command_array[idx] = const_cast<char *>(it.c_str());
    size_array[idx++] = it.length();
  }

  const char** cmds = const_cast<const char **>(command_array);
  redisReply* reply = (redisReply *) redisCommandArgv(_dbconn, idx, cmds, size_array);
  if (reply){
    if (reply->type == REDIS_REPLY_ARRAY){
      rc = 0;
      for (auto i = 0; i < (int)reply->elements; ++i){
        result[fields[i]] = std::string(reply->element[i]->str, reply->element[i]->len);
      }
    } else {
       // This is the only other repsonse that makes sense. However, REDIS_REPLY_STATUS might
       // show up, so if this assert is triggering, consider updating the logic.
       assert(reply->type == REDIS_REPLY_ERROR);
       LOG("error in %s: %s\n", __FUNCTION__, reply->str);
    }
    freeReplyObject(reply);
  } else {
    LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
  }
  delete [] size_array;
  delete [] command_array;
  return rc;
}

int RedisClient::scan(const TableId& table, const Key& start_key, int count,
                  const FieldList& fields, std::vector<ValueMap>& result){
  int rc = -1;
  const size_t nCmds = 7;
  size_t size_array[nCmds];
  char* command_array[nCmds];
  uint64_t idx_score = MurmurHash64A((const void *) start_key.c_str(),
                                     (int) start_key.length());
  int idx = 0;
  double real_score = create_valid_double(&idx_score);
  std::stringstream sscore, scount;
  sscore << real_score;
  scount << count;
  std::string score = sscore.str();
  std::string count_string = scount.str();

  command_array[idx] = const_cast<char*>("ZRANGEBYSCORE"); size_array[idx++] = 13;
  command_array[idx] = const_cast<char *>(_idx_name);
  size_array[idx++] = strlen(_idx_name);
  command_array[idx] = const_cast<char *>(score.c_str());
  size_array[idx++] = score.length();
  command_array[idx] = const_cast<char*>("+inf"); size_array[idx++] = 4;
  command_array[idx] = const_cast<char*>("LIMIT"); size_array[idx++] = 5;
  command_array[idx] = const_cast<char*>("0"); size_array[idx++] = 1; 
  command_array[idx] = const_cast<char *>(count_string.c_str());
  size_array[idx++] = count_string.length();

  const char** cmds = const_cast<const char **>(command_array);
  redisReply* reply = (redisReply *) redisCommandArgv(_dbconn, idx, cmds, size_array);
  if (reply){
    if (reply->type == REDIS_REPLY_ARRAY){
      rc = 0;
      assert((int)reply->elements <= count);
      for (auto i = 0; i < (int)reply->elements; ++i){
        result.push_back(ValueMap());
        rc = read(table, Key(reply->element[i]->str, reply->element[i]->len),
                  fields, result.back());
        if (rc != 0)
          break;
      }
    } else {
      assert(reply->type == REDIS_REPLY_ERROR);
      LOG("error in %s: %s\n", __FUNCTION__, reply->str);
    }
    freeReplyObject(reply);
  } else {
    LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
  }
  return rc;
}

int RedisClient::auxupdate(const TableId& table, const Key& key, const ValueMap& values){
  int rc = -1;
  size_t nCmds = 2 * values.size() + 2;
  size_t* size_array = new size_t[nCmds];
  char** command_array = new char*[nCmds];
  int idx = 0;
  command_array[idx] = const_cast<char*>("HMSET"); size_array[idx++] = 5;
  
  uint64_t hash = MurmurHash64A(key.c_str(), (int) key.length());
  command_array[idx] = (char*) &hash;
  size_array[idx++] = 8;
  
  for (auto& it : values){
    command_array[idx] = const_cast<char *>(it.first.c_str());
    size_array[idx++] = it.first.length();
    command_array[idx] = const_cast<char *>(it.second.c_str());
    size_array[idx++] = it.second.length();
  }
  const char** cmds = const_cast<const char **>(command_array);
  redisReply* reply = (redisReply *) redisCommandArgv(_dbconn, idx, cmds, size_array);

  if (reply){
    if (reply->type == REDIS_REPLY_STATUS){
      rc = 0;
    } else {
      assert(reply->type == REDIS_REPLY_ERROR);
      LOG("error in %s: %s\n", __FUNCTION__, reply->str);
    }
    freeReplyObject(reply);
  } else {
    LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
  }
  delete [] size_array;
  delete [] command_array;
  return rc;
}

int RedisClient::update(const TableId& table, const Key& key, const ValueMap& values){
  int rc = -1;
  FieldList fields;
  ValueMap result;

  for (auto& it : values){
    fields.push_back(it.first);
  }
  
  rc = read(table, key, fields, result);
  if (rc) return rc;

  // apply a simple computation (convert to upper case)
  for (auto& it : result){
    std::transform(it.second.begin(), it.second.end(), it.second.begin(), toupper);
  }

  rc = auxupdate(table, key, result);
  return rc;
}

int RedisClient::insert(const TableId& table, const Key& key, const ValueMap& values){
  int rc = -1;
  // Add to index
  const size_t nCmds = 4;
  size_t size_array[nCmds];
  char* command_array[nCmds];
  uint64_t idx_score = MurmurHash64A((const void *) key.c_str(), (int) key.length());
  int idx = 0;
  double real_score = create_valid_double(&idx_score);
  std::stringstream sscore;
  sscore << real_score;
  std::string score = sscore.str();

  // First, update the index.
  command_array[idx] =const_cast<char*>( "ZADD"); size_array[idx++] = 4;
  command_array[idx] = const_cast<char *>(_idx_name);
  size_array[idx++] = strlen(_idx_name);
  command_array[idx] = const_cast<char *>(score.c_str());
  size_array[idx++] = score.length();

  uint64_t hash = MurmurHash64A(key.c_str(), (int) key.length());
  command_array[idx] = (char*) &hash;
  size_array[idx++] = 8;
  
  const char** cmds = const_cast<const char **>(command_array);
  redisReply* reply = (redisReply *) redisCommandArgv(_dbconn, idx, cmds, size_array);
  if (reply){
    if (reply->type == REDIS_REPLY_INTEGER){
      if (!(reply->integer == 0 || reply->integer == 1)){
        LOG("Wrong integer in insert: %d\n", reply->integer);
      }
      rc = 0;
    } else {
      assert(reply->type == REDIS_REPLY_ERROR);
      LOG("error in %s: %s\n", __FUNCTION__, reply->str);
    }
    freeReplyObject(reply);
  } else {
    LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
  }

  // Just use update to add the actual values.
  if (rc == 0){
    rc = auxupdate(table, key, values);
  }
  if (rc != 0){
    LOG("Just kidding, that last error was for %s\n", __FUNCTION__);
  }
  return rc;
}

// This just batches the index queries, batching the updates is more code than
// I want to write.
int RedisClient::BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values){
  int rc = -1;
  const size_t nCmds = 4;
  size_t size_array[nCmds];
  char* command_array[nCmds];
  uint64_t hash;
  for (auto& key : keys){
    uint64_t idx_score = MurmurHash64A((const void *) key.c_str(), (int) key.length());
    int idx = 0;
    double real_score = create_valid_double(&idx_score);
    std::stringstream sscore;
    sscore << real_score;
    std::string score = sscore.str();

    // First, update the index.
    command_array[idx] = const_cast<char*>("ZADD"); size_array[idx++] = 4;
    command_array[idx] = const_cast<char *>(_idx_name);
    size_array[idx++] = strlen(_idx_name);
    command_array[idx] = const_cast<char *>(score.c_str());
    size_array[idx++] = score.length();

    hash = MurmurHash64A(key.c_str(), (int) key.length());
    command_array[idx] = (char*) &hash;
    size_array[idx++] = 8;
    
    const char** cmds = const_cast<const char **>(command_array);
    rc = redisAppendCommandArgv(_dbconn, idx, cmds, size_array);
    if(rc != 0){
      return rc;
    }
  }
  for (auto& key : keys){
    redisReply* reply = nullptr;
    rc = redisGetReply(_dbconn, reinterpret_cast<void **>(&reply));
    if (reply){
      if (reply->type == REDIS_REPLY_INTEGER){
        assert(reply->integer == 0 || reply->integer == 1);
        rc = 0;
      } else {
        assert(reply->type == REDIS_REPLY_ERROR);
        LOG("error in %s: %s\n", __FUNCTION__, reply->str);
      }
      freeReplyObject(reply);
    } else {
      LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
    }
  }
  assert(keys.size() == values.size());
  for (auto i = 0; i < (int)keys.size(); ++i){
    if (0 != auxupdate(table, keys[i], values[i])){
      return 1;
    }
  }
  return 0;
}

int RedisClient::remove(const TableId& table, const Key& key){
  int rc = -1;
  const size_t nCmds = 3;
  size_t size_array[nCmds];
  char* command_array[nCmds];
  int idx = 0;
  uint64_t hash;

  // This section removes the item from the index
  command_array[idx] = const_cast<char*>("ZREM"); size_array[idx++] = 4;
  command_array[idx] = const_cast<char *>(_idx_name);
  size_array[idx++] = strlen(_idx_name);

  hash = MurmurHash64A(key.c_str(), (int) key.length());
  command_array[idx] = (char*) &hash;
  size_array[idx++] = 8;

  const char** cmds = const_cast<const char **>(command_array);
  redisReply* reply = (redisReply *) redisCommandArgv(_dbconn, idx, cmds, size_array);
  if (reply){
    if (reply->type == REDIS_REPLY_INTEGER){
      if (reply->integer == 0){
        LOG("Nothing removed in %s\n", __FUNCTION__);
      }
      rc = 0;
    } else {
      assert(reply->type == REDIS_REPLY_ERROR);
      LOG("error in %s: %s\n", __FUNCTION__, reply->str);
    }
    freeReplyObject(reply);
  } else {
    LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
  }
  if (rc != 0){
    return rc;
  }

  // This section performs the actual delete.
  idx = 0;
  command_array[idx] = const_cast<char*>("DEL"); size_array[idx++] = 3;

  hash = MurmurHash64A(key.c_str(), (int) key.length());
  command_array[idx] = (char*) &hash;
  size_array[idx++] = 8;
  
  const char** cmds2 = const_cast<const char **>(command_array);
  reply = (redisReply *) redisCommandArgv(_dbconn, idx, cmds2, size_array);
  if (reply){
    if (reply->type == REDIS_REPLY_INTEGER){
      rc = 0;
    } else {
      assert(reply->type == REDIS_REPLY_ERROR);
      LOG("error in %s: %s\n", __FUNCTION__, reply->str);
    }
    freeReplyObject(reply);
  } else {
    LOG("error in %s: %s\n", __FUNCTION__, _dbconn->errstr);
  }
  return rc;
}
