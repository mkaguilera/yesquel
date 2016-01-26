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
#include <errno.h>
#include <string.h>

#include <yajl/yajl_parse.h>

#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <fstream>
#include <iostream>
#include <sstream>
#include <streambuf>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "bench-config.h"

ConfigParser::ConfigParser (const char* path) : _dict_level(false), _path(path){}

void ConfigParser::Dump (){
  for (auto& it : _sections){
    printf("Section %s\n", it.first.c_str());
    for (auto& jt : it.second){
      if (jt.first != "fake")
        printf("\t%s = %s\n", jt.first.c_str(), jt.second.c_str());
    }
  }
}

int ConfigParser::handle_bool(int val){
  assert(_dict_level == 2);
  std::stringstream ss;
  ss << val;
  _sections[_workload][_mapkey] = ss.str();
  return 1;
}

int ConfigParser::handle_int(long long val){
  assert(_dict_level == 2);
  std::stringstream ss;
  ss << val;
  _sections[_workload][_mapkey] = ss.str();
  return 1;
}

int ConfigParser::handle_double(double val){
  assert(_dict_level == 2);
  std::stringstream ss;
  ss << val;
  _sections[_workload][_mapkey] = ss.str();
  return 1;
}

int ConfigParser::handle_string(const unsigned char* cstr, size_t len){
  assert(_dict_level == 2);
  std::string str((const char*) cstr, len);
  _sections[_workload][_mapkey] = str;
  return 1;
}

int ConfigParser::map_key(const unsigned char* cstr, size_t len){
  std::string key((const char*) cstr, len);
  if (_dict_level == 1){
    _workload = key;
    _section_names.emplace_back(key);
  } else if (_dict_level == 2){
    _mapkey = key;
  } else {
    /* _dict_level is zero */
    assert(false);
  }
  return 1;
}

int ConfigParser::start_map(){
  _dict_level++;
  assert(_dict_level <= 2);
  return 1;
}

int ConfigParser::end_map(){
  _dict_level--;
  assert(_dict_level >= 0);
  return 1;
}

void ConfigParser::Parse (){
  yajl_callbacks my_cbs;
  memset(&my_cbs, 0, sizeof my_cbs);

  // Thanks stack overflow user Tyler McHenry for this dead-easy to write
  // file read.
  std::ifstream ifs(_path);
  std::string json((std::istreambuf_iterator<char>(ifs)),
                   std::istreambuf_iterator<char>());

// Pragmas from stack overflow user Deduplicator
// The purpose of the pragmas is to silence GCC because ConfigParse is never
// virtual, and thus this conversion is safe.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpmf-conversions"
  my_cbs.yajl_string = reinterpret_cast<int (*)(void*, const unsigned char*, size_t)>(&ConfigParser::handle_string);
  my_cbs.yajl_boolean = reinterpret_cast<int (*)(void*, int)>(&ConfigParser::handle_bool);
  my_cbs.yajl_integer = reinterpret_cast<int (*)(void*, long long)>(&ConfigParser::handle_int);
  my_cbs.yajl_map_key = reinterpret_cast<int (*)(void*, const unsigned char*, size_t)>(&ConfigParser::map_key); 
  my_cbs.yajl_start_map = reinterpret_cast<int (*)(void*)>(&ConfigParser::start_map);
  my_cbs.yajl_end_map = reinterpret_cast<int (*)(void*)>(&ConfigParser::end_map);
#pragma GCC diagnostic pop

  yajl_handle handle = yajl_alloc(&my_cbs, NULL, this);
  auto st = yajl_parse(handle, (const unsigned char*) json.c_str(), json.size());
  if (st == yajl_status_ok){
    st = yajl_complete_parse(handle);
  }
  if (st != yajl_status_ok){
    // This leaks a char* pointer, but that's not a problem if there's an
    // exit immediately following.
    std::cerr << yajl_get_error(handle, 1 /* verbose = true */,
                                (const unsigned char*) json.c_str(), json.size())
              << std::endl;
    exit(EXIT_FAILURE);
  }
  yajl_free(handle);
  _section_names.sort();
}

Config::Config(const ConfigParser* cfg, const std::string& workload) 
  : _workload(workload), _conf(cfg->_sections){}
