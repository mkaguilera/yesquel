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

#include <assert.h>
#include <list>
#include <map>
#include <string>
#include <stdio.h>
#include <sstream>

typedef std::map<std::string,std::map<std::string,std::string>> ConfigMap;

class ConfigParser {
public:
  ConfigParser(const char* path);

  void Parse();
  void Dump();

   ConfigMap _sections;
   std::list<std::string> _section_names;


private:
  // Functions and state for parsing
  int handle_bool(int val);
  int handle_int(long long val);
  int handle_double(double val);
  int handle_string(const unsigned char* str, size_t len);
  int map_key(const unsigned char* str, size_t len);
  int start_map();
  int end_map();

  int _dict_level;
  const char* _path;
  std::string _workload;
  std::string _mapkey;
};

class Config {
public:
  Config(const ConfigParser* cfg, const std::string& workload);

  template<typename T>
  T get(const std::string& key, T defaultval) const;

  const std::string& _workload;
private:
  const ConfigMap& _conf;
};

template<typename T>
T
Config::get(const std::string& key, T defaultval) const {
  T rval;
  auto workload = _conf.find(_workload);
  assert(workload != _conf.cend());
  auto entry = workload->second.find(key);
  if (entry != workload->second.cend()) {
    std::stringstream ss(entry->second.c_str());
    ss >> rval;
  } else {
    auto common = _conf.find("Common");
    auto option = common->second.find(key);
    if (option != common->second.cend()) {
      std::stringstream ss(option->second.c_str());
      ss >> rval;
    } else {
      rval = defaultval;
    }
  }
  return rval;
}
