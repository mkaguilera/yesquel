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
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

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

#include "bench-log.h"

static std::recursive_mutex log_mutex;
static FILE* logfile = stdout;
bool init = false;
bool add_timestamp = true;

void StartBulkLog(){
  log_mutex.lock();
  add_timestamp = false;
}

void EndBulkLog(){
  add_timestamp = true;
  log_mutex.unlock();
}

void FlushLog(){
  fflush(logfile);
}

void SetLog(const char* full_path){
  std::lock_guard<std::recursive_mutex> lock(log_mutex);
  //std::string out;
  //out.append(full_path);
  //out.append(".out.txt");
  if (0 != fflush(logfile)){
    fprintf(stderr, "Error flusing log file\n");
  }
  if (logfile != stdout && 0 != fclose(logfile)){
    fprintf(stderr, "Error closing log file\n");
  }
  logfile = fopen(full_path, "w");
  if (!logfile){
    fprintf(stderr, "Error opening %s, falling back to stdout\n", full_path);
    logfile = stdout;
  }
  printf("set log file to %s\n", full_path);
}

char buf[64];
time_t tt;

void LOG(const char* fmt, ...){
  std::lock_guard<std::recursive_mutex> lock(log_mutex);
  if (add_timestamp){
    tt = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    ctime_r(&tt, buf);
    buf[strlen(buf) - 1] = '\0';
    fprintf(logfile, "%s: ", buf);
  }
  va_list myargs;
  va_start(myargs, fmt);
  vfprintf(logfile, fmt, myargs);
  va_end(myargs);
  fflush(logfile);
}
