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
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <values.h>

#include <netdb.h>
#include <sys/socket.h>

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
#include <algorithm>

#include "bench-config.h"
#include "bench-log.h"
#include "bench-runner.h"
#include "bench-sql.h"


#define Sleep(X) usleep(X * 1000)

uint64_t MurmurHash64A ( const void * key, int len);
int optSyncPost=0;
int optSyncWait=0;
int optClientno=0;
char *optServerPort=0;

char *usage = "%s: [-pw] [-s servername[:portno]] <config> <clientno>\n"
              "    -p: create synclistpost when ready to start\n"
              "    -w: if cliearno==0 (leader), wait for existence of syncfilewait\n"
              "    -s servername[:portno]: name and port of server to contact\n";

int handleOpts(int argc, char *argv[]){
  int c;
  int badargs=0;

  while ((c = getopt(argc, argv, "pws:")) != -1){
    switch(c){
    case 'p':
      optSyncPost=1;
      break;
    case 's':
      optServerPort = strdup(optarg);
      break;
    case 'w':
      optSyncWait=1;
      break;
    default:
      ++badargs;
    }
  }
  return optind;
}



// This clock is adapted from stack overflow user Mateusz Pusz
class windows_clock {
public:
  typedef std::chrono::nanoseconds duration;
  typedef duration::rep rep;
  typedef duration::period period;
  typedef std::chrono::time_point<windows_clock, duration> time_point;
  static bool is_steady;

  static time_point now(){
      return time_point(duration(std::chrono::high_resolution_clock::now().time_since_epoch()));
  }

private:
  static bool _initialized;
  static bool _successful_init;

  static bool init(){
    /*
    if (QueryPerformanceFrequency(&_freq) == 0){
      LOG("%s\n", GetLastError());
      LOG("Failed to initialize QueryPerformanceCounter, falling back to C++11 clock\n");
      return false;
    }
    LOG("Successfully initialized windows clock\n");
    */
    return true;
  }
};

bool windows_clock::is_steady = true;
bool windows_clock::_initialized = false;
bool windows_clock::_successful_init= false;


// Write access to this vector is protected by a call to std::call_once. DO NOT UPDATE OUTSIDE
// OF THAT FUNCTION WITHOUT ADDING LOCKS.
static std::vector<Key> keys_;
Key smallestkey_;

class RandomWrapper {
 public:
   RandomWrapper () : _field_base("FIELD"),
                      _valid_chars("abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "1234567890!@#$%^&*()-=[]{}"),
                                   _character_distribution(0,(int)strlen(_valid_chars) - 1),
                      _indices(_n_fields)
  {
     for (auto i = 0; i < _n_fields; ++i){
       std::stringstream field;
       field << _field_base << i;
       _fields.push_back(field.str());
     }
     std::iota(_indices.begin(), _indices.end(), 0);
   }

   ~RandomWrapper (){}

   std::string GenerateRandomString(int len);
   void InitializeRandomFieldList(FieldList& list);
   const std::string &PickRandomField(int curr_index);
   int GetRandomInt(int min, int max);
   void SetSeed(unsigned seed){_generator.seed(seed);}
  
protected:
   std::default_random_engine _generator;

private:
   friend class ExperimentState;
   static const int _n_fields = 1;
   const std::string _field_base;
   const char* _valid_chars;

   std::uniform_int_distribution<int> _character_distribution;
   std::vector<std::string> _fields;
   std::vector<unsigned int> _indices;
   RandomWrapper(const RandomWrapper&);
   RandomWrapper& operator=(const RandomWrapper);
};

class ZipfianGenerator {
public:
  ZipfianGenerator(int nItems, double order) :
    _distribution(0.0, 1.0), _curr_number(0.0), _count(1), _order(order),
    _discretized_cdf(nItems, 0.0), _uniform_distribution(0, nItems - 1){}

  // Formula for calculating Zipfian CDF from Wikipedia
  void Init(){
    if (_order == 0.0){
      LOG("Shape is 0.0, using uniform distribution\n");
      return;
    } else {
      LOG("Shape is %f, using zipfian distribution\n", _order);
    }
    for (size_t i = 0; i < _discretized_cdf.size(); ++i){
      _discretized_cdf[i] = getNextGeneralizedHarmonicNumber();
    }
    for (size_t i = 0; i < _discretized_cdf.size(); ++i){
      _discretized_cdf[i] /= _discretized_cdf[_discretized_cdf.size() - 1];
    }
  }

  // Perform binary search to find the highest index less than a random (0.0, 1.0).
  // This effective "inverts" the CDF, giving us the target distribution.
  int GetIndex(std::default_random_engine& engine){
    if (_order == 0.0) return _uniform_distribution(engine);

    double p = _distribution(engine); 
    auto pos = std::lower_bound(_discretized_cdf.cbegin(), _discretized_cdf.cend(), p);
    return static_cast<int>(std::distance(_discretized_cdf.cbegin(), pos));
  }

  void print(){
    for (auto i = _discretized_cdf.cbegin(); i != _discretized_cdf.cend(); ++i){
      LOG("%lld: %f\n", std::distance(_discretized_cdf.cbegin(), i), *i);
    }
  }
  
private:
  // Formula for calucalting GHN from wikipedia
  double getNextGeneralizedHarmonicNumber(){
    _curr_number += 1 / (pow(double(_count++), _order));
    return _curr_number;
  }

  std::uniform_real_distribution<double> _distribution;
  double _curr_number;
  int _count;
  const double _order;
  std::vector<double> _discretized_cdf;
  std::uniform_int_distribution<int> _uniform_distribution;
};


typedef std::chrono::time_point<windows_clock> Timepoint;

struct Parameters;

class ExperimentState {
public:
  ExperimentState(std::shared_ptr<ZipfianGenerator> zipf, Parameters* p) :
    _workerno(0),
    _zipf(zipf),
    _start_time(windows_clock::now()),
    _params(p),
    _real_start(std::chrono::system_clock::now()){}

  int getZipfIdx(){
    int id = _zipf->GetIndex(_wrapper._generator);
    return id;
  }

  void seedRandom(unsigned u){
    _wrapper._generator.seed(u);
  }

  void setWorkerno(int w){
    _workerno = w;
  }

  int getWorkerno(){
    return _workerno;
  }

  RandomWrapper& getRandom(){
    return _wrapper;
  }

  void RecordTime(long long nanoseconds, const char* function);

  void PrintTimes();

private:
  ExperimentState(const ExperimentState&);
  ExperimentState& operator=(const ExperimentState&);

  
  long long get_interval(){
    return std::chrono::duration_cast<std::chrono::seconds>(windows_clock::now() - _start_time).count();
  }

  int _workerno;
  std::shared_ptr<ZipfianGenerator> _zipf;
  RandomWrapper    _wrapper;
  Timepoint _start_time;
  Parameters* _params;
  std::chrono::time_point<std::chrono::system_clock> _real_start;
  std::unordered_map<const char*,std::map<long long, int>> _ops_time_map;
  std::unordered_map<const char*,std::map<long long, int>> _ops_count_map;
};

class Timer {
public:
  Timer(ExperimentState& es, const char* place) :
    _start(windows_clock::now()), _es(es),
    _loc(place), _record(true){ 
  }

  void Cancel(){
    _record = false;
  }

  ~Timer(){
    if (_record){
    _end = windows_clock::now();
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(_end - _start);
    _es.RecordTime(d.count(), _loc);
    }
  }

private:
  Timepoint _start, _end;
  ExperimentState& _es;
  const char* _loc;
  bool _record;
};

struct Parameters {
  Parameters(const Config* conf){
    nTuples = conf->get<int>("tuples", 10000);
    max_fields = conf->get<int>("max_fields", 1);
    key_len = conf->get<int>("key_len", 256);
    value_len = conf->get<int>("value_len", 512);
    duration = conf->get<int>("duration", 120);
    shape = conf->get<double>("shape", 0.0);
    load = conf->get<bool>("load", true);
    sync = conf->get<bool>("sync", true);
    scan_max = conf->get<int>("max_scan", 5);
    lead_time = conf->get<int>("lead-time", 120);
    syncfilepost = conf->get<std::string>("syncfilepost", ""); 
    syncfilewait = conf->get<std::string>("syncfilewait", ""); 
    warmup = conf->get<int>("warmup", 15);
    cooldown = conf->get<int>("cooldown", 15);
    txn_ops = conf->get<int>("txn_ops", 4);
    seed = conf->get<int>("seed", 1012013);
    wiki_mix = conf->get<int>("wiki-mix", 95);
  }
    
  int nTuples;
  int max_fields;
  int key_len;
  int scan_max;
  int value_len;
  int duration;
  unsigned seed;
  double shape;
  const char* key_file;
  bool load;
  bool sync;
  int lead_time;
  std::string syncfilepost;
  std::string syncfilewait;
  int warmup;
  int cooldown;
  int txn_ops;
  int wiki_mix;
};

int do_zipfian_test(ExperimentState& es){
  {
    ZipfianGenerator zg(1000, 2);
    Timer t(es, "1000");
    zg.Init();
  }
  {
    ZipfianGenerator zg(10000, 2);
    Timer t(es, "10000");
    zg.Init();
  }
  {
    ZipfianGenerator zg(100, 2);
    Timer t(es, "100");
    zg.Init();
  }
  {
    ZipfianGenerator zg(100000, 2);
    Timer t(es, "100000");
    zg.Init();
  }
  ZipfianGenerator zg(10000000, 0.5);
  {
    Timer t(es, "ten-million-gen");
    zg.Init();
  }
  std::unordered_map<int,int> histogram;
  {
    std::default_random_engine engine;
    Timer t(es, "one-million-sample-and-set");
    for (auto i = 0; i < 1000000; ++i){
      int j = zg.GetIndex(engine);
      if (histogram.find(j) != histogram.end()){
        histogram[j]++;
      } else {
        histogram[j] = 1;
      }
    }
  }
  for (auto i = 0; i < 100; ++i){
    LOG("%d: %f\n", i, histogram[i]/double(1000000));
  }
  es.PrintTimes();
  return 0;
}

void ExperimentState::RecordTime(long long nanoseconds, const char* function){
  auto now = std::chrono::system_clock::now();
  auto since_start = std::chrono::duration_cast<std::chrono::seconds>(now - _real_start).count();
  if (since_start < _params->warmup || since_start > (_params->duration - _params->cooldown))
    return;
  // Bucket into 100s of microseconds
  long long delay_key = nanoseconds/(1000 * 100);
  auto it = _ops_time_map[function].find(delay_key);
  if (it != _ops_time_map[function].end()){
    _ops_time_map[function][delay_key]++;
  } else {
    _ops_time_map[function][delay_key] = 1;
  }

  long long interval_key = get_interval();
  auto it2 = _ops_count_map[function].find(interval_key);
  if (it2 != _ops_count_map[function].end()){
    _ops_count_map[function][interval_key]++;
  } else {
    _ops_count_map[function][interval_key] = 1;
  }
}

void ExperimentState::PrintTimes(){
  StartBulkLog();
  LOG("===================== Request latency ====================\n");
  for (auto& it : _ops_time_map){
    for (auto& it2 : it.second){
      // Was bucketed into 100s of microseconds, dividing by 10 will
      // yield milliseconds
      double this_start = it2.first/10.;
      double this_end = this_start + 1/10.;
      LOG("%s: [%0.2f -- %0.2f ms]: %d\n",
                       it.first, this_start, this_end, it2.second);
    }
    LOG("\n");
  }
  LOG("\n\n");
  LOG("===================== Throughput ====================\n");
  for (auto& it : _ops_count_map){
    for (auto& it2 : it.second){
      int start = (int)(it2.first + std::chrono::duration_cast<std::chrono::seconds>(_real_start.time_since_epoch()).count());
      int end = start + 1;
      LOG("%s: [%d -- %d]: %d\n",
                       it.first,
                       start, end, it2.second);
    }
    LOG("\n");
  }
  EndBulkLog();
}

// Non-inclusive for the last int.
inline int RandomWrapper::GetRandomInt(int min, int max){
  return std::uniform_int_distribution<int>(min, max - 1)(_generator);
}

const std::string &RandomWrapper::PickRandomField(int curr_index){
  /*
  if (curr_index == 0){
    std::random_shuffle(_indices.begin(), _indices.end());
  }
  */
  static const std::string& field("FIELD0");
  
  return field;
}

std::string RandomWrapper::GenerateRandomString(int len){
  std::string st;
  st.reserve(len);
  auto gen_char = [this](){
    return _valid_chars[_character_distribution(_generator)];
  };
  while (len-- > 0){
    st += gen_char();
  }
  return st;
}

static int do_insert(ExperimentState& state, ClientPtr clp, int max_fields,
          int key_len, int value_len, std::vector<Key>* keys=NULL){
  ValueMap values;
  Key k(state.getRandom().GenerateRandomString(key_len));
  if (keys) keys->push_back(k);
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    values[state.getRandom().PickRandomField(i)] =
            state.getRandom().GenerateRandomString(value_len);
  }
  Timer t(state, __FUNCTION__);
  int rc = clp->insert(TableId(TABLENAME), k, values);
  if (rc) t.Cancel();
  return rc;
}

static int do_read(ExperimentState& state, ClientPtr clp, int max_fields, Key key){
  FieldList fields;
  ValueMap result;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    fields.push_back(state.getRandom().PickRandomField(i));
  }
  Timer t(state, __FUNCTION__);
  int rc = clp->read(TableId(TABLENAME), key, fields, result);
  if (rc) t.Cancel();
  return rc;
}

static int do_scan(ExperimentState& state, ClientPtr clp, int max_fields, int nRows, Key key, bool getdata){
  FieldList fields;
  std::vector<ValueMap> result;
  int rc;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    fields.push_back(state.getRandom().PickRandomField(i));
  }
  Timer t(state, __FUNCTION__);
  if (getdata) rc = clp->scan(TableId(TABLENAME), key, nRows, fields, result);
  else rc = clp->scanNodata(TableId(TABLENAME), key, nRows, fields, result);
  if (rc) t.Cancel();
  return rc;
}

static int do_scan_update(ExperimentState& state, ClientPtr clp, int max_fields, int nRows, Key key1, Key key2, int value_len, bool getdata){
  FieldList fields;
  std::vector<ValueMap> result;
  int rc;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  
  for (auto i = 0; i < nFields; ++i){
    fields.push_back(state.getRandom().PickRandomField(i));
  }
  Timer t(state, __FUNCTION__);
  if (getdata) rc = clp->scan(TableId(TABLENAME), key1, nRows, fields, result);
  else rc = clp->scanNodata(TableId(TABLENAME), key2, nRows, fields, result);

  if (!rc){
    ValueMap towrite;
    int nFieldswrite = state.getRandom().GetRandomInt(1, max_fields + 1);
    for (auto i = 0; i < nFieldswrite; ++i){
      towrite[state.getRandom().PickRandomField(i)] =
              state.getRandom().GenerateRandomString(value_len);
    }
    rc  = clp->insert(TableId(TABLENAME), key2, towrite);
  }
  
  if (rc) t.Cancel();
  return rc;
}

static int do_update(ExperimentState& state, ClientPtr clp, int max_fields, int value_len,
          Key key){
  ValueMap values;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    values[state.getRandom().PickRandomField(i)] =
            state.getRandom().GenerateRandomString(value_len);
  }
  Timer t(state, __FUNCTION__);
  int rc = clp->update(TableId(TABLENAME), key, values);
  if (rc) t.Cancel();
  return rc;
}

static int do_remove(ExperimentState& state, ClientPtr clp, Key key){
  Timer t(state, __FUNCTION__);
  int rc = clp->remove(TableId(TABLENAME), key);
  if (rc) t.Cancel();
  return rc;
}

static int do_bcache_read(ExperimentState& state, ClientPtr clp, int seed){
  Timer t(state, __FUNCTION__);
  int rc = clp->browser_cache_read(seed);
  if (rc) t.Cancel();
  return rc;
}

static int do_database_read(ExperimentState& state, ClientPtr clp, int seed){
  Timer t(state, __FUNCTION__);
  int rc = clp->database_read(seed);
  if (rc) t.Cancel();
  return rc;
}

static int read_wrapper(ClientPtr clp, const Key* key, const FieldList* fields, ValueMap* values){
  return clp->read(TableId(TABLENAME), *key, *fields, *values);
}

static int insert_wrapper(ClientPtr clp, const Key* key, const ValueMap* values){
  return clp->insert(TableId(TABLENAME), *key, *values);
}

static int update_wrapper(ClientPtr clp, const Key* key, const ValueMap* values){
  return clp->update(TableId(TABLENAME), *key, *values);
}

static int scan_wrapper(ClientPtr clp, const Key* key, int nRows, const FieldList* fields, std::vector<ValueMap>* values){
  return clp->scan(TableId(TABLENAME), *key, nRows, *fields, *values);
}

static int remove_wrapper(ClientPtr clp, const Key* key){
  return clp->remove(TableId(TABLENAME), *key);
}

static int do_txn(ExperimentState& state, ClientPtr clp, Parameters& param){
  int rc = -1;
  // read params
  int max_fields = param.max_fields;
  int value_len = param.value_len;

  // initialize methods/params
  ValueMap update_values;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    update_values[state.getRandom().PickRandomField(i)] =
                  state.getRandom().GenerateRandomString(value_len);
  }

  ValueMap insert_values;
  nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    insert_values[state.getRandom().PickRandomField(i)] =
                  state.getRandom().GenerateRandomString(value_len);
  }

  FieldList scan_fields;
  std::vector<ValueMap> scan_result;
  nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    scan_fields.push_back(state.getRandom().PickRandomField(i));
  }

  FieldList read_fields;
  ValueMap read_result;
  nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    read_fields.push_back(state.getRandom().PickRandomField(i));
  }

  // Some hacks follow.
  enum ops {
    READ,
    UPDATE,
    INSERT,
    SCAN,
    REMOVE
  };

  auto read_op = std::bind(read_wrapper, clp, std::placeholders::_1, &read_fields, &read_result);
  auto scan_op = std::bind(scan_wrapper, clp, std::placeholders::_1, param.scan_max, &scan_fields, &scan_result);
  auto insert_op = std::bind(insert_wrapper, clp, std::placeholders::_1, &insert_values);
  auto update_op = std::bind(update_wrapper, clp, std::placeholders::_1, &update_values);
  auto remove_op = std::bind(remove_wrapper, clp, std::placeholders::_1);

  std::vector<ops> ops;
  std::vector<Key> keys;
  for (auto i = 0; i < param.txn_ops; ++i){
    ops.push_back((enum ops)state.getRandom().GetRandomInt(READ, UPDATE+1));
    if (ops[i] != UPDATE  && ops[i] != READ){
      keys.push_back(state.getRandom().GenerateRandomString(param.key_len));
    } else {
      keys.push_back(keys_[state.getZipfIdx()]);
    }
  }

  // Actual logic for do_txn
  Timer t(state, __FUNCTION__);
  rc = clp->begin();
  if (!rc){
    for (auto i = 0; i < param.txn_ops; ++i){
      switch (ops[i]){
      case INSERT:
        rc = insert_op(&keys[i]);
        break;
      case UPDATE:
        rc = update_op(&keys[i]);
        break;
      case READ:
        rc = read_op(&keys[i]);
        break;
      case SCAN:
        rc = scan_op(&keys[i]);
        break;
      case REMOVE:
        rc = remove_op(&keys[i]);
        break;
      default:
        LOG("Fell through in do_txn");
        rc = -1;
        assert(false);
      }
      if (rc) break;
    }
  }
  int commit_rc = clp->complete();
  if (rc || commit_rc) t.Cancel();
  return rc;
}

static int do_txl(ExperimentState& state, ClientPtr clp, int max_fields, int nRows, Key key1, Key key2){
  FieldList fields;
  ValueMap result1;
  ValueMap result2;
  int rc;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    fields.push_back(state.getRandom().PickRandomField(i));
  }
  Timer t(state, __FUNCTION__);
  rc = clp->begin();
  if (!rc){
    rc = clp->read(TableId(TABLENAME), key1, fields, result1);
    if (!rc){
      rc = clp->read(TableId(TABLENAME), key2, fields, result2);
    }
  }
  int commit_rc = clp->complete();
  if (rc || commit_rc) t.Cancel();
  return rc;
}

static int do_txm(ExperimentState& state, ClientPtr clp, int max_fields, int nRows, Key key1,
       std::vector<Key> &keys, int nkeys, int value_len){
  FieldList fields;
  ValueMap result1;
  ValueMap towrite;
  Key key2;
  int rc;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    fields.push_back(state.getRandom().PickRandomField(i));
  }
  Timer t(state, __FUNCTION__);
  rc = clp->begin();
  if (!rc){
    rc = clp->read(TableId(TABLENAME), key1, fields, result1);
    if (!rc){
      int nFieldswrite = state.getRandom().GetRandomInt(1, max_fields + 1);
      for (auto i = 0; i < nFieldswrite; ++i){
        towrite[state.getRandom().PickRandomField(i)] =
                state.getRandom().GenerateRandomString(value_len);
      }

      std::string s = result1[state.getRandom().PickRandomField(0)];
      key2 = keys[MurmurHash64A(s.c_str(), s.length()) % nkeys];
      rc  = clp->insert(TableId(TABLENAME), key2, towrite);
    }
  }
  int commit_rc = clp->complete();
  if (rc || commit_rc) t.Cancel();
  return rc;
}

static int do_monot_insert(ExperimentState& state, ClientPtr clp, int firstkey, int max_fields, int value_len){
  ValueMap values;
  int nFields = state.getRandom().GetRandomInt(1, max_fields + 1);
  for (auto i = 0; i < nFields; ++i){
    values[state.getRandom().PickRandomField(i)] =
            state.getRandom().GenerateRandomString(value_len);
  }
  int rc, ikey;

  rc = clp->GetMonotonicInt(ikey, firstkey);
  //ikey = MAXINT - ikey; // this is to insert in reverse order
  assert(!rc);
  Timer t(state, __FUNCTION__);
  rc = clp->InsertInt(TableId(TABLENAME), ikey, values);
  if (rc) t.Cancel();
  return rc;
}

// update (read-modify-write)
static int do_workload_a(ClientPtr clp, ExperimentState& st, Parameters& param){
  std::stringstream s;
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    int idx = st.getZipfIdx();
    Key key = keys_[idx];
    //if (st.getRandom().GetRandomInt(0, 2) == 1){
    //  do_read(st, clp, param.max_fields, key);
    //} else {
    do_update(st, clp, param.max_fields, param.value_len, key);
    //}
  }
  return 0;
}

// read (95%)+update(5%)
static int do_workload_b(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key = keys_[st.getZipfIdx()];
    if (st.getRandom().GetRandomInt(0,100) < 95){
      do_read(st, clp, param.max_fields, key);
    } else {
      do_update(st, clp, param.max_fields, param.value_len, key);
    }
  }
  return 0;
}

// read
static int do_workload_c(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key = keys_[st.getZipfIdx()];
    do_read(st, clp, param.max_fields, key);
  }
  return 0;
}

// scan(95%)+update(5%)
static int do_workload_e(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key = keys_[st.getZipfIdx()];
    if (st.getRandom().GetRandomInt(0,100) < 95){
      int nRows = st.getRandom().GetRandomInt(1, param.scan_max);
      do_scan(st, clp, param.max_fields, nRows, key, true);
    } else {
      do_update(st, clp, param.max_fields, param.value_len, key);
    }
  }
  return 0;
}

// random inserts
static int do_workload_f(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    do_insert(st, clp, param.max_fields, param.key_len, param.value_len);
  }
  return 0;

}

// monotonic inserts
static int do_workload_g(ClientPtr clp, ExperimentState& st, Parameters& param, int firstkey){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    do_monot_insert(st, clp, firstkey, param.max_fields, param.value_len);
  }
  return 0;
}

// transactions
static int do_workload_h(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    do_txn(st, clp, param);
  }
  return 0;
}

// monotonic inserts with more and more clients. First comes at time 15s,
// then another comes for every next 10s. Assumes one thread per client.
static int do_workload_i(ClientPtr clp, ExperimentState& st, Parameters& param, int firstkey){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);
  int currenttime;
  int myactivetime = 15 + optClientno*10;

  while ((currenttime = std::chrono::duration_cast<std::chrono::seconds>(now() - start).count()) < param.duration){
    if (currenttime >= myactivetime){
      do_monot_insert(st, clp, firstkey, param.max_fields, param.value_len);
    } else {
      Sleep(10); // wait 10ms
    }
  }
  return 0;
}

// clients are running updates, and then at time 40s, one scan occurs
static int do_workload_j(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  if (optClientno==0){
    int currenttime;
    int myactivetime = 40;
    int nRows = 500;
    Key key = smallestkey_;
    while ((currenttime = std::chrono::duration_cast<std::chrono::seconds>(now() - start).count()) < param.duration){
      if (st.getWorkerno() == 0){
        if (currenttime >= myactivetime){
          do_scan(st, clp, 1, nRows, key, true);
        } else {
          Sleep(10); // wait 10ms
        } // if
      } else {
        Sleep(500); // for first client, only first worker does anything
      }
    } // while
  } else {
    while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
      int idx = st.getZipfIdx();
      Key key = keys_[idx];
      do_update(st, clp, param.max_fields, param.value_len, key);
    }
  }
  return 0;
}

// scan of key, no data
static int do_workload_k(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key = keys_[st.getZipfIdx()];
    int nRows = st.getRandom().GetRandomInt(1, param.scan_max);
    do_scan(st, clp, param.max_fields, nRows, key, false);
  }
  return 0;
}

// transaction type L: read two random keys
static int do_workload_l(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key1 = keys_[st.getZipfIdx()];
    Key key2 = keys_[st.getZipfIdx()];
    int nRows = st.getRandom().GetRandomInt(1, param.scan_max);
    do_txl(st, clp, param.max_fields, nRows, key1, key2);
  }
  return 0;
}

// transaction type M: read random key, determine a location to write from it,
//  write that key
static int do_workload_m(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key = keys_[st.getZipfIdx()];
    int nRows = st.getRandom().GetRandomInt(1, param.scan_max);
    do_txm(st, clp, param.max_fields, nRows, key, keys_, param.nTuples, param.value_len);
  }
  return 0;
}

// scan of key with data and fixed number of rows
static int do_workload_n(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key = keys_[st.getZipfIdx()];
    int nRows = param.scan_max;
    do_scan(st, clp, param.max_fields, nRows, key, true);
  }
  return 0;
}

// scan of key with data and fixed number of rows, plus a write at the end
static int do_workload_o(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    Key key1 = keys_[st.getZipfIdx()];
    Key key2 = keys_[st.getZipfIdx()];
    int nRows = param.scan_max;
    do_scan_update(st, clp, param.max_fields, nRows, key1, key2, param.value_len, true);
  }
  return 0;
}


// wikipedia workload
static int do_workload_w(ClientPtr clp, ExperimentState& st, Parameters& param){
  auto start = std::chrono::system_clock::now();
  auto now = std::bind(std::chrono::system_clock::now);

  while (std::chrono::duration_cast<std::chrono::seconds>(now() - start).count() < param.duration){
    int seed = st.getRandom().GetRandomInt(0, INT_MAX);
    if (st.getRandom().GetRandomInt(0,100) < param.wiki_mix){
      do_bcache_read(st, clp, seed);
    } else {
      do_database_read(st, clp, seed);
    }
  }
  return 0;
}

static int do_simple_test(ClientPtr clp, ExperimentState& st){
  // Some workload descriptors
  static const int nInserts = 1000000;
  static const int nReads = 10000;
  static const int nScans = 1000;
  static const int nUpdates = 1000000;
  static const int nRemoves = 1;

  // Workload features
  static const int max_fields = 10;
  static const int key_len = 128;
  static const int value_len = 200;

  std::vector<std::string> keys;
  int err_count = 0;

  // Do inserts.
  for (auto i = 0; i < nInserts; ++i){
    if (0 != do_insert(st, clp, max_fields, key_len, value_len, &keys)){
      err_count++;
    }
  }

  // Do reads.
  for (auto i = 0; i < nReads; ++i){
    int idx = st.getZipfIdx();
    const Key key = keys[idx];
    if (0 != do_read(st, clp, max_fields, key)){
      err_count++;
    }
  }

  // Do updates
  for (auto i = 0; i < nUpdates; ++i){
    const Key key = keys[st.getZipfIdx()];
    if (0 != do_update(st, clp, max_fields, value_len, key)){
      err_count++;
    }
  }
  if (err_count) LOG("%d errors in update\n", err_count);
  err_count = 0;

  // Do scans
  for (auto i = 0; i < nScans; ++i){
    const int nRows = 5;
    const Key key = keys[st.getZipfIdx()];
    if (0 != do_scan(st, clp, max_fields, nRows, key, true)){
      err_count++;
    }
  }
  if (err_count) LOG("%d errors in scan\n", err_count);
  err_count = 0;

  // Do Removes
  for (auto i = 0; i < nRemoves; ++i){
    //const int nRows = 5;
    const Key key = keys[st.getZipfIdx()];
    if (0 != do_remove(st, clp, key)){
      err_count++;
    }
  }
  if (err_count) LOG("%d errors in remove\n", err_count);

  st.PrintTimes();
  return 0;
}

int do_sanity_check(ClientPtr clp){
  FieldList fields1, fields2;
  ValueMap authority1, authority2, update;

  authority1["FIELD0"] = "test_value 1";
  authority1["FIELD8"] = "test_value 2";

  authority2["FIELD1"] = "test_value 3";
  authority2["FIELD9"] = "test_value 4";

  update["FIELD5"] = "updated field\n\r\t";

  // CHECK INSERT
  assert(0 == clp->insert(TableId(TABLENAME), Key("key1"), authority1));
  assert(0 == clp->insert(TableId(TABLENAME), Key("key2"), authority2));

  // CHECK READ WHAT YOU WRITE
  ValueMap read1, read2;
  for (auto& it : authority1)
    fields1.push_back(it.first);
  assert(0 == clp->read(TableId(TABLENAME), Key("key1"), fields1, read1));
  for (auto& it : authority1)
    assert(authority1[it.first] == read1[it.first]);

  // CHECK READ WHAT YOU UPDATE
  for (auto& it : update){
    fields2.push_back(it.first);
    authority2[it.first] = it.second;
  }
  assert(0 == clp->update(TableId(TABLENAME), Key("key1"), update));
  assert(0 == clp->update(TableId(TABLENAME), Key("key2"), update));
  assert(0 == clp->read(TableId(TABLENAME), Key("key2"), fields2, read2));
  for (auto& it : fields2)
    assert(authority2[it] == read2[it]);
  for (auto& it : update)
    assert(update[it.first] == read2[it.first]);

  // CHECK SCAN
  std::vector<ValueMap> result;
  FieldList update_field;
  update_field.push_back("FIELD5");
  assert(0 == clp->scan(TableId(TABLENAME), Key("key1"), 2, update_field, result));
  assert(result.size() >= 1);
  size_t original_result_size = result.size();
  result.clear();
  assert(result.size() == 0);

  // CHECK REMOVE WHAT YOU REMOVE
  assert(0 == clp->remove(TableId(TABLENAME), Key("key1")));
  assert(0 == clp->scan(TableId(TABLENAME), Key("key1"), 2, update_field, result));
  assert(result.size() < original_result_size || result.cbegin()->begin()->first != Key("key1"));

  return 0;
}

static std::once_flag flag;
static std::shared_ptr<ZipfianGenerator> zipf = nullptr;

bool synchronize(ClientPtr clp, const Parameters* param, const std::string& expStr){
  auto now = std::bind(std::chrono::system_clock::now);
  time_t s_time = 0;
  //size_t sz;
  Key k(expStr);
  ValueMap v, rv;
  std::string syncfilepost(param->syncfilepost);
  std::string syncfilewait(param->syncfilewait);
  LOG("I am client %d%s\n", optClientno, optClientno == 0 ? " (leader)" : "");
  LOG("Syncfilepost is: \"%s\", syncfilewait is: \"%s\"\n", syncfilepost.c_str(), syncfilewait.c_str());
  if (syncfilepost != ""){
    int fd = open(syncfilepost.c_str(), O_CREAT | O_RDWR, 0644 );
    if (fd == -1){
      LOG("Cannot create syncfilepost: %s\n", strerror(0));
    } else close(fd);
  }
  int retries = 45;
  if (optClientno==0){
    int rc;
    LOG("I am the leader\n");
    if (syncfilewait != ""){
      LOG("Waiting for syncfilewait\n");
      struct stat dummy;
      int rc, shownerror=0;
      while (1){
        rc = stat(syncfilewait.c_str(), &dummy);
        if (rc == 0) break; // done
        if (errno != ENOENT && !shownerror){
          ++shownerror;
          LOG("Error stat filewait: %s\n", strerror(0));
        }
        Sleep(1000);
      }
      LOG("Got syncfilewait\n");
    }

    do {
      std::stringstream time;
      std::string starttime;
      FieldList f;
      s_time = std::chrono::system_clock::to_time_t(now() + std::chrono::seconds(param->lead_time));
      time.clear();
      time << s_time;
      starttime = time.str();
      LOG("Time is: %s\n", starttime.c_str());
      v["FIELD1"] = starttime;
      LOG("Writing to synctable: %s (%lld) %s\n", k.c_str(),
          (long long) MurmurHash64A(k.c_str(), k.length()),
          starttime.c_str());
      rc = clp->insert(TableId("synctable"), k, v);
      if (rc){
        LOG("Couldn't start synchronize: cannot write to synctable.\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      } else {
        f.push_back(std::string("FIELD1"));
        rc = clp->read(TableId("synctable"), k, f, rv);
      }
      if (rc){
        LOG("Couldn't start synchronize: cannot read synctable.\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      } else {
        LOG("Read: %s from synctable\n", rv[f[0]].c_str());
      }
    retries--;
    } while (rc && retries > 0);
    if (retries == 0){
      _exit(1);
      LOG("YOU SHOULD NEVER SEE THIS!\n");
    }
  } else {
    LOG("waiting for leader\n");
    FieldList l;
    l.push_back("FIELD1");
    int count = 0;
    int limit = 2;
    int rc;
    do {
      v.clear();
      rc = clp->read(TableId("synctable"), k, l, v);
      if (!rc  && v[l[0]].size() > 0)
        break;
      auto t = now();
      time_t rtime = std::chrono::system_clock::to_time_t(t);
      if (count++ % limit == 0){
        limit += limit;
        LOG("DID NOT FIND SYNC INFO rc=%d\n", rc);
      }
      std::this_thread::sleep_for(std::chrono::seconds(5));
      count++;
      if (count > 9000){ // wait up to 150 mins
        _exit(1);
      }
    } while (1);
    // std::string starttime;
    // starttime = v[l[0]];
    std::stringstream time;
    time << v[l[0]];
    time >> s_time;
    //std::this_thread::sleep_for(std::chrono::seconds(10));
    //if (count++ % limit == 0){
    //  limit += limit;
    //  LOG("Got: %d bytes: %s", v[l[0]].size(), ctime(&s_time));
    //}
    LOG("read: %s:%s %d bytes time %s\n", k.c_str(), v[l[0]].c_str(), v[l[0]].size(), ctime(&s_time));
    //} while (s_time == 0);
  }
  LOG("Sleeping in synchronize until: %s", ctime(&s_time));
  if (s_time > std::chrono::system_clock::to_time_t(now())){
    std::this_thread::sleep_until(std::chrono::system_clock::from_time_t(s_time));
  } else {
    LOG("FATAL ERROR, DIDN'T GET START TIME UNTIL THE FUTURE\n");
    LOG("Start time is: %s, seems funny, so I'm bailing\n", ctime(&s_time));
    return true;
  }
  return false;
}

void startup(ClientPtr clp, const Parameters* param){
  zipf = std::shared_ptr<ZipfianGenerator>(new ZipfianGenerator(param->nTuples, param->shape));
  zipf->Init();
  // ExperimentState Bogus(nullptr); Use this if we want to time BulkInsert performance
  RandomWrapper key_rand;
  key_rand.SetSeed(param->seed);
  RandomWrapper value_rand;
  int bulkSz = 200;
  int firstkey = true;

  // This initializes the clock, don't remove
  windows_clock::time_point now = windows_clock::now();
  LOG("LOADING %d tuples\n", param->nTuples);
  if (param->load && optClientno == 0){
    LOG("I'm actually loading the DB.\n");
    for (auto i = 0; i < param->nTuples; i += bulkSz){
      if (i % 1000 == 0){
        LOG("I've loaded %d tuples\n", i);
      }
      std::vector<Key> keys;
      std::vector<ValueMap> values;
      for (auto j = 0; j < bulkSz; ++j){
        Key k(key_rand.GenerateRandomString(param->key_len));
        keys.push_back(k);
        values.push_back(ValueMap());

        if (firstkey){
          smallestkey_ = k;
          firstkey = false;
        } else {
          if (k < smallestkey_){
            smallestkey_ = k;
          }
        }
        // Use a different random source for field/value generation
        for (auto k = 0; k < 10; ++k){
          values[j][value_rand.PickRandomField(k)] =
              value_rand.GenerateRandomString(param->value_len);
        }
      }
      if (0 != clp->BulkInsert(TableId(TABLENAME), keys, values)){
        LOG("Couldn't bulk insert.\n");
        _exit(1);
      }
      keys_.insert(keys_.end(), keys.begin(), keys.end());
      // std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  } else{
    LOG("I'm just loading keys into memory\n");
    // Load keys from seed
    for (auto i = 0; i < param->nTuples; ++i){
      Key k(key_rand.GenerateRandomString(param->key_len));
      keys_.push_back(k);
    }
  }
  if (param->nTuples > 0){
    LOG("loading took: %lld seconds, first key is %s\n",
        std::chrono::duration_cast<std::chrono::seconds>(windows_clock::now() - now).count(),
        keys_[0].c_str());
  } else {
    LOG("No keys were loaded\n");
  }
}

Workload getWorkloadFromString(const std::string& desc){
  switch (desc[0]){
    case 'a':
    case 'A':
      return WorkloadA;
    case 'b':
    case 'B':
      return WorkloadB;
    case 'c':
    case 'C':
      return WorkloadC;
    case 'd':
    case 'D':
      return WorkloadD;
    case 'e':
    case 'E':
      return WorkloadE;
    case 'f':
    case 'F':
      return WorkloadF;
    case 'g':
    case 'G':
      return WorkloadG;
    case 'h':
    case 'H':
      return WorkloadH;
    case 'i':
    case 'I':
      return WorkloadI;
    case 'j':
    case 'J':
      return WorkloadJ;
    case 'k':
    case 'K':
      return WorkloadK;
    case 'l':
    case 'L':
      return WorkloadL;
    case 'm':
    case 'M':
      return WorkloadM;
    case 'n':
    case 'N':
      return WorkloadN;
    case 'o':
    case 'O':
      return WorkloadO;
    case 'w':
    case 'W':
      return WorkloadW;
    case 'z':
    case 'Z':
      return ZipfianTest;
    default:
      assert(false);
      return WorkloadA;
  }
}

static int count = 0;
static std::mutex sync_mutex;

bool bail;

int RunWorkload(ClientPtr clp, Workload w, const Config* conf){
  Parameters p(conf);
  std::call_once(flag, std::bind(startup, clp, &p));

  // moved to before synchronize because DNS may choke
  time_t t;
  unsigned seed;
  int workerno;
  // Barrier for synchronization: all threads will wait until the first one has finished
  // synchronizing. The last thread will reset the count so the next experiment can 
  // synchronize.
  {
    std::lock_guard<std::mutex> lock(sync_mutex);
    if (count == 0 && p.sync){ // only sync if we are first thread and sync parameter is true
      bail = synchronize(clp, &p, conf->_workload);
    }
    t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    // Thread id from stack overflow user 888 and user Barry
    seed = (unsigned int)(std::hash<std::thread::id>()(std::this_thread::get_id()) ^ t + getpid());
    workerno = count;
    count++;
    if (count == conf->get<int>("threads", 0))
      count = 0;
  }
  if (bail){
    LOG("Bailing! %s\n", conf->_workload.c_str());
    return 0;
  }

  ExperimentState st(zipf, &p);
  st.seedRandom(seed);
  st.setWorkerno(workerno);
  std::string babble = st.getRandom().GenerateRandomString(10);
  LOG("My babble is: %s\n", babble.c_str());
  switch(w){
  case WorkloadA:
    do_workload_a(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadB:
    do_workload_b(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadC:
    do_workload_c(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadE:
    do_workload_e(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadF:
    do_workload_f(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadG:
    { // this { is here to avoid warning about non-initialization of firstkey
    int firstkey = conf->get<int>("firstkey", 1);
    do_workload_g(clp, st, p, firstkey);
    st.PrintTimes();
    return 0;
    }
  case WorkloadH:
    do_workload_h(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadI:
    { // this { is here to avoid warning about non-initialization of firstkey
    int firstkey = conf->get<int>("firstkey", 1);
    do_workload_i(clp, st, p, firstkey);
    st.PrintTimes();
    return 0;
    }
  case WorkloadJ:
    do_workload_j(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadK:
    do_workload_k(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadL:
    do_workload_l(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadM:
    do_workload_m(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadN:
    do_workload_n(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadO:
    do_workload_o(clp, st, p);
    st.PrintTimes();
    return 0;
  case WorkloadW:
    do_workload_w(clp, st, p);
    st.PrintTimes();
    return 0;
  // Regression test performs basic sanity checks, then runs simple test.
  case RegressionTest:
    if (do_sanity_check(clp) == -1)
      return -1;
  case SimpleTest:
    return do_simple_test(clp, st);
  // ZipfianTest is a playground for checking zipf distribution
  case ZipfianTest:
    return do_zipfian_test(st);
  default:
    return -1;
  }
}


