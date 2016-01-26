#ifndef _BENCH_CLIENT_H
#define _BENCH_CLIENT_H

#include <map>
#include <string>
#include <vector>

typedef std::string TableId;
typedef std::string Key;
typedef std::vector<std::string> FieldList;
typedef std::map<std::string,std::string> ValueMap;

class BenchmarkClient {
public:
  // REQUIRED YCSB functions
  virtual int Init() = 0;
  virtual int read(const TableId& table, const Key& key,
	           const FieldList& fields, ValueMap& result) = 0;
  virtual int scan(const TableId& table, const Key& start_key, int count,
	           const FieldList& fields, std::vector<ValueMap>& result) = 0;
  virtual int scanNodata(const TableId& table, const Key& start_key, int count,
	           const FieldList& fields, std::vector<ValueMap>& result) = 0;
  virtual int update(const TableId& table, const Key& key, const ValueMap& values) = 0;
  virtual int insert(const TableId& table, const Key& key, const ValueMap& values) = 0;
  virtual int remove(const TableId& table, const Key& key) = 0;
  virtual int BulkInsert(const TableId& table, const std::vector<Key>& keys,
                         const std::vector<ValueMap>& values) = 0;

  // OPTIONAL YCSB functions (for workloads F, G, H)
  virtual int begin(void) {return -1;}
  // complete() should commit or abort the transaction (whichever is appropriate)
  virtual int complete(void) {return -1;}
  virtual int GetMonotonicInt(int& monot_int, const int hint){ return -1; }
  virtual int InsertInt(const TableId& table, int i, const ValueMap& values){ return -1; }

  // OPTIONAL Wikipedia functions (for workload W)
  virtual int browser_cache_read(int seed) {return -1;}
  virtual int memcache_read(int seed) {return -1;}
  virtual int database_read(int seed) {return -1;}
  virtual ~BenchmarkClient () {};
};

#endif
