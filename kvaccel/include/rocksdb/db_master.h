#pragma once

#include <stdint.h>
#include <stdio.h>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/block_cache_trace_writer.h"
#include "rocksdb/iterator.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/transaction_log.h"
#include "rocksdb/types.h"
#include "rocksdb/version.h"
#include "rocksdb/wide_columns.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/clue_entry_set.h"
#include "util/hash_map.h"
#include <thread>
#include <queue>
#include <mutex>
#include <utility>
#include <tuple>
#include <iostream>
#include <filesystem>
#include <shared_mutex>

#include <functional>
#include <condition_variable>


#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#endif

#if defined(__GNUC__) || defined(__clang__)
#define ROCKSDB_DEPRECATED_FUNC __attribute__((__deprecated__))
#elif _WIN32
#define ROCKSDB_DEPRECATED_FUNC __declspec(deprecated)
#endif

//#define L0_high 28 // hyperparameter
#define monitor_interval 100000 // monitoring module checks every interval * microseconds. hyperparameter 1000000 is 1s
#define Memtable_middle_split 8 
#define RESERVE 2097152 //2MB
//#define rollback_threshold 2147483640 //temp disabled
#define rollback_threshold 1000 //temp disabled

#define BLOOM_FILTER_SIZE 200000000
#define HASH_FUNCTION 8


namespace ROCKSDB_NAMESPACE {

struct BloomFilter{
  size_t bit_array[BLOOM_FILTER_SIZE];
  std::mutex bf_mtx;
};

class DB_MASTER{
  public:
  DB_MASTER(){
    dbptr_array = new DB*[MAX_DB_INSTANCE];
    num_instances = 0;
    //put_idx = 0;
    cnt_put = 0;
    //mtx_array = new std::mutex[MAX_DB_INSTANCE];
  }
  /*~DB_MASTER(){
    for(int i = 0; i < num_instances; i++){
        dbptr_array[i]->DB::Close();
    }
    delete dbptr_array;
  }*/


  Status Open(const Options& options, const std::string& name, const int num_instances);
  Status Put(const WriteOptions& options, const Slice& key,
                      const Slice& value);
  Status PutBatch(const WriteOptions& options, const Slice& key,
                      const Slice& value);
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);          
  Status GetPin(const ReadOptions& options, const Slice& key, PinnableSlice* pinnable_val);          
  Status DestroyDB_Master(const Options& options);

  void IteratorGet(Iterator** iter_to_use_master, std::unique_ptr<Iterator> *single_iter_master);
  void IteratorReset(std::unique_ptr<Iterator> *single_iter_master, ReadOptions options);
  void IteratorSeek(Iterator** iter_to_use_master, Slice key);
  bool IteratorValid(Iterator** iter_to_use_master);
  bool IteratorCompare(Iterator** iter_to_use_master, Slice key);
  void IteratorNext(Iterator** iter_to_use_master);
  void IteratorPrev(Iterator** iter_to_use_master);
  void DestroyIter();
  Slice IteratorValue(Iterator** iter_to_use_master);
  Slice IteratorKey(Iterator** iter_to_use_master);
  
  //[KI] bloom filter
  uint64_t double_hash(const Slice& key, int n);
  void bloom_filter_init(BloomFilter *filter);
  void bloom_filter_add(BloomFilter *filter, const Slice& key);
  int bloom_filter_check(BloomFilter *filter,const Slice& key);
  void bloom_filter_delete(BloomFilter *filter, const Slice& key);
  
  int min_iterator_index;

  void Consumer();

  void Monitor_Consumer();

  void Rollback_Consumer(int lsm_idx);

  int GetNumDB(){
    return num_instances;
  }

  DB* GetNthDB(int i){
    return dbptr_array[i];
  }

  // only for debug
  void PutThroughCE(const WriteOptions& options, const Slice& key,
                      const Slice& value);
  
  int totalL0(void);
  int totalL0Num;
  int local_l0_stall;

  unsigned int totalCE(void);
  unsigned int totalCENum;
  std::shared_mutex rollback_mtx;
  

  void printstat(void);

  HashMap<uint64_t, std::string> keyTracker;

  private:
  DB **dbptr_array;
  Iterator* iter_array;
  int num_instances;
  unsigned int ilsm_iter_id;
  unsigned int iter_location;
  //std::mutex *mtx_array;  // 각 DB 인스턴스에 대한 뮤텍스 배열
 
  //int put_idx; //put to db[idx]
  int cnt_put; //cnt put to current idx
  
  int *l0_size;
  int l0_stop_trigger;
  int l0_compaction_trigger;
  int l0_compaction_db_num;

  /* for MMO */
  int max_memtable_number;
  uint64_t set_memtalbe_size;
  uint64_t total_memtable_size;

  /* for RDO */
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
 

  static const int MAX_DB_INSTANCE = 100;

  int hash_key(const Slice& key);


  //monitoring module
  int monitor_L0_status_arr; // 0 : no stall, 1 : L0 high, 2 : stall
  int rollback_status;
  std::thread monitor_thread;
  bool run_monitor_thread;
  enum Level{
    LEVEL_ZERO,
    LEVEL_LOW,
    LEVEL_MIDDLE,
    LEVEL_HIGH
  };

  //rollback module
  std::thread *rollback_thread_arr;
  std::mutex **rollback_mutex_arr;
  std::mutex **rollback_write_mutex_arr;

  // for count
  int cnt_primary_put;
  int cnt_primary_but_over_CTT;
  int cnt_primary_get;
  std::atomic<int> kache_flag; // [KI]: flag for dev_lsm
  std::atomic<int> rollback_flag; 

};


}
