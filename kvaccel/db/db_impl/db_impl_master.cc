#include <cinttypes>

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/periodic_task_scheduler.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/table.h"
#include "rocksdb/wal_filter.h"
#include "test_util/sync_point.h"
#include "util/rate_limiter_impl.h"
#include "util/udt_util.h"
#include "util/murmurhash.h"

#include "rocksdb/db_master.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include <stdlib.h>
#include <string>
#include <sched.h>

#include <chrono>
#include <fstream>
#include <filesystem> 
#include <stdexcept> 

#include "rocksdb/iLSM.h"

//for iLSM
iLSM::DB ilsm_db_;

#define ROLLBACK_CTRL

struct OperationTime {
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point mid;
    std::chrono::high_resolution_clock::time_point end;
};

std::vector<OperationTime> times_put;
std::vector<OperationTime> times_hash;
std::vector<OperationTime> times_fb_ce;
std::vector<OperationTime> times_fb_tkv;

std::vector<OperationTime> times_get;
std::vector<OperationTime> times_get_ce;

std::mutex mtx;
#ifdef ROLLBACK_CTRL
std::mutex reset_mtx; // dev_lsm reset mutex
#endif

//std::atomic<int> iter_counter = 0;

//static uint32_t err_count = 0;
static pid_t checker = -1;
#define __int64 long long

std::atomic<bool> shouldExit(false); 
std::atomic<bool> rollback_should_exit(false); 
std::atomic<bool> finalexit(false); 
int L0_high;


namespace ROCKSDB_NAMESPACE{
  namespace fs = std::filesystem; // 네임스페이스 별칭 생성
  BloomFilter test_filter;

Status DB_MASTER::Open(const Options& options, const std::string& name, const int num_instances_){
  /*
  if(num_instances_ > MAX_DB_INSTANCE){
    return Status::NotSupported(
      "Open() max 100 instances.");
  }
  */
 
  /* [KI]: BloomFilter init */
  //bloom_filter_init(&test_filter);

  if(num_instances_ != 1){
    return Status::NotSupported("[KI]: Open() must be 1."); 
  }

  num_instances = num_instances_;
  kache_flag = 0; // [KI]: dev_lsm is empty
  rollback_flag = 0;
  cnt_primary_put = 0;
  cnt_primary_but_over_CTT = 0;
  cnt_primary_get = 0;
  keyTracker.count = 0;

  /* open N instances */
  std::string *name_arr = new std::string[num_instances];

  for(int i = 0; i < num_instances; i++){
      name_arr[i] = name + std::to_string(i);
  }

  Status ret_val = Status::OK();

  for(int i = 0; i < num_instances; i++){
    printf("Open DB path : %s\n", name_arr[i].c_str());
    Options tmp_option = Options(options);

    tmp_option.db_master_ptr = this;
    ret_val = DB::Open(tmp_option, name_arr[i], &dbptr_array[i]);

    if(!ret_val.ok()){
      printf("failed while opening DB path : %s\n", name_arr[i].c_str());
      break;
    }
  }

  // temp hard corded
  // open iLSM
  int err = ilsm_db_.Open("/dev/nvme1n1");
  if (err < 0) {
    fprintf(stderr, "[iLSM] open error: %d\n", err);
  }
  //dbptr_array[0]->GetDBOptions(options);
  l0_stop_trigger = options.level0_stop_writes_trigger;
  l0_compaction_trigger = options.level0_file_num_compaction_trigger;
  l0_size = (int *)malloc(sizeof(int)*num_instances);
  for(int i=0; i<num_instances; i++){
    l0_size[i] = 0;
  }
  L0_high = 7;
  //printf("stop trigger: %d\n", l0_stop_trigger);
  //L0_high = (int)l0_stop_trigger * 0.8;
  //local_l0_stall = (l0_stop_trigger / num_instances) - 1;
    
	l0_compaction_db_num = 0;

  set_memtalbe_size = options.write_buffer_size;
  total_memtable_size = set_memtalbe_size * options.max_write_buffer_number;

  /* create BG thread for monitoring L0 files */
  shouldExit = false;
  /*if (checker == -1) {
    monitor_thread = std::thread(&rocksdb::DB_MASTER::Monitor_Consumer, this);
    monitor_thread.detach();
  }*/

  monitor_L0_status_arr = LEVEL_LOW;
  rollback_status = 0;
  
  //diable rollback threads
  rollback_should_exit = false;
  rollback_thread_arr = new std::thread[num_instances];
  if (checker == -1) {
    for(int i = 0; i < num_instances; i++){
      rollback_thread_arr[i] = std::thread(&rocksdb::DB_MASTER::Rollback_Consumer, this, i);
      rollback_thread_arr[i].detach();
    }
  }
  
  

  soft_pending_compaction_bytes_limit = options.soft_pending_compaction_bytes_limit;
  hard_pending_compaction_bytes_limit = options.hard_pending_compaction_bytes_limit;

  if (checker == -1) {
    checker = gettid();
  }

  return ret_val;
}

Status DB_MASTER::Put(const WriteOptions& options, const Slice& key, const Slice& value){
  /* [KI]: write path */
  // 1. if write stall, put to dev-lsm
  // 2. no write stall, then ...
  //  (1) rollback yet: check dev-lsm first, dev-lsm contain -> delete, and then put to main-lsm
  //  (2) already rollback: put to main-lsm
  //printf("[KI]: PUT\n");
  //int home_lsm_idx = hash_key(key);
  //int target_lsm_idx = home_lsm_idx;
  int home_lsm_idx = 0; // home_lsm_idx = main_lsm
  Status ret_val;

  /*std::string l0_num_str, memtable_size_str, pending_size_str;
  [[maybe_unused]]int l0_num_tmp;
  [[maybe_unused]]uint64_t pending_compaction_bytes;

  [[maybe_unused]]uint64_t memtable_size;

  std::string write_stall_str;
  [[maybe_unused]]int write_stall;*/

  /* Make True-Entry (TE) */
  //std::string value_str = std::string("a") + std::string(value.ToString());
  std::string value_str = std::string(value.ToString());
  std::string key_str = std::string(key.ToString());

  /*std::string tmp_value;
  dbptr_array[home_lsm_idx]->GetProperty("rocksdb.is-write-stopped", &write_stall_str);
  write_stall = stoi(write_stall_str);*/

  /* no write stall */
  if(monitor_L0_status_arr != LEVEL_HIGH){
#ifdef ROLLBACK_CTRL
    if(kache_flag == 0){ // dev_lsm is empty(rollback alreay did)
      //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      ret_val = dbptr_array[home_lsm_idx]->DB::Put(options, key, value_str);
      //std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      //std::cout << "Time difference (main-lsm) = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
      return ret_val;
    }
    else{

      //put with rollback
#ifdef ROLLBACK_CTRL
      reset_mtx.lock();
#endif
      rollback_mtx.lock();
      //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      if (keyTracker.Contains(double_hash(key, 1))){
        keyTracker.Delete(double_hash(key, 1));
        ret_val = dbptr_array[home_lsm_idx]->DB::Put(options, key, value_str);
      } else {
        ret_val = dbptr_array[home_lsm_idx]->DB::Put(options, key, value_str);
      }
      rollback_mtx.unlock();
#ifdef ROLLBACK_CTRL
      reset_mtx.unlock();
#endif
      return ret_val;
    }
#else
    ret_val = dbptr_array[home_lsm_idx]->DB::Put(options, key, value_str);
    return ret_val;
#endif
  } else{
    //std::cout << "[KI]: redirection to dev_lsm, key is " << key.ToString() << std::endl;
    //test_filter.bf_mtx.lock();
    //hm_mutex.lock();
#ifdef ROLLBACK_CTRL
    reset_mtx.lock();
#endif
    //rollback_mtx.lock();
    kache_flag = 1;
    //value_str = std::to_string(err_count++);
    //test_filter.bf_mtx.unlock();
    //std::cout << "put key: " << test << std::endl;
    //printf("put val: %c%c%c%c\n", value_str[0], value_str[1], value_str[2], value_str[3]);
    ilsm_db_.Put(key_str,value_str);
    keyTracker.Insert(double_hash(key, 1), key.ToString());
    //rollback_mtx.unlock();
#ifdef ROLLBACK_CTRL
    reset_mtx.unlock();
#endif
    //hm_mutex.unlock();
    ret_val = Status::OK();
    return ret_val;
  }
  /*
  for(int i=0; ; i++){
    if(i >= num_instances) i=0;
    mtx.lock();
    target_lsm_idx = (*slsms_ptr)[i].id;
    mtx.unlock();

    dbptr_array[target_lsm_idx]->GetProperty("rocksdb.is-write-stopped", &write_stall_str);
    write_stall = stoi(write_stall_str);
    
    if(monitor_L0_status_arr[target_lsm_idx] != LEVEL_HIGH && write_stall != 1){
      if(target_lsm_idx != home_lsm_idx){
        ret_val = dbptr_array[target_lsm_idx]->DB::Put(options, key, value_str);
        ret_val = dbptr_array[home_lsm_idx]->DB::Put(options, key, std::to_string(target_lsm_idx));
        ce_count++;

        return ret_val;
      }
      else{
        ret_val = dbptr_array[home_lsm_idx]->DB::Put(options, key, value_str);
        return ret_val;
      }
    }
  }
  */
  //Put iLSM
  //ilsm_db_.Put(key.ToString(), value_str);

}

Status DB_MASTER::Get(const ReadOptions& options, const Slice& key, std::string* value){

  //OperationTime opTime_get, opTime_get_ce;
  OperationTime opTime_get;
  Status ret_val = Status::NotFound("Nothing found.");

  std::string tmp_value;
  std::string key_str = std::string(key.ToString());
  //int target_lsm_idx;

  opTime_get.start = std::chrono::high_resolution_clock::now();
  //opTime_get_ce.start = std::chrono::high_resolution_clock::now();
  // read from main_lsm
  if(kache_flag == 0){
    if(dbptr_array[0]->Get(options,key,&tmp_value).Status::ok()){
      //if(tmp_value[0] == 'a'){
        ret_val = Status::OK();
        value->assign(tmp_value.substr(0,tmp_value.length()));

        opTime_get.end = std::chrono::high_resolution_clock::now();
        //std::lock_guard<std::mutex> lock(mtx);
        times_get.push_back(opTime_get);
      //}
    } /*else if(ilsm_db_.Get(key_str, tmp_value) < 0){
      std::cout << "literally nothing" << std::endl;
    }*/
  } else if (rollback_flag == 1) {
    //std::chrono::steady_clock::time_point be = std::chrono::steady_clock::now();
    rollback_mtx.lock_shared();
    //std::chrono::steady_clock::time_point en = std::chrono::steady_clock::now();
    //std::cout << "Time difference (lock ) = " << std::chrono::duration_cast<std::chrono::nanoseconds>(en - be).count() << "[ns]" << std::endl;
    //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    if(keyTracker.Contains(double_hash(key, 1))){ 
      //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      if(ilsm_db_.Get(key_str, tmp_value) >= 0){
      //std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      //std::cout << "Time difference (dev-lsm) = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
        rollback_mtx.unlock_shared();
        ret_val = Status::OK();
        value->assign(tmp_value.substr(0,tmp_value.length()));

        opTime_get.end = std::chrono::high_resolution_clock::now();
        //std::lock_guard<std::mutex> lock(mtx);
        times_get.push_back(opTime_get);
      } else {
        //printf("key not found\n");
        rollback_mtx.unlock_shared();
      }
    }
    else{
      if(dbptr_array[0]->Get(options,key,&tmp_value).Status::ok()){
        rollback_mtx.unlock_shared();
        ret_val = Status::OK();
        value->assign(tmp_value.substr(0,tmp_value.length()));
        opTime_get.end = std::chrono::high_resolution_clock::now();
        times_get.push_back(opTime_get);
      } else {
        //printf("key not found\n");
        rollback_mtx.unlock_shared();
      }
    }
  } else {
    if(keyTracker.Contains(double_hash(key, 1))){
      //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      if(ilsm_db_.Get(key_str, tmp_value) >= 0){
        //std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      //std::cout << "Time difference (dev-lsm) = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
        ret_val = Status::OK();
        value->assign(tmp_value.substr(0,tmp_value.length()));
        opTime_get.end = std::chrono::high_resolution_clock::now();
        times_get.push_back(opTime_get);
      } else {
        //printf("key not found\n");
      }
    }
    else{
      if(dbptr_array[0]->Get(options,key,&tmp_value).Status::ok()){
        ret_val = Status::OK();
        value->assign(tmp_value.substr(0,tmp_value.length()));
        opTime_get.end = std::chrono::high_resolution_clock::now();
        times_get.push_back(opTime_get);
      } else {
        //printf("key not found\n");
      }
    }
  }
  return ret_val;
}

Status DB_MASTER::GetPin(const ReadOptions& options, const Slice& key, PinnableSlice* pinnable_val){
  //OperationTime opTime_get, opTime_get_ce;
  OperationTime opTime_get;
  Status ret_val = Status::NotFound("Nothing found.");

  std::string tmp_value;
  //int target_lsm_idx;
  int home_lsm_idx = 0;
  //int hashkey = hash_key(key);

  opTime_get.start = std::chrono::high_resolution_clock::now();
  //opTime_get_ce.start = std::chrono::high_resolution_clock::now();
  
  /* check main_lsm */
  if(kache_flag == 0){
    if(dbptr_array[home_lsm_idx]->Get(options, dbptr_array[home_lsm_idx]->DefaultColumnFamily(),key,pinnable_val).Status::ok()){
      if(pinnable_val->ToString()[0] == 'a'){
        ret_val = Status::OK();

        opTime_get.end = std::chrono::high_resolution_clock::now();
        //std::lock_guard<std::mutex> lock(mtx);
        times_get.push_back(opTime_get);
      }
    }
    

  }
  /*
  if(dbptr_array[hashkey]->Get(options, 
          dbptr_array[hashkey]->DefaultColumnFamily(), key, pinnable_val).Status::ok()){
    // node found, ptr node or data node 


    if(pinnable_val->ToString()[0] == 'a'){  // data node 
      ret_val = Status::OK();

      opTime_get.end = std::chrono::high_resolution_clock::now();
      std::lock_guard<std::mutex> lock(mtx);
      times_get.push_back(opTime_get);


    }
    else{ // ptr node 
      if(pinnable_val->size() > 2){
        std::cout << "[DEBUG Get] value " << tmp_value << std::endl;
      }
      else{
        target_lsm_idx = std::stoi(pinnable_val->ToString());
        opTime_get_ce.mid = std::chrono::high_resolution_clock::now();
        if(dbptr_array[target_lsm_idx]->Get(options, dbptr_array[target_lsm_idx]->DefaultColumnFamily(), key, pinnable_val).Status::ok()){
          ret_val = Status::OK();

        }
        opTime_get_ce.end = std::chrono::high_resolution_clock::now();
        std::lock_guard<std::mutex> lock2(mtx);
        times_get_ce.push_back(opTime_get_ce);

      }
    }
  
  }
  */
  return ret_val;
}

void DB_MASTER::IteratorGet(Iterator** iter_to_use_master, std::unique_ptr<Iterator> *single_iter_master){
  iter_to_use_master[0] = single_iter_master[0].get();
}

void DB_MASTER::IteratorReset(std::unique_ptr<Iterator> *single_iter_master, ReadOptions options){
  single_iter_master[0].reset(dbptr_array[0]->NewIterator(options));
  if (kache_flag == 1) {
    ilsm_db_.CreateIter(ilsm_iter_id);
    printf("create iter id: %d\n",ilsm_iter_id);
  }
}

void DB_MASTER::IteratorSeek(Iterator** iter_to_use_master, Slice key){
  //unsigned int target_key = (unsigned long long)strtoul(key.ToString(true).substr(0, 16).c_str(), NULL, 16);
  iter_to_use_master[0]->Seek(key);
  if (kache_flag == 1) {
    //rollback_mtx.lock_shared();
    printf("seek iter id: %d\n",ilsm_iter_id);
    std::string target_key = key.ToString();
    std::string value;
    ilsm_db_.SeekScan(ilsm_iter_id, target_key, value);
    ilsm_db_.iter_values[ilsm_iter_id] = value;
    ilsm_db_.iter_keys[ilsm_iter_id] = target_key;
    int test = iter_to_use_master[0]->key().compare(target_key);
    if (test <= 0) iter_location = 0;
    else iter_location = 1;
    //rollback_mtx.unlock_shared();
  }
}

bool DB_MASTER::IteratorValid(Iterator** iter_to_use_master){
  if(iter_to_use_master[0]->Valid())
    return true;
  else
    return false;
}

bool DB_MASTER::IteratorCompare(Iterator** iter_to_use_master, Slice key){
  bool rtv = false;
  if(iter_to_use_master[0]->Valid() && iter_to_use_master[0]->key().compare(key) == 0){
    rtv = true;
  } else if (kache_flag == 1){
    if (key.compare(ilsm_db_.iter_keys[ilsm_iter_id]) == 0){
      rtv = true;
    }
  }
  return rtv;
}

void DB_MASTER::IteratorNext(Iterator** iter_to_use_master){
  //int flag;
  if (kache_flag == 0) {
    if (iter_to_use_master[0]->Valid())
      iter_to_use_master[0]->Next();
  } else {
    if (iter_location == 0 && iter_to_use_master[0]->Valid()) {
      iter_to_use_master[0]->Next();
      if (iter_to_use_master[0]->key().compare(ilsm_db_.iter_keys[ilsm_iter_id]) > 0){
        std::string key;
        std::string value;
        ilsm_db_.NextScan(ilsm_iter_id, key, value);
        ilsm_db_.iter_values[ilsm_iter_id] = value;
        ilsm_db_.iter_keys[ilsm_iter_id] = key;
        iter_location = 1;
      }
    } else if (iter_location == 1) {
      std::string key;
      std::string value;
      ilsm_db_.NextScan(ilsm_iter_id, key, value);
      ilsm_db_.iter_values[ilsm_iter_id] = value;
      ilsm_db_.iter_keys[ilsm_iter_id] = key;
      if (iter_to_use_master[0]->key().compare(ilsm_db_.iter_keys[ilsm_iter_id]) < 0){
        iter_to_use_master[0]->Next();
        iter_location = 0;
      }
    }
  }
}

void DB_MASTER::IteratorPrev(Iterator** iter_to_use_master){
  
  if(iter_to_use_master[0]->Valid()){
      iter_to_use_master[0]->Prev();
    };
}

void DB_MASTER::DestroyIter(){
  ilsm_db_.DestroyIter(ilsm_iter_id);
}

Slice DB_MASTER::IteratorValue(Iterator** iter_to_use_master){
  if (iter_location == 0)
    return iter_to_use_master[0]->value();
  else
    return ilsm_db_.iter_values[ilsm_iter_id];
}

Slice DB_MASTER::IteratorKey(Iterator** iter_to_use_master){
  if (iter_location == 0)
    return iter_to_use_master[0]->key();
  else
    return ilsm_db_.iter_keys[ilsm_iter_id];
}



Status DB_MASTER::DestroyDB_Master(const Options& options){
  Status ret_val = Status::OK();

  shouldExit = true;
  rollback_should_exit = true;
  
  for(int i = 0; i < num_instances; i++){
    std::string tmp = dbptr_array[i]->GetName();

    delete dbptr_array[i];

    dbptr_array[i] = nullptr;

    Status st = DestroyDB(tmp, options);
    if(!st.ok()){
      ret_val = st;
      printf("[DEBUG] DestoryDB_Master, %d-th not ok.\n", i);
    }
  }



  return ret_val;
}

/*
int DB_MASTER::hash_key(const Slice& key){
  unsigned long long hex_int = (unsigned long long)strtoull(key.ToString(true).substr(0, 16).c_str(), NULL, 16);

  return hex_int % num_instances;

}
*/

inline void DB_MASTER::Monitor_Consumer(){
  //printf("monitoring...\n");
  std::string l0_num_str, memtable_size_str, pending_size_str, slsm_size_str;
  int l0_num_tmp;
  uint64_t pending_compaction_bytes = 0;
  [[maybe_unused]]uint64_t memtable_size;
  [[maybe_unused]]uint64_t slsm_size;
  //static int count = 0;
  Level level_tmp;
  dbptr_array[0]->GetProperty("rocksdb.num-files-at-level0", &l0_num_str);
  l0_num_tmp = stoi(l0_num_str);

  /* for MMO */
  dbptr_array[0]->GetProperty("rocksdb.size-all-mem-tables", &memtable_size_str);
  memtable_size = stoull(memtable_size_str);

  /* for RDO */
  dbptr_array[0]->GetProperty("rocksdb.estimate-pending-compaction-bytes", &pending_size_str);
  pending_compaction_bytes = stoull(pending_size_str);

  if(l0_num_tmp >= 36 || //hardcode
      l0_num_tmp + (int)(memtable_size / set_memtalbe_size) >= 36 ||
    pending_compaction_bytes >= (hard_pending_compaction_bytes_limit - RESERVE)){
    level_tmp = LEVEL_HIGH;
  }
  else if(l0_num_tmp >= L0_high ||
  pending_compaction_bytes >= (soft_pending_compaction_bytes_limit + hard_pending_compaction_bytes_limit) / 2){
    level_tmp = LEVEL_MIDDLE;
  }
  else{
    level_tmp = LEVEL_LOW;
  }

  if(l0_num_tmp == 0){
    level_tmp = LEVEL_ZERO;
  }
/*
  count++;
  if(count == 10) {
    std::cout << "l0_num_tmp: " << l0_num_tmp << ", memtable: " << (int)(memtable_size / set_memtalbe_size) << "\n";
    count = 0;
  }
*/
  monitor_L0_status_arr = level_tmp;
  rollback_status = l0_num_tmp;
}


void DB_MASTER::Rollback_Consumer(int lsm_idx){
  printf("[DEBUG] rollback_consumer_running %d, %d\n", gettid(), lsm_idx);
  //int timer = 0;
  std::string key, tmp_value, tmp2;
  [[maybe_unused]] int flag = -2;
  //unsigned int raw_id;
  [[maybe_unused]] unsigned int iter_id;
  //usleep(1000000); 
  Status ret_val;

  while (!rollback_should_exit.load()){
    usleep(100000);
    Monitor_Consumer();
#ifdef ROLLBACK_CTRL
    if(rollback_should_exit.load()) break;
    if(rollback_status <= 26 && kache_flag == 1){
      //timer = 0;
      reset_mtx.lock();
      rollback_flag = 1;
      std::cout << "count " << keyTracker.count << ",";
      std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      while (true) {
        //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        if (flag < 0) {
          ilsm_db_.CreateIter(iter_id);
          //printf("key: %s\n", key.c_str());
          key.assign("start");
          flag = ilsm_db_.SeekScan(iter_id, key, tmp_value);
          //printf("flag: %d\n", flag);
          /*ilsm_db_.Get(key, tmp2);
          if (tmp2 != tmp_value){
            std::cout << "seekscan wrong: key: " << key << std::endl;
            printf("scan: %c%c%c%c\n", tmp_value[0], tmp_value[1], tmp_value[2], tmp_value[3]);
            printf("get: %c%c%c%c\n", tmp2[0], tmp2[1], tmp2[2], tmp2[3]);
          } else {
            std::cout << "is key: " << key << std::endl;
            printf("scan: %c%c%c%c\n", tmp_value[0], tmp_value[1], tmp_value[2], tmp_value[3]);
            printf("get: %c%c%c%c\n", tmp2[0], tmp2[1], tmp2[2], tmp2[3]);
          }*/
        rollback_mtx.lock();
        if (keyTracker.Contains(double_hash(key, 1))){
          keyTracker.Delete(double_hash(key, 1));
          //std::cout << "[seek] putting key: " << key;
          //printf(", value: %c%c%c%c\n", tmp_value[0], tmp_value[1], tmp_value[2], tmp_value[3]);
          ret_val = dbptr_array[0]->DB::Put(WriteOptions(), rocksdb::Slice(key), rocksdb::Slice(tmp_value));
          //std::cout << ret_val.ToString() << std::endl;
          //std::cout << "put success" << std::endl;
        }
        //  std::cout << "[seek] key: " << key << "not in" << std::endl;
        //}
        rollback_mtx.unlock();
        //startKey.clear();
        } else {
          flag = ilsm_db_.NextScan(iter_id, key, tmp_value);
          /*ilsm_db_.Get(key, tmp2);
          if (tmp2 != tmp_value){
            std::cout << "nextscan wrong: key: " << key << std::endl;
            printf("scan: %c%c%c%c\n", tmp_value[0], tmp_value[1], tmp_value[2], tmp_value[3]);
            printf("get: %c%c%c%c\n", tmp2[0], tmp2[1], tmp2[2], tmp2[3]);
          } else {
            std::cout << "is key: " << key << std::endl;
            printf("scan: %c%c%c%c\n", tmp_value[0], tmp_value[1], tmp_value[2], tmp_value[3]);
            printf("get: %c%c%c%c\n", tmp2[0], tmp2[1], tmp2[2], tmp2[3]);
          }*/
          //std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
          //std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
          //std::cout << "lock wait dur = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
          rollback_mtx.lock();
          if (flag == -3) {
            //std::cout << "reset ilsm (hashmap empty) " << keyTracker.count << std::endl;
            ilsm_db_.Reset();
            keyTracker.Clear();
            kache_flag = 0;
            rollback_flag = 0;
            flag = -2;
            rollback_mtx.unlock();
            //std::cout << "reset done" << std::endl;//
            //std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            //std::cout << "Time difference (reset) = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
            break;
          } else {
            if (keyTracker.Contains(double_hash(key, 1))){
              keyTracker.Delete(double_hash(key, 1));
              //std::cout << "[next] putting key: " << ++err_count << std::endl;
              //printf(", value: %c%c%c%c\n", tmp_value[0], tmp_value[1], tmp_value[2], tmp_value[3]);
              ret_val = dbptr_array[0]->DB::Put(WriteOptions(), rocksdb::Slice(key), rocksdb::Slice(tmp_value));
              //std::cout << ret_val.ToString() << std::endl;
              //std::cout << "put success" << std::endl;
            }
            rollback_mtx.unlock();
          }
        }
        Monitor_Consumer();
        //std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        //std::cout << "Time difference (rollback) = " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "[ns]" << std::endl;
      }
      std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
      std::cout << "Time difference (rollback) = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;
      reset_mtx.unlock();
    }
#endif
  }
}

int DB_MASTER::totalL0(void){
  return totalL0Num;
}

unsigned int DB_MASTER::totalCE(void){
  return totalCENum;
}
/* [KI] for BloomFilter */
uint64_t DB_MASTER::double_hash(const Slice& key, int n){
  uint64_t hashValue[2];
  MurmurHash3_x64_128(key.ToString().c_str(), 4, 0, hashValue);
  return (hashValue[0] + n * hashValue[1]);
}

void DB_MASTER::bloom_filter_init(BloomFilter *filter){
  memset(filter->bit_array,0,BLOOM_FILTER_SIZE*sizeof(size_t));
}

void DB_MASTER::bloom_filter_add(BloomFilter *filter, const Slice& key){
  for(int i = 0;i< HASH_FUNCTION;i++){
    uint64_t hash = double_hash(key,i);
    filter->bit_array[hash]++;
  }
}

int DB_MASTER::bloom_filter_check(BloomFilter *filter,const Slice& key){
  for(int i = 0;i < HASH_FUNCTION; i++){
    uint64_t hash = double_hash(key,i);
    //printf("%ld\n", filter->bit_array[hash]);
    if(filter->bit_array[hash] > 0){
      return 1; // possibly present
    }
  }
  return 0; // not present
}
void DB_MASTER::bloom_filter_delete(BloomFilter *filter,const Slice& key){
  for(int i=0;i<HASH_FUNCTION;i++){
    uint64_t hash = double_hash(key,i);
    if(filter->bit_array[hash] > 0){
      filter->bit_array[hash]--;
    }
  }
}
/* ------------------------------------------------------------------------- */
/*
void DB_MASTER::printstat(void){
  std::string stats;
  for(int i=0; i<num_instances; i++){
    dbptr_array[i]->GetProperty("rocksdb.stats", &stats);
    std::cout << "\nLSM-Shard " << i << std::endl;
    std::cout << stats << std::endl;

  }
  namespace fs = std::filesystem; 
  fs::path current_path = fs::current_path();

  std::ofstream outputFile1(fs::current_path() / "times_put.txt");

  if (outputFile1.is_open()) {
      for (const auto& time : times_put) {
          auto start = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.start).time_since_epoch().count();
          auto end = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.end).time_since_epoch().count();
          //outputFile1 << lltoa(start) << " " << std::to_string(end). << "\n";
          start -= 1700000000000000000;
          end -= 1700000000000000000;
          outputFile1 << start << " " << end << "\n";
      }
      outputFile1.close();
  } else {
      printf("[DEBUG_FLEX] file errer1\n");
  }

  std::ofstream outputFile2(fs::current_path() / "times_hash.txt");

  if (outputFile2.is_open()) {
      for (const auto& time : times_hash) {
          auto start = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.start).time_since_epoch().count();
          auto end = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.end).time_since_epoch().count();
          
          start -= 1700000000000000000;
          end -= 1700000000000000000;

          outputFile2 << start << " " << end << "\n";
      }
      outputFile2.close();
  } else {
      printf("[DEBUG_FLEX] file errer1\n");
  }

  std::ofstream outputFile3(fs::current_path() / "times_fb_tkv.txt");

  if (outputFile3.is_open()) {
      for (const auto& time : times_fb_tkv) {
          auto start = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.start).time_since_epoch().count();
          auto end = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.end).time_since_epoch().count();
          
          start -= 1700000000000000000;
          end -= 1700000000000000000;

          outputFile3 << start << " " << end << "\n";
      }
      outputFile3.close();
  } else {
      printf("[DEBUG_FLEX] file errer1\n");
  }

  std::ofstream outputFile4(fs::current_path() / "times_fb_ce.txt");
  if (outputFile4.is_open()) {
      for (const auto& time : times_fb_ce) {
          auto start = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.start).time_since_epoch().count();
          auto end = std::chrono::time_point_cast<std::chrono::nanoseconds>(time.end).time_since_epoch().count();
          
          start -= 1700000000000000000;
          end -= 1700000000000000000;

          outputFile4 << start << " " << end << "\n";
      }
      outputFile4.close();
  } else {
      printf("[DEBUG_FLEX] file errer1\n");
  }


  std::ofstream outputFile5(fs::current_path() / "times_get.txt");
  if (outputFile5.is_open()) {
      for (const auto& time : times_get) {
          auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(time.end - time.start).count();
          
          outputFile5 << duration << "\n";
      }
      outputFile5.close();
  } else {
      printf("[DEBUG_FLEX] file errer1\n");
  }


  std::ofstream outputFile6(fs::current_path() / "times_get_ce.txt");
  if (outputFile6.is_open()) {
      for (const auto& time : times_get_ce) {
          auto duration1 = std::chrono::duration_cast<std::chrono::nanoseconds>(time.mid - time.start).count();
          auto duration2 = std::chrono::duration_cast<std::chrono::nanoseconds>(time.end - time.mid).count();
          auto duration3 = std::chrono::duration_cast<std::chrono::nanoseconds>(time.end - time.start).count();
          outputFile6 << "CE "<< duration1 << " TKV " << duration2 << " TOTAL " << duration3 << "\n";
      }
      outputFile6.close();
  } else {
      printf("[DEBUG_FLEX] file errer1\n");
  }
  usleep(5000000); //5sec
}
*/
/*
void DB_MASTER::PutThroughCE(const WriteOptions& options, const Slice& key, const Slice& value){
  int primary_idx = hash_key(key);

  int fallback_idx;
  for(fallback_idx = 0; fallback_idx < num_instances; fallback_idx++){
    if(fallback_idx != primary_idx){
      break;
    }
  }

  ce_set_arr[primary_idx]->put(key.ToString(true), std::to_string(fallback_idx));

  dbptr_array[fallback_idx]->Put(options, key, Slice("a" + value.ToString()));
}
*/

}
