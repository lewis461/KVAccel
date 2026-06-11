#include "rocksdb/iLSM.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/nvme_ioctl.h>
#include <fcntl.h>
#include <cstring>
#include <cstdint>
#include <stdlib.h>
#include <list>
#include <errno.h>
#include <iostream>

//#define DEBUG_iLSM

#ifdef DEBUG_iLSM
#include <iostream>
#include <cstdio>
#include <inttypes.h>
#endif

using namespace std;

const unsigned int PAGE_SIZE = 4096;
const unsigned int NSID = 96623821;
///////////////////////////////////////////////// Bulk Interface
const unsigned int MAX_CMD_NUM = 43691; // defined in memory_map.h of Host-Assisted Design
const unsigned int MAX_BUFLEN = 508*1024; // 512KB, Max Size of Value
const unsigned int BLOCK_SIZE = 4096;   // Size of NVMe Block (4KB)

/* # of iterators */
#define MAX_ITERATOR_NUM 10

/* Buffer for values retrieved during RangeScan execution */
list<string> values[MAX_ITERATOR_NUM];
list<string> keys[MAX_ITERATOR_NUM];
//list<uint32_t> raw_keys[MAX_ITERATOR_NUM];
unsigned int values_idx[MAX_ITERATOR_NUM];
unsigned int scan_cnt[MAX_ITERATOR_NUM];
unsigned int brk_point_key[MAX_ITERATOR_NUM];
// It doesn't allow concurrent accesses since here's only one global buffer

/* Serialization: Device-Side or Host-Side */
//#define DEVICE_SERIALIZATION     // Comment out if the Host-Assisted firmware is on

/* Internal Iterator: Stateless or Stateful */
//#define STATELESS_ITERATOR
#ifdef STATELESS_ITERATOR
unsigned int iter_id_idx;
#endif
///////////////////////////////////////////////// Bulk Interface

int iLSM::DB::Open(const std::string &dev)
{
    int err;
    err = open(dev.c_str(), O_RDONLY);
    if (err < 0)
        return -1; // fail to open
    fd_ = err;
    struct stat nvme_stat;
    err = fstat(fd_, &nvme_stat);
    if (err < 0)
        return -1;
    if (!S_ISCHR(nvme_stat.st_mode) && !S_ISBLK(nvme_stat.st_mode))
        return -1;
#ifdef BUFFERING_MODE
    buffering_info.is_init_required = true;
    buffering_info.buffering_cnt = 0;
    cout << "LIMIT_VALUE_BUFFER_ENTRY: "<<  LIMIT_VALUE_BUFFER_ENTRY << "\n";
#endif
    return 0;
}

int iLSM::DB::Put(const std::string &key, const std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _Put(key, value);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::Put, d);
    return ret;
}

int iLSM::DB::_Put(const std::string &key, const std::string &value)
{
#ifdef BUFFERING_MODE
    int ret = iLSM::DB::_Buffering(key, value);
    if(buffering_info.KV_pair_offset == LIMIT_VALUE_BUFFER_ENTRY)
        iLSM::DB::_Flush();
    return ret;
#endif
    void *data = NULL;
    unsigned int data_len = value.size();
    unsigned int nlb = ((data_len-1)/PAGE_SIZE); 
    data_len = (nlb+1) * PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memcpy(data, value.c_str(), value.size());
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = value.size();
    err = nvme_passthru(NVME_CMD_KV_PUT, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    free(data);
    if (err < 0) {
        return -1;
    } else if (result != 0) {
        return -1;
    }
    return 0;
}

#ifndef GET_FOR_SEEK_AND_NEXT_ILSM
int iLSM::DB::Seek(const unsigned int iter_id, const std::string &key, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _Seek(iter_id, key, value);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    // s[cnt++] = d;
    finishOp(iLSMOp::Seek, d);
    return ret;
}
#else

int iLSM::DB::Seek(const unsigned int iter_id, const std::string &key, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret;
    memcpy((void*) &iter[iter_id].key, (void*) key.c_str(), 4); 
    unsigned int i = iter[iter_id].key;
    while (1) {
        std::string k((char *)&i, 4); 
        numGetofSeek++;
        ret = _Get(k, value);
        if (ret != -2) // No Such Key
            break;
        else
            i++;
    }

    iter[iter_id].key = i;

    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::Seek, d);
    return ret;
}

#endif

int iLSM::DB::_Seek(const unsigned int iter_id, const std::string &key, std::string &value)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = iter_id;
    err = nvme_passthru(NVME_CMD_KV_ITER_SEEK, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);

    if (err < 0) {
        // ioctl fail
#ifdef DEBUG_iLSM
        perror("ilsm seek");
#endif
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

    if (result > 0) { // value length
        value = std::string(static_cast<const char*>(data), (int)result);
    }  else {
        value = std::string();
    }
    free(data);
    return result;
}

#ifndef GET_FOR_SEEK_AND_NEXT_ILSM
int iLSM::DB::Next(const unsigned int iter_id, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _Next(iter_id, value);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::Next, d);
    return ret;
}
#else
int iLSM::DB::Next(const unsigned int iter_id, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret;
    unsigned int i = iter[iter_id].key;
    i++;
    while (1) {
        std::string k((char *)&i, 4); 
        numGetofNext++;
        ret = _Get(k, value);
        if (ret != -2) // No Such Key
            break;
        else
            i++;
    }
    iter[iter_id].key = i;

    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::Next, d);
    return ret;
}
#endif

int iLSM::DB::_Next(const unsigned int iter_id, std::string &value)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    std::string key="1234";
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    cdw10 = 0; // key
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = iter_id;
    err = nvme_passthru(NVME_CMD_KV_ITER_NEXT, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);

    if (err < 0) {
        // ioctl fail
#ifdef DEBUG_iLSM
        perror("ilsm next");
#endif
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

    if (result > 0) { // value length
        value = std::string(static_cast<const char*>(data), (int)result);
    }  else {
        value = std::string();
    }

    free(data);
    return result;
}

int iLSM::DB::Get(const std::string &key, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _Get(key, value);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::Get, d);
    return ret;
}

int iLSM::DB::_Get(const std::string &key, std::string &value)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
    err = nvme_passthru(NVME_CMD_KV_GET, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);

    if (err < 0) {
        // ioctl fail
#ifdef DEBUG_iLSM
        perror("ilsm get");
#endif
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

    if (result > 0) { // value length
        value = std::string(static_cast<const char*>(data), (int)result);
    }  else {
        value = std::string();
    }

    free(data);
    return result;
}

#ifndef GET_FOR_SEEK_AND_NEXT_ILSM
int iLSM::DB::CreateIter(unsigned int &iter_id)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _CreateIter(iter_id);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::CreateIter, d);
    return ret;
}
#else
int iLSM::DB::CreateIter(unsigned int &iter_id)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _CreateIter(iter_id);

    if (ret >= 0)
        iter[iter_id].key = 0;
    
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::CreateIter, d);
    return ret;
}
#endif

int iLSM::DB::_CreateIter(unsigned int &iter_id)
{
#ifdef BUFFERING_MODE
    if(buffering_info.buffering_cnt != 0)
        iLSM::DB::_Flush();
#endif

    void *data = NULL;
    unsigned int data_len = 0;
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
#ifndef STATELESS_ITERATOR
    err = nvme_passthru(NVME_CMD_KV_ITER_CREATE_ITER, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
//    fprintf(stderr, "iLSM::DB::CreateIter => err=%d, result=%d\n", err, result);
    if (err < 0) {
        // ioctl fail
        perror("ilsm create iter");
        return -1;
    } 
    
    if (err == 0x7C1) {
        // no such key
        return -2;
    }
#else
    result = err = data_len = cdw2 = 0; data = &data_len; data = (unsigned int*)data + 1;
    iter_id = iter_id_idx++;
#endif  
    iter_id = result;
    return 0;
}

#ifndef GET_FOR_SEEK_AND_NEXT_ILSM
int iLSM::DB::DestroyIter(const unsigned int iter_id)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _DestroyIter(iter_id);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::DestroyIter, d);
    return ret;
}
#else
int iLSM::DB::DestroyIter(const unsigned int iter_id)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _DestroyIter(iter_id);

    if (ret >= 0)
       iter[iter_id].key = 0; 

    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    finishOp(iLSMOp::DestroyIter, d);
    return ret;
}
#endif

int iLSM::DB::_DestroyIter(const unsigned int iter_id)
{
    void *data = NULL;
    unsigned int data_len = 0;
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    //    memcpy(&cdw10, key.c_str()+4, 4);
    //    memcpy(&cdw11, key.c_str(), 4);
    cdw13 = iter_id;
#ifndef STATELESS_ITERATOR
    err = nvme_passthru(NVME_CMD_KV_ITER_DESTROY_ITER, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    if (err < 0) {
        // ioctl fail
        perror("ilsm destroy iter");
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        return -2;
    }
#else
    result = err = data_len = cdw2 = 0; data = &data_len; data = (unsigned int*)data + 1;
    iter_id_idx--;
#endif
    values[iter_id].clear();     // We must free value buffer!
    return result;
}

///////////////////////////////////////////////// Bulk Interface
int iLSM::DB::Reset()   
{
    keys->clear();
    values->clear();
    //raw_keys->clear();
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    err = nvme_passthru(NVME_CMD_KV_RESET, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    return err;
}

/* Interface wrapper for _SeekScan */
int iLSM::DB::SeekScan(const unsigned int iter_id, std::string &key, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _SeekScan(iter_id, key, value);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    // printf("SeekScan with iter_id(stateless debug): %u\n", iter_id);
    finishOp(iLSMOp::Seek, d);
    return ret;
}

/* Seek() interface for user application (RangeScan) */
int iLSM::DB::_SeekScan(const unsigned int iter_id, std::string &key, std::string &value)
{
    int err = -2;
    //uint32_t raw_key;

    // Let's scan. The first scan will be only 512KB(DMA capability)
    scan_cnt[iter_id] = 1;
    brk_point_key[iter_id] = RangeScan(NVME_CMD_KV_RANGE_SCAN_SEEK, iter_id, key);   // Maybe record the breakpoint
    //fprintf(stderr, "[brk]: %x\n", brk_point_key[iter_id]);
    if (values[iter_id].size() > 0)
    {   //fprintf(stderr, "(SeekScan with yes RangeScan)\n");
        value = std::string(values[iter_id].back());
        this->iter_values[iter_id] = value;
        values[iter_id].pop_back();
        key = std::string(keys[iter_id].front());
        this->iter_keys[iter_id] = key;
        keys[iter_id].pop_front();
        /*raw_key = raw_keys[iter_id].front();
        fprintf(stderr, "[k]: %d\n", raw_key);
        raw_keys[iter_id].pop_front();*/
        err = 1;
    }

    return err;
}

/* Interface wrapper for _NextScan */
int iLSM::DB::NextScan(const unsigned int iter_id, std::string &key, std::string &value)
{
    auto st = chrono::high_resolution_clock::now();
    int ret = _NextScan(iter_id, key, value);
    auto ed = chrono::high_resolution_clock::now();
    chrono::nanoseconds d = ed-st;
    // printf("NextScan with iter_id(stateless debug): %u\n", iter_id);
    finishOp(iLSMOp::Next, d);
    return ret;
}

/* Next() interface for user application (based on the RangeScan) */
int iLSM::DB::_NextScan(const unsigned int iter_id, std::string &key, std::string &value)
{
    int err = -1;
    //uint32_t raw_key;
    // Retrieve a value from the DRAM value buffer 'values'
    if (values[iter_id].size() > 0)
    {   //fprintf(stderr, "(NextScan with no RangeScan)\n");
        value = std::string(values[iter_id].back());
        this->iter_values[iter_id] = value;
        values[iter_id].pop_back();
        key = std::string(keys[iter_id].front());
        this->iter_keys[iter_id] = key;
        keys[iter_id].pop_front();
        /*raw_key = raw_keys[iter_id].front();
        fprintf(stderr, "[k]: %d\n", raw_key);
        raw_keys[iter_id].pop_front();*/
        err = 1;
    }
    else
    {   // If buffer is empty, then scan again from the breakpoint key
        // if (scan_cnt[iter_id] > 1)        // Adaptive Feature
        // {
        if (brk_point_key[iter_id] == 0xFFFFFFFF){
            fprintf(stderr, "end of the line\n");
            err = -3;
        } else {
            string empty;

            brk_point_key[iter_id] = RangeScan(NVME_CMD_KV_RANGE_SCAN_NEXT, iter_id, empty);
            if (values[iter_id].size() > 0)
            {   //fprintf(stderr, "(NextScan with yes RangeScan)\n");
                value = std::string(values[iter_id].back());
                values[iter_id].pop_back();
                key = std::string(keys[iter_id].front());
                keys[iter_id].pop_front();
                err = 1;
            }
            else err = -2; // No such key
            //fprintf(stderr, "[next brk]: %x\n", brk_point_key[iter_id]);
        }
        // }                                 // Adaptive Feature
        // else err = -2; // No such key     // Adaptive Feature
    }

    return err;
}

/* RangeScan with next-key-processing */
unsigned int iLSM::DB::RangeScan(const unsigned int is_seek, const unsigned int iter_id, const std::string &key)
{
    string start_key = key;
    unsigned int next, i = 0; 
    unsigned int start_uint = brk_point_key[iter_id];
    int err; // fprintf(stderr, "[ DoBulkyScan with scan_cnt: %d ]\n", scan_cnt[iter_id]);

    // Do 2^k times 512KB bulky DMA (power of 2 for each call)
    // for (i = 0; i < scan_cnt[iter_id]; i++)   // Adaptive Feature
    for (i = 0; i < 1; i++)
    {
        // if (is_seek == NVME_CMD_KV_RANGE_SCAN_SEEK && i == 0)    // Adaptive Feature
        if (is_seek == NVME_CMD_KV_RANGE_SCAN_SEEK)	
            err = DoBulkyScan(is_seek, iter_id, start_key, &next);
        else err = DoBulkyNextScan(is_seek, iter_id, start_uint, &next); 
        // Get a new start key for the next bulky scan
        // start_key = std::string(static_cast<const char*>((char*)&next), (int)4);
        start_uint = next;       // Only for the db_bench scheme!

	if (err == -2 || next == 0xFFFFFFFF)
        {   // If a calling sequence is over, then initialize scan count
            scan_cnt[iter_id] = 0xFFFFFFFF;
            break;
        }
    }
    // If current RangeScan call wasn't enough, then try double times next time
    // if (i == scan_cnt[iter_id] && !(err == -2 || next == 0xFFFFFFFF))     // Adaptive Feature
    // {                                                                     // Adaptive Feature
    //     if (scan_cnt[iter_id] < 4)        // Adaptive Feature (Doubling with Max Limit : 2MB)
    //         scan_cnt[iter_id] *= 2;                                       // Adaptive Feature
    // }                                                                     // Adaptive Feature

    return start_uint;
}

/* Do bulky scan up to maximum of 512KB */
int iLSM::DB::DoBulkyScan(const unsigned int is_seek, const unsigned int iter_id, const std::string &key, unsigned int *next)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
    
    // strncpy((char*)&cdw10, key.c_str(), 4);
    if (key != "start")
        memcpy(&cdw10, key.c_str(), 4);
    cdw12 = 0 | (0xFFFF & nlb);
#ifndef STATELESS_ITERATOR
    cdw13 = iter_id;
#else
    cdw13 = 0xFFFFFFFF;    // Default 512KB, that means, there's no max count limit now!
#endif
    err = nvme_passthru(is_seek, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result); //cdw10 == startkey addr, result == endkey addr
    // fprintf(stderr, "- BulkyScan performed...\n");

    if (err < 0) {
        // ioctl fail
        perror("ilsm bulkyScan");
        free(data);
        return -1;
    }   
    
    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }
     
#ifndef DEVICE_SERIALIZATION
    /* Host-Side Analysis of Bulky Scan (Host-Assisted & Stateful) */
    uint8_t *temp = (uint8_t*)data;
    vector<uint32_t> key_list; string temp_val, temp_key;
    uint32_t i = 0, key_list_idx = 0, key_list_cnt = 0, temp_len, temp_nlb;

    // 1st DMA part: <ValueLen|Key> Serialization
    while (i < MAX_CMD_NUM)
    {   // If it reaches the terminal, then stop!
        if (*((uint32_t*)temp) == 0) break;
        temp_len = *((uint32_t*)temp);       // Value Length
        key_list.push_back(temp_len);
        temp = (uint8_t*)temp + 4;
        temp_key = std::string(reinterpret_cast<const char *>((char*)temp), 4);
        //std::cout << "[bulk k]" << (*((uint32_t*)temp)) << std::endl;
        //raw_keys[iter_id].push_back((*((uint32_t*)temp)));
        keys[iter_id].push_back(temp_key);

        temp = (uint8_t*)temp + 4;
        key_list_cnt++; i++;
    }
    // 2nd DMA part: Value-Unit-DMAs
    if (*((unsigned int*)temp) == 0)
    {   // Align to SECTOR_SIZE since 1st & 2nd DMA occurs separately
        temp = (uint8_t*)temp + (BLOCK_SIZE - ((uintptr_t)temp) % BLOCK_SIZE);
        while (key_list_idx < key_list_cnt)
        {
            // For each key_list entry, retrive corresponding value
            temp_len = key_list[key_list_idx];
            temp_val = std::string(static_cast<const char*>((char*)temp), (int)(temp_len));   // Value
            //std::cout << "[v]" << temp_val << std::endl;
            values[iter_id].push_back(temp_val);
                
            // Advance
            temp_nlb = (temp_len / BLOCK_SIZE) + ((temp_len % BLOCK_SIZE) > 0 ? 1 : 0);
            temp = (uint8_t*)temp + (temp_nlb * BLOCK_SIZE);
            key_list_idx++;
        }
    }
#else
    /* Host-Side Analysis of Bulky Scan (Stateless Design) */
    uint8_t *temp = (uint8_t*)data;
    string temp_val; uint32_t i = 0, temp_len; 

    while (i < MAX_CMD_NUM)
    {   // If it reaches the terminal, then stop!
        if (*((unsigned int*)temp) == 0) break;
	            
        temp_len = *((uint32_t*)temp);       // Value Length
        temp_val = std::string(static_cast<const char*>((char*)(temp + 4)), (int)(temp_len));		    
        values[iter_id].push_back(temp_val);

        // Advance
        temp_len = temp_len + (4 - temp_len % 4);
        temp = (uint8_t*)temp + (4 + temp_len); i++;
    }
#endif

    /* report the next key! */
    *next = result;  // If it's not zero, then recursion will occur!
    
    free(data);
    return result;
}

/* Do bulky scan up to maximum of 512KB (for the NextScan only): Only for db_bench scheme */
int iLSM::DB::DoBulkyNextScan(const unsigned int is_seek, const unsigned int iter_id, const unsigned int key, unsigned int *next)
{
    void *data = NULL;
    unsigned int data_len = MAX_BUFLEN;
    unsigned int nlb = (MAX_BUFLEN-1) / PAGE_SIZE;
    if (posix_memalign(&data, PAGE_SIZE, data_len)) {
        return -ENOMEM;
    }
    memset(data, 0, data_len);
    int err;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;

    cdw10 = key;
    cdw12 = 0 | (0xFFFF & nlb);
#ifndef STATELESS_ITERATOR
    cdw13 = iter_id;
#else
    cdw13 = 0xFFFFFFFF;    // Default 512KB, that means, there's no max count limit now!
#endif
    err = nvme_passthru(is_seek, 0, 0, NSID, cdw2, cdw3,
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15,
            data_len, data, result);
    // fprintf(stderr, "- BulkyNextScan performed...\n");

    if (err < 0) {
        // ioctl fail
        perror("ilsm bulkyNextScan");
        free(data);
        return -1;
    }

    if (err == 0x7C1) {
        // no such key
        free(data);
        return -2;
    }

#ifndef DEVICE_SERIALIZATION
    /* Host-Side Analysis of Bulky Scan (Host-Assisted & Stateful) */
    uint8_t *temp = (uint8_t*)data;
    vector<uint32_t> key_list; string temp_val, temp_key; 
    uint32_t i = 0, key_list_idx = 0, key_list_cnt = 0, temp_len, temp_nlb;

    // 1st DMA part: <ValueLen> Serialization
    while (i < MAX_CMD_NUM)
    {   // If it reaches the terminal, then stop!
        if (*((uint32_t*)temp) == 0) break;

        temp_len = *((uint32_t*)temp);       // Value Length
        key_list.push_back(temp_len);
        temp = (uint8_t*)temp + 4;
        //std::cout << "[bulknext k]" << (*((uint32_t*)temp)) << std::endl;
        temp_key = std::string(static_cast<const char*>((char*)temp), 4);
        //raw_keys[iter_id].push_back((*((uint32_t*)temp)));
        keys[iter_id].push_back(temp_key);

        temp = (uint8_t*)temp + 4;
        key_list_cnt++; i++;
    }
    // 2nd DMA part: Value-Unit-DMAs
    if (*((unsigned int*)temp) == 0)
    {   // Align to SECTOR_SIZE since 1st & 2nd DMA occurs separately
        temp = (uint8_t*)temp + (BLOCK_SIZE - ((uintptr_t)temp) % BLOCK_SIZE);
        while (key_list_idx < key_list_cnt)
        {
            // For each key_list entry, retrive corresponding value
            temp_len = key_list[key_list_idx];
            temp_val = std::string(static_cast<const char*>((char*)temp), (int)(temp_len));     // Value
            values[iter_id].push_back(temp_val);
                
            // Advance
            temp_nlb = (temp_len / BLOCK_SIZE) + ((temp_len % BLOCK_SIZE) > 0 ? 1 : 0);
            temp = (uint8_t*)temp + (temp_nlb * BLOCK_SIZE);
            key_list_idx++;
        }
    }
#else
    /* Host-Side Analysis of Bulky Scan (Stateless Design) */
    uint8_t *temp = (uint8_t*)data;
    string temp_val; uint32_t i = 0, temp_len; 

    while (i < MAX_CMD_NUM)
    {   // If it reaches the terminal, then stop!
        if (*((unsigned int*)temp) == 0) break;
	            
        temp_len = *((uint32_t*)temp);       // Value Length
        temp_val = std::string(static_cast<const char*>((char*)(temp + 4)), (int)(temp_len));		    
        values[iter_id].push_back(temp_val);

        // Advance
        temp_len = temp_len + (4 - temp_len % 4);
        temp = (uint8_t*)temp + (4 + temp_len); i++;
    }
#endif

    /* report the next key! */
    *next = result;  // If it's not zero, then recursion will occur!

    free(data);
    return result;
}
///////////////////////////////////////////////// Bulk Interface

#ifdef BUFFERING_MODE
int iLSM::DB::_Buffering(const std::string &key, const std::string &value)
{
    BUFFERING_INFO *bip = &buffering_info;
#ifdef DEBUG_iLSM
        cout << "buffering start\n";
#endif
    //init
    if(bip->is_init_required) {
        if (posix_memalign(&bip->value_log_addr, PAGE_SIZE, MAX_VALUE_BUFFER_SIZE)) return -ENOMEM;
        if (posix_memalign(&bip->KV_pair_addr, PAGE_SIZE, MAX_KEY_VALUE_BUFFER_SIZE)) return -ENOMEM;
        bip->value_log_offset = 0;
        bip->KV_pair_offset = 0;
        bip->is_init_required = false;
        bip->buffering_cnt = 0;
#ifdef DEBUG_iLSM
        cout << "buffering init\n";
#endif
    }
    unsigned int nlb = ((value.size() - 1) / PAGE_SIZE);

    //value insert
    unsigned V_offset = bip->value_log_offset * PAGE_SIZE;
    char* V_addr = (char *)bip->value_log_addr + V_offset;
    strncpy(V_addr, value.c_str(), value.size());

    //KV pair insert
    unsigned KV_offset = bip->KV_pair_offset * BYTES_PER_KV_PAIR_OFFSET;
    unsigned int *KV_addr = (unsigned int*)((char *)bip->KV_pair_addr + KV_offset);
    memcpy(KV_addr, key.c_str(), 4);
    memcpy(KV_addr + 1, (const void *)&bip->value_log_offset, 4);
    uint32_t value_size = value.size();
    memcpy(KV_addr + 2, (const void *)&value_size, 4);
    //cout << '(' << std::hex << *KV_addr << std::dec << ' ' << *(KV_addr + 1) << ' ' << *(KV_addr + 2) << ')';

    bip->KV_pair_offset++;
    bip->value_log_offset += nlb + 1;
    bip->buffering_cnt++;
#ifdef DEBUG_iLSM
    cout << "buffering_cnt: " << bip->buffering_cnt << '\n';
#endif
    return 0;
}

int iLSM::DB::_ValueFlush(void)
{
    BUFFERING_INFO *bip = &buffering_info;
    
    void *data = bip->value_log_addr;
    unsigned int *KV_addr = (unsigned int*)((char *)bip->KV_pair_addr + (bip->KV_pair_offset - 1)* BYTES_PER_KV_PAIR_OFFSET);
    unsigned int total_data_len = *(KV_addr + 1) * PAGE_SIZE + *(KV_addr + 2);
    unsigned int v_cnt = bip->value_log_offset;
    unsigned int data_len, nlb;


#ifdef DEBUG_iLSM
    cout << "value flush start\n";
    cout << "KV_addr: " << KV_addr << '\n';
    cout << *KV_addr << ' ' << *(KV_addr + 1) << ' ' << *(KV_addr + 2) << '\n';
    //cout << "total_data_len: " << *(KV_addr + 1) << "*" << PAGE_SIZE << " + " << *(KV_addr + 2) << " = "<< total_data_len << '\n';
#endif

    int err = 0;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;

    for(unsigned int i = 0; v_cnt > i * LIMIT_VALUE_BUFFER_ENTRY; i++) {
        //cout << "_ValueFlush start";
        data = (void *)((char *)bip->value_log_addr + (i * LIMIT_VALUE_BUFFER_ENTRY * PAGE_SIZE));
        if(v_cnt <= (i + 1)*LIMIT_VALUE_BUFFER_ENTRY) {
            cdw13 = total_data_len - (i * LIMIT_VALUE_BUFFER_ENTRY * PAGE_SIZE);
        }
        else
            cdw13 = (LIMIT_VALUE_BUFFER_ENTRY * PAGE_SIZE);
        //cout << data << " ";
        //cout << cdw13 << " ";
        nlb = ((cdw13 - 1) / PAGE_SIZE);
        //cout << nlb << " ";
        data_len = (nlb + 1) * PAGE_SIZE;
        //cout << data_len << " ";
        cdw12 = 0 | (0xFFFF & nlb);
        //cout << cdw12 << " ";
        cdw14 = !i;
        //cout << cdw14 << " ";
        err = nvme_passthru(NVME_CMD_V_FLUSH, 0, 0, NSID, cdw2, cdw3, 
                cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
                data_len, data, result);  
        //cout << " end(" << result << ", " << err << ", " << errno << ")\n";
        if (err < 0)
            return -1;
    }
#ifdef DEBUG_iLSM
    cout << "value flush end\n";
#endif
    return 0;
}

int iLSM::DB::_KeyValueFlush(void)
{
    BUFFERING_INFO *bip = &buffering_info;

    int err = 0;
    uint32_t result;
    uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
    cdw2 = cdw3 = cdw10  = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;

#ifdef DEBUG_iLSM
    cout << "key value flush start\n";
#endif

    void *data = bip->KV_pair_addr;
    unsigned int data_len = bip->KV_pair_offset * BYTES_PER_KV_PAIR_OFFSET;
    unsigned int nlb = (data_len - 1) / PAGE_SIZE;
    cdw12 = 0 | (0xFFFF & nlb);
    cdw13 = data_len;
    data_len = (nlb + 1) * PAGE_SIZE;

    //cout << data << ' ' << nlb << ' ' << cdw13 << ' ' << data_len << " ";

    err = nvme_passthru(NVME_CMD_KV_FLUSH, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
            data_len, data, result);
    //cout << " end(" << result << ", " << err << ", " << errno << ")\n";

    if (err < 0)
        return -2;
#ifdef DEBUG_iLSM
    cout << "key value flush end\n";
#endif
    return 0;
}

int iLSM::DB::_Flush(void) {
#ifdef DEBUG_iLSM
    cout << "flush start\n";
#endif
     if(_ValueFlush())
        cout << "_ValueFlush error\n";
    if(_KeyValueFlush())
        cout << "_KeyValueFlush error\n";

    free(buffering_info.KV_pair_addr);
    free(buffering_info.value_log_addr);

    buffering_info.is_init_required = true;
    buffering_info.KV_pair_addr = NULL;
    buffering_info.value_log_addr = NULL;
    buffering_info.KV_pair_offset = 0;
    buffering_info.value_log_offset = 0;
    buffering_info.buffering_cnt = 0;
#ifdef DEBUG_iLSM
    cout << "flush end\n";
#endif
    //cout << buffering_info.buffering_cnt << '\n';
    return 0;
}
#endif

int iLSM::DB::nvme_passthru(uint8_t opcode,
        uint8_t flags, uint16_t rsvd,
        uint32_t nsid, uint32_t cdw2, uint32_t cdw3, uint32_t cdw10, uint32_t cdw11,
        uint32_t cdw12, uint32_t cdw13, uint32_t cdw14, uint32_t cdw15,
        uint32_t data_len, void *data, uint32_t &result)
{
    auto st = chrono::high_resolution_clock::now();
    struct nvme_passthru_cmd cmd = {
        .opcode		= opcode,
        .flags		= flags,
        .rsvd1		= rsvd,
        .nsid		= nsid,
        .cdw2		= cdw2,
        .cdw3		= cdw3,
        .metadata	= (uint64_t)(uintptr_t) NULL,
        .addr		= (uint64_t)(uintptr_t) data,
        .metadata_len	= 0,
        .data_len	= data_len,
        .cdw10		= cdw10,
        .cdw11		= cdw11,
        .cdw12		= cdw12,
        .cdw13		= cdw13,
        .cdw14		= cdw14,
        .cdw15		= cdw15,
        .timeout_ms	= 0,
        .result		= 0,
    };
    int err;
#ifdef DEBUG_iLSM
    {/*
        fprintf(stderr, "-- iLSM::DB::nvme_passthru --\n");
        fprintf(stderr, "opcode       : %02x\n", cmd.opcode);
        fprintf(stderr, "flags        : %02x\n", cmd.flags);
        fprintf(stderr, "rsvd1        : %04x\n", cmd.rsvd1);
        fprintf(stderr, "nsid         : %08x\n", cmd.nsid);
        fprintf(stderr, "cdw2         : %08x\n", cmd.cdw2);
        fprintf(stderr, "cdw3         : %08x\n", cmd.cdw3);
        fprintf(stderr, "data_len     : %08x\n", cmd.data_len);
        fprintf(stderr, "metadata_len : %08x\n", cmd.metadata_len);
        fprintf(stderr, "addr         : %llx\n", cmd.addr);
        fprintf(stderr, "metadata     : %llx\n", cmd.metadata);
        fprintf(stderr, "cdw10        : %08x\n", cmd.cdw10);
        fprintf(stderr, "cdw11        : %08x\n", cmd.cdw11);
        fprintf(stderr, "cdw12        : %08x\n", cmd.cdw12);
        fprintf(stderr, "cdw13        : %08x\n", cmd.cdw13);
        fprintf(stderr, "cdw14        : %08x\n", cmd.cdw14);
        fprintf(stderr, "cdw15        : %08x\n", cmd.cdw15);
        fprintf(stderr, "timeout_ms   : %08x\n", cmd.timeout_ms); */
    }
#endif
    {
#ifdef THREAD_SAFE_ILSM
        lock_guard<mutex> l(ioctl_mtx);
#endif
            err = ioctl(fd_, NVME_IOCTL_IO_CMD, &cmd);
    }

    ///////////////////////////////////////////////////////// Bulk Interface
    if (!err && (opcode < NVME_CMD_KV_LAST || opcode == NVME_CMD_KV_RESET || opcode == NVME_CMD_KV_RANGE_SCAN_SEEK || opcode == NVME_CMD_KV_RANGE_SCAN_NEXT)) {
        result = cmd.result; 
        auto ed = chrono::high_resolution_clock::now();
        chrono::nanoseconds d = ed-st;
        finishPassthru(static_cast<enum NvmeOpcode>(opcode), d);
    } 
    else {
        fprintf(stderr, "opcode:%X iLSM::PASSTHRU Error %d\n", opcode, errno);
    }
    ///////////////////////////////////////////////////////// Bulk Interface

    return err;
}


void iLSM::DB::finishOp(const enum iLSMOp op, chrono::nanoseconds &d)
{
#ifdef THREAD_SAFE_ILSM
    lock_guard<mutex> l(op_stat_mtx);
#endif
    int idx = static_cast<int>(op);
    op_stat.t[idx] += d;
    op_stat.c[idx] ++;
}

void iLSM::DB::finishPassthru(const enum NvmeOpcode opcode, chrono::nanoseconds &d)
{
#ifdef THREAD_SAFE_ILSM
    lock_guard<mutex> l(passthru_stat_mtx);
#endif
    int idx = opcode - NvmeOpcode::NVME_CMD_KV_PUT;
    passthru_stat.t[idx] += d;
    passthru_stat.c[idx] ++;
}

string iLSM::DB::Report()
{
#ifdef THREAD_SAFE_ILSM
    lock_guard<mutex> l(report_mtx);
#endif
    string msg;
    for (int i = 0; i < static_cast<int>(iLSMOp::LAST); i++) {
        
        if (!op_stat.t[i].count())
            continue;

        switch(static_cast<enum iLSMOp>(i)) {
            case iLSMOp::Put:
                msg += "[Put] ";
                break;
            case iLSMOp::Get:
                msg += "[Get] ";
                break;
            case iLSMOp::CreateIter:
                msg += "[CreateIter] ";
                break;
            case iLSMOp::Seek:
                msg += "[Seek] ";
                break;
            case iLSMOp::Next:
                msg += "[Next] ";
                break;
            case iLSMOp::DestroyIter:
                msg += "[DestroyIter] ";
                break;
            default:
                msg += "[????] ";
        }
        double total, avg;
        total = (double) op_stat.t[i].count() / 1000;
        avg = total / op_stat.c[i];
        msg += "Elapse Time " + to_string (total) + " us / " + to_string(op_stat.c[i]) + " = Average " + to_string(avg) + " us \n";
    }
    for (int i = 0; i < (static_cast<int>(NvmeOpcode::NVME_CMD_KV_LAST) - 0xA0); i++) {

        if (!passthru_stat.t[i].count())
            continue;

        switch(static_cast<enum NvmeOpcode>(i + 0xA0)) {
            case NVME_CMD_KV_PUT:
                msg += "[NVME_CMD_KV_PUT] ";
                break;
            case NVME_CMD_KV_GET:
                msg += "[NVME_CMD_KV_GET] ";
                break;
            case NVME_CMD_KV_DELETE:
                msg += "[NVME_CMD_KV_DELETE] ";
                break;
            case NVME_CMD_KV_ITER_CREATE_ITER:
                msg += "[NVME_CMD_KV_ITER_CREATE_ITER] ";
                break;
            case NVME_CMD_KV_ITER_SEEK: case NVME_CMD_KV_RANGE_SCAN_SEEK:
                msg += "[NVME_CMD_KV_ITER_SEEK] ";
                break;
	    case NVME_CMD_KV_ITER_NEXT: case NVME_CMD_KV_RANGE_SCAN_NEXT:
                msg += "[NVME_CMD_KV_ITER_NEXT] ";
                break;
            case NVME_CMD_KV_ITER_DESTROY_ITER:
                msg += "[NVME_CMD_KV_ITER_DESTROY_ITER] ";
                break;
            default:
                msg += "[???] ";
        }

        double total, avg;
        total = (double) passthru_stat.t[i].count() / 1000;
        avg = total / passthru_stat.c[i];
        msg += "Elapse Time " + to_string (total) + " us / " + to_string(passthru_stat.c[i]) + " = Average " + to_string(avg) + " us \n";

    }
 
    {
        void *data = NULL;
        unsigned int data_len = 0;
        int err;
        uint32_t result;
        uint32_t cdw2, cdw3, cdw10, cdw11, cdw12, cdw13, cdw14, cdw15;
        cdw2 = cdw3 = cdw10 = cdw11 = cdw12 = cdw13 = cdw14 = cdw15 = 0;
        err = nvme_passthru(NVME_CMD_KV_LAST, 0, 0, NSID, cdw2, cdw3, 
            cdw10, cdw11, cdw12, cdw13, cdw14, cdw15, 
                data_len, data, result);
        if (err < 0) {
            // ioctl fail
            perror("ilsm report failed");
        }
     
    }
    return msg;
}
