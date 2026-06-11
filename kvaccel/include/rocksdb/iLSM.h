#pragma once

#include <string>
#include <mutex>
#include <chrono>
#include <vector>

#define MAX_ITER_NUM 10

#define THREAD_SAFE_ILSM
//#define GET_FOR_SEEK_AND_NEXT_ILSM

#define BUFFERING_MODE
#ifdef BUFFERING_MODE
#define LIMIT_VALUE_BUFFER_ENTRY    128
#define MAX_VALUE_BUFFER_ENTRY      (341*1)
#define MAX_VALUE_BUFFER_SIZE       (MAX_VALUE_BUFFER_ENTRY * PAGE_SIZE)
#define BYTES_PER_KV_PAIR_OFFSET    12
#define MAX_KEY_VALUE_BUFFER_SIZE   (((MAX_VALUE_BUFFER_ENTRY*BYTES_PER_KV_PAIR_OFFSET/PAGE_SIZE) + 1)*PAGE_SIZE)
#endif

namespace iLSM {
    class DB{
        public:
            std::string iter_keys[MAX_ITER_NUM];
            std::string iter_values[MAX_ITER_NUM];

            DB() : fd_(-1) {
                op_stat.t.resize(8);
                op_stat.c.resize(8);
                passthru_stat.t.resize(9);
                passthru_stat.c.resize(9, 0);
            }
            int Open(const std::string &dev);
            int Put(const std::string &key, const std::string &value);
            int Get(const std::string &key, std::string &value);
            int CreateIter(unsigned int &iter_id);
            int Seek(const unsigned int iter_id, const std::string &key, std::string &value);
            int Next(const unsigned int iter_id, std::string &value);
            int DestroyIter(const unsigned int iter_id);
            ///////////////////////////////////////////////////// Bulk Interface
            int Reset();
            int SeekScan(const unsigned int iter_id, std::string &key, std::string &value);
            int NextScan(const unsigned int iter_id, std::string &key, std::string &value);
            unsigned int RangeScan(const unsigned int is_seek, const unsigned int iter_id, const std::string &key);
            int DoBulkyScan(const unsigned int is_seek, const unsigned int iter_id, const std::string &key, unsigned int *next);
            int DoBulkyNextScan(const unsigned int is_seek, const unsigned int iter_id, const unsigned int key, unsigned int *next);

            ///////////////////////////////////////////////////// Bulk Interface
            std::string Report();
        private:
            enum NvmeOpcode {
                NVME_CMD_KV_PUT                 = 0xA0,
                NVME_CMD_KV_GET                 = 0xA1,
                NVME_CMD_KV_DELETE              = 0xA2,
                NVME_CMD_KV_ITER_CREATE_ITER    = 0xA3,
                NVME_CMD_KV_ITER_SEEK           = 0xA4,
                NVME_CMD_KV_ITER_NEXT           = 0xA5,
                NVME_CMD_KV_ITER_DESTROY_ITER   = 0xA6,///////////////////
                NVME_CMD_KV_RANGE_SCAN_SEEK     = 0xA7,  // Bulk Interface
                NVME_CMD_KV_RANGE_SCAN_NEXT     = 0xA8,  // Bulk Interface
                //NVME_CMD_KV_LAST              = 0xA9,///////////////////
                NVME_CMD_KV_RESET               = 0xAA,
#ifdef BUFFERING_MODE
                NVME_CMD_V_FLUSH          	    = 0xAB,
                NVME_CMD_KV_FLUSH       	    = 0xAC,
#endif
                NVME_CMD_KV_LAST                = 0xAD,
            };
            
            enum class iLSMOp :int{
                Put             = 0,
                Get             = 1,
                CreateIter      = 2,
                Seek            = 3,
                Next            = 4,
                DestroyIter     = 5,
                RANGE_SCAN_SEEK = 6,
                RANGE_SCAN_NEXT = 7,
                LAST            = 8,
            };

            struct OP_STAT {
                std::vector<std::chrono::nanoseconds> t;
                std::vector<int> c;
            } op_stat;

            struct PASSTHRU_STAT {
                std::vector<std::chrono::nanoseconds> t;
                std::vector<int> c;
            } passthru_stat;

            int fd_;
            int cnt=0;
            #ifdef THREAD_SAFE_ILSM
                        std::mutex ioctl_mtx;
                        std::mutex op_stat_mtx;
                        std::mutex passthru_stat_mtx;
                        std::mutex report_mtx;
            #endif

#ifdef BUFFERING_MODE
            typedef struct _BUFFERING_INFO {
                bool is_init_required;
                void *value_log_addr;
                unsigned int value_log_offset;
                void *KV_pair_addr;
                unsigned int KV_pair_offset;
                unsigned int buffering_cnt; 
            } BUFFERING_INFO;
            BUFFERING_INFO buffering_info;
#endif

            inline int _Put(const std::string &key, const std::string &value);
            inline int _Get(const std::string &key, std::string &value);
            inline int _CreateIter(unsigned int &iter_id);
            inline int _Seek(const unsigned int iter_id, const std::string &key, std::string &value);
            inline int _Next(const unsigned int iter_id, std::string &value);
            inline int _DestroyIter(const unsigned int iter_id);
            ///////////////////////////////////////////////////// Bulk Interface
            inline int _SeekScan(const unsigned int iter_id, std::string &key, std::string &value);
            inline int _NextScan(const unsigned int iter_id, std::string &key, std::string &value);
            ///////////////////////////////////////////////////// Bulk Interface
#ifdef BUFFERING_MODE
            inline int _Buffering(const std::string &key, const std::string &value);
            inline int _Flush();
            inline int _ValueFlush();
            inline int _KeyValueFlush();
#endif
            int nvme_passthru(uint8_t opcode,
                    uint8_t flags, uint16_t rsvd, uint32_t nsid,
                    uint32_t cdw2, uint32_t cdw3, uint32_t cdw10, uint32_t cdw11,
                    uint32_t cdw12, uint32_t cdw13, uint32_t cdw14, uint32_t cdw15,
                    uint32_t data_len, void *data, uint32_t &result);

            void finishOp(const enum iLSMOp op, std::chrono::nanoseconds &d);
            void finishPassthru(const enum NvmeOpcode opcode, std::chrono::nanoseconds &d);

        #ifdef GET_FOR_SEEK_AND_NEXT_ILSM
                    // For Iterator Using Get()
                    struct _iterator {
                        unsigned int key;
                    };
                    
                    struct _iterator iter[MAX_ITER_NUM];
                    unsigned long long numGetofSeek=0;
                    unsigned long long numGetofNext=0;
        #endif
    };
}