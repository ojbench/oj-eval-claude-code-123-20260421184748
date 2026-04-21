#include "fstream.h"
#include <vector>
#include <cstring>
#include <stdexcept>

// 磁盘事件类型：正常、故障、更换
enum class EventType {
  NORMAL,   // 正常：所有磁盘工作正常
  FAILED,   // 故障：指定磁盘发生故障（文件被删除）
  REPLACED  // 更换：指定磁盘被更换（文件被清空）
};

class RAID5Controller {
private:
  std::vector<sjtu::fstream *> drives_;
  int blocks_per_drive_ = 0;
  int block_size_ = 0;
  int num_disks_ = 0;
  std::vector<bool> failed_; // 记录磁盘是否不可用（失败/不存在）

  // 每个条带（stripe）跨 num_disks_ 个磁盘，其中 1 个为校验盘
  // 第 s 个条带的校验盘索引：s % num_disks_
  inline int parity_index(int stripe) const { return stripe % num_disks_; }

  // 将逻辑块号映射到物理磁盘与条带号
  // 逻辑容量为 (num_disks_ - 1) * blocks_per_drive_
  // 对于逻辑块 b：条带 s=b/(n-1)，数据在该条带内的偏移 d=b%(n-1)
  // 物理磁盘索引 = (parity + 1 + d) % n
  inline void map_logical(int block_id, int &stripe, int &disk) const {
    if (block_id < 0 || block_id >= Capacity()) {
      throw std::out_of_range("block_id out of range");
    }
    stripe = block_id / (num_disks_ - 1);
    int d = block_id % (num_disks_ - 1);
    int p = parity_index(stripe);
    disk = (p + 1 + d) % num_disks_;
  }

  inline std::streampos offset_for(int stripe) const {
    return static_cast<std::streampos>(stripe) * static_cast<std::streampos>(block_size_);
  }

  bool drive_available(int i) const { return i >= 0 && i < num_disks_ && !failed_[i]; }

  void read_block_raw(int disk, int stripe, char *buf) {
    sjtu::fstream *fs = drives_[disk];
    fs->clear();
    fs->seekg(offset_for(stripe), std::ios::beg);
    fs->read(buf, block_size_);
    std::streamsize got = fs->gcount();
    if (got < block_size_) {
      std::memset(buf + got, 0, block_size_ - got);
    }
  }

  void write_block_raw(int disk, int stripe, const char *buf) {
    sjtu::fstream *fs = drives_[disk];
    fs->clear();
    fs->seekp(offset_for(stripe), std::ios::beg);
    fs->write(buf, block_size_);
    fs->flush();
  }

  // 计算某条带在排除某个磁盘后的按字节 XOR（用于恢复或计算校验）
  void xor_across_stripe(int stripe, int exclude_disk, char *out) {
    std::memset(out, 0, block_size_);
    std::vector<char> tmp(block_size_);
    for (int d = 0; d < num_disks_; ++d) {
      if (d == exclude_disk) continue;
      if (!drive_available(d)) continue; // 跳过不可用盘
      read_block_raw(d, stripe, tmp.data());
      for (int i = 0; i < block_size_; ++i) out[i] ^= tmp[i];
    }
  }

public:
  RAID5Controller(std::vector<sjtu::fstream *> drives, int blocks_per_drive,
                  int block_size = 4096)
      : drives_(std::move(drives)), blocks_per_drive_(blocks_per_drive),
        block_size_(block_size) {
    num_disks_ = static_cast<int>(drives_.size());
    failed_.assign(num_disks_, false);
  }

  /**
   * @brief 启动 RAID5 系统
   */
  void Start(EventType event_type_, int drive_id) {
    if (event_type_ == EventType::NORMAL) {
      std::fill(failed_.begin(), failed_.end(), false);
      return;
    }

    if (drive_id < 0 || drive_id >= num_disks_) {
      return;
    }

    if (event_type_ == EventType::FAILED) {
      failed_[drive_id] = true;
      return;
    }

    if (event_type_ == EventType::REPLACED) {
      failed_[drive_id] = false;
      std::vector<char> rebuilt(block_size_);
      for (int s = 0; s < blocks_per_drive_; ++s) {
        xor_across_stripe(s, drive_id, rebuilt.data());
        write_block_raw(drive_id, s, rebuilt.data());
      }
      return;
    }
  }

  void Shutdown() {
    for (auto *fs : drives_) {
      if (fs) {
        fs->flush();
        if (fs->is_open()) fs->close();
      }
    }
  }

  void ReadBlock(int block_id, char *result) {
    int stripe, disk;
    map_logical(block_id, stripe, disk);
    if (drive_available(disk)) {
      read_block_raw(disk, stripe, result);
      return;
    }
    xor_across_stripe(stripe, disk, result);
  }

  void WriteBlock(int block_id, const char *data) {
    int stripe, disk;
    map_logical(block_id, stripe, disk);
    int p = parity_index(stripe);

    std::vector<char> parity(block_size_);
    std::memset(parity.data(), 0, block_size_);

    std::vector<char> tmp(block_size_);
    for (int d = 0; d < num_disks_; ++d) {
      if (d == p) continue; // 跳过校验盘
      if (d == disk) {
        for (int i = 0; i < block_size_; ++i) parity[i] ^= data[i];
      } else {
        if (!drive_available(d)) {
          continue;
        }
        read_block_raw(d, stripe, tmp.data());
        for (int i = 0; i < block_size_; ++i) parity[i] ^= tmp[i];
      }
    }

    if (drive_available(disk)) {
      write_block_raw(disk, stripe, data);
    }

    if (drive_available(p)) {
      write_block_raw(p, stripe, parity.data());
    }
  }

  int Capacity() const { return (num_disks_ - 1) * blocks_per_drive_; }
};