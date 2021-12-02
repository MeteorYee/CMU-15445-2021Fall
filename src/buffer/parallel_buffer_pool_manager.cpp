//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(uint32_t num_instances, size_t pool_size,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : num_instances_{num_instances}, instance_pool_size_{pool_size} {
  // Allocate and create individual BufferPoolManagerInstances
  for (uint32_t i = 0; i < num_instances; i++) {
    buffer_pool_instances_.push_back(
        std::make_unique<BufferPoolManagerInstance>(pool_size, num_instances, i, disk_manager, log_manager));
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  // Actually, the smart pointers will take care of the space relaiming by itself.
  buffer_pool_instances_.clear();
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return static_cast<size_t>(num_instances_) * instance_pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return buffer_pool_instances_.at(page_id % num_instances_).get();
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  auto *bpm = GetBufferPoolManager(page_id);
  return bpm->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  auto *bpm = GetBufferPoolManager(page_id);
  return bpm->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  auto *bpm = GetBufferPoolManager(page_id);
  return bpm->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called

  // get the start index atomically
  uint32_t expected_start_index = start_index_.load(std::memory_order_relaxed);
  while (!std::atomic_compare_exchange_weak_explicit(&start_index_, &expected_start_index,
                                                     (expected_start_index + 1) % num_instances_,
                                                     std::memory_order_relaxed, std::memory_order_relaxed)) {
    expected_start_index = start_index_.load(std::memory_order_relaxed);
  }
  Page *page = nullptr;
  for (uint32_t i = 0; i < num_instances_; i++) {
    uint32_t index = (expected_start_index + i) % num_instances_;
    page = buffer_pool_instances_[index]->NewPage(page_id);
    if (page != nullptr) {
      return page;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  auto *bpm = GetBufferPoolManager(page_id);
  return bpm->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto &bpm : buffer_pool_instances_) {
    bpm->FlushAllPages();
  }
}

}  // namespace bustub
