//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.h
//
// Identification: src/include/buffer/buffer_pool_manager.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>

#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

/**
 * BufferPoolManager reads disk pages to and from its internal buffer pool.
 */
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  /**
   * Creates a new BufferPoolManagerInstance.
   * @param pool_size the size of the buffer pool
   * @param num_instances total number of BPIs in parallel BPM
   * @param instance_index index of this BPI in the parallel BPM
   * @param disk_manager the disk manager
   * @param log_manager the log manager (for testing only: nullptr = disable logging)
   */
  BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                            DiskManager *disk_manager, LogManager *log_manager = nullptr);

  /**
   * Destroys an existing BufferPoolManagerInstance.
   */
  ~BufferPoolManagerInstance() override;

  /** @return size of the buffer pool */
  size_t GetPoolSize() override { return pool_size_; }

  /** @return pointer to all the pages in the buffer pool */
  Page *GetPages() { return pages_; }

 protected:
  /**
   * Fetch the requested page from the buffer pool.
   * @param page_id id of page to be fetched
   * @return the requested page
   */
  Page *FetchPgImp(page_id_t page_id) override;

  /**
   * Unpin the target page from the buffer pool.
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise.
   *  Actually, we don't bother using this parameter, because it could be possible that
   *  someone has flushed the page before it is marked dirty. Then we would have a wrong
   *  hint bit. To solve this, we mark the page dirty under the protection of the page
   *  latch. Every page access that intends to modify the page should remember to mark
   *  the page dirty through this way.
   *
   * @return false if the page pin count is <= 0 before this call, true otherwise
   */
  bool UnpinPgImp(page_id_t page_id, __attribute__((unused)) bool is_dirty) override;

  /**
   * Flushes the target page to disk.
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  bool FlushPgImp(page_id_t page_id) override;

  /**
   * Creates a new page in the buffer pool.
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  Page *NewPgImp(page_id_t *page_id) override;

  /**
   * Deletes a page from the buffer pool.
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  bool DeletePgImp(page_id_t page_id) override;

  /**
   * Flushes all the pages in the buffer pool to disk.
   */
  void FlushAllPgsImp() override;

  /**
   * Allocate a page on disk.∂
   * @return the id of the allocated page
   */
  page_id_t AllocatePage();

  /**
   * Deallocate a page on disk.
   * @param page_id id of the page to deallocate
   */
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }

  /**
   * Validate that the page_id being used is accessible to this BPI. This can be used in all of the functions to
   * validate input data and ensure that a parallel BPM is routing requests to the correct BPI
   * @param page_id
   */
  void ValidatePageId(page_id_t page_id) const;

  void ResetPageMeta(Page *page, const page_id_t new_page_id);

  void InnerPageFlush(Page *page);

  frame_id_t FreeListGetFrame(page_id_t *page_id);

  frame_id_t ReplacerGetFrame(page_id_t *page_id);

  /* Invalid frame id */
  static constexpr frame_id_t INVALID_FRAME_ID = -1;

  /** Number of pages in the buffer pool. */
  const size_t pool_size_;
  /** How many instances are in the parallel BPM (if present, otherwise just 1 BPI) */
  const uint32_t num_instances_ = 1;
  /** Index of this BPI in the parallel BPM (if present, otherwise just 0) */
  const uint32_t instance_index_ = 0;
  /** Each BPI maintains its own counter for page_ids to hand out, must ensure they mod back to its instance_index_ */
  std::atomic<page_id_t> next_page_id_ = instance_index_;

  /** Array of buffer pool pages. */
  Page *pages_;
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. */
  LogManager *log_manager_ __attribute__((__unused__));
  /** Page table for keeping track of buffer pool pages. */
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  /** Replacer to find unpinned pages for replacement. */
  Replacer *replacer_;
  /** List of free pages. */
  std::list<frame_id_t> free_list_;
  /**
   * This latch protects shared data structures. We recommend updating this comment to describe what it protects.
   *
   * In the implementation, it only protects the coherence of the free list. To illustrate, the page table is
   * protected by the *table_mutex_* decalred below. The *replacer_* is thread-safe per se. Therefore, only the
   * free list needs to be protected by the latch.
   */
  std::mutex latch_;
  /**
   * This shared mutex protects the consistency of page table.
   *
   * The reason adding this mutex is to achieve a finer granularity of concurrency control comparing to the project
   * itself required. For example, the starter code here just gave us one *latch_* for the sake of a thread-safe
   * buffer pool (if I understand it correctly). In that way, we are going to protect the buffer pool manager as a
   * whole in order to prevent any race conditions. However, we can split the buffer manager into several parts and
   * protect them respectively, through which we can get a higher throughput of the buffer pool manager.
   */
  mutable std::shared_mutex table_mutex_;
};
}  // namespace bustub
