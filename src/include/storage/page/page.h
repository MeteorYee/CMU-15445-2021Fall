//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page.h
//
// Identification: src/include/storage/page/page.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>
#include <iostream>

#include "common/config.h"
#include "common/logger.h"
#include "common/rwlatch.h"
#include "common/spinlock.h"

namespace bustub {

/**
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
 */
class Page {
  // There is book-keeping information inside the page that should only be relevant to the buffer pool manager.
  friend class BufferPoolManagerInstance;

 public:
  /** Constructor. Zeros out the page data. */
  Page() { ResetMemory(); }

  /** Default destructor. */
  ~Page() = default;

  /** @return the actual data contained within this page */
  inline char *GetData() { return data_; }

  /** @return the page id of this page */
  inline page_id_t GetPageId() { return page_id_; }

  /** @return the pin count of this page */
  inline int GetPinCount() { return pin_count_; }

  /** @return true if the page in memory has been modified from the page on disk, false otherwise */
  inline bool IsDirty() { return is_dirty_; }

  /** Acquire the page write latch. */
  inline void WLatch() { rwlatch_.WLock(); }

  /** Release the page write latch. */
  inline void WUnlatch() { rwlatch_.WUnlock(); }

  /** Acquire the page read latch. */
  inline void RLatch() { rwlatch_.RLock(); }

  /** Release the page read latch. */
  inline void RUnlatch() { rwlatch_.RUnlock(); }

  /** Acquire the lock which protects the page meta info. */
  inline void MetaLock() { meta_latch_.Lock(); }

  /** Release the page meta lock. */
  inline void MetaUnLock() { meta_latch_.Unlock(); }

  /** @return the page LSN. */
  inline lsn_t GetLSN() { return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN); }

  /** Sets the page LSN. */
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }

  /**
   * @brief Mark the buffer page diirty when modifying the page. CAVEAT: The method should be
   * invoked only under the protection of page's write latch.
   */
  void MarkPageDirty() {
    MetaLock();
    assert(pin_count_ > 0);
    is_dirty_ = true;
    MetaUnLock();
  }

 protected:
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  static constexpr size_t SIZE_PAGE_HEADER = 8;
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN = 4;

 private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  char data_[PAGE_SIZE]{};
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page. */
  int pin_count_ = 0;
  /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
  bool is_dirty_ = false;
  /** Page latch. */
  ReaderWriterLatch rwlatch_;
  /** Page meta latch.
   *
   * The latch is used to protect the page's meta info, such as pin_count_, is_dirty, page_id, etc. N.B. This revision
   * violates the requirement of the cmu-15445-fall-2021 project 1. If you are a CMU student, refrain from doing this
   * (altough you shouldn't even look at this :D). All in all, this latch facilitates a more efficient concurrency
   * control upon page objects as it is a spin lock. In theory, We can use the page latch to protect the meta info,
   * but that's going to hurt the performance of accessing the page data itself.
   */
  SpinLock meta_latch_;
};

}  // namespace bustub
