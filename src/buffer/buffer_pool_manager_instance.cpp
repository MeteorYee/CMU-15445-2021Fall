//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  Page *page = nullptr;
  frame_id_t frame_id = INVALID_FRAME_ID;
  int old_pin_count;

  {
    std::shared_lock lock(table_mutex_);
    if (page_table_.count(page_id) == 0) {
      LOG_ERROR("Try to flush a non-existing page! page id = %d.", page_id);
      return false;
    }
    frame_id = page_table_[page_id];
    page = pages_ + frame_id;
    page->MetaLock();
    if (!page->is_dirty_) {
      // no need to flush the page
      page->MetaUnLock();
      return true;
    }
    old_pin_count = page->pin_count_;
    page->pin_count_++;
    page->just_dirtied = false;
    page->MetaUnLock();
  }

  // remove the frame from the lru list if it applies.
  if (old_pin_count == 0) {
    replacer_->Pin(frame_id);
  }

  page->RLatch();
  disk_manager_->WritePage(page_id, page->data_);
  page->RUnlatch();

  // remember to unpin it
  page->MetaLock();
  old_pin_count = page->pin_count_;
  page->pin_count_--;
  if (!page->just_dirtied) {
    page->is_dirty_ = false;
  }
  // else: TBD: someone might've re-dirtied the page, may need re-flush it again?
  page->MetaUnLock();

  if (old_pin_count == 1) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it! - done
  // acquire the shared table lock to prevent any possible revisions
  Page *page = nullptr;
  bool is_dirty = false;
  std::shared_lock lock(table_mutex_);
  for (const auto &pair : page_table_) {
    page = pages_ + pair.second;

    page->MetaLock();
    assert(page->GetPageId() == pair.first);
    is_dirty = page->is_dirty_;
    page->just_dirtied = false;
    page->MetaUnLock();

    if (!is_dirty) {
      // no need to flush the page
      continue;
    }

    page->RLatch();
    disk_manager_->WritePage(pair.first, page->data_);
    page->RUnlatch();

    page->MetaLock();
    if (!page->just_dirtied) {
      page->is_dirty_ = false;
    }
    // else: TBD: someone might've re-dirtied the page, may need re-flush it again?
    page->MetaUnLock();
  }
  /* We don't bother doing pin and unpin up above because once the page table is protected by the table lock,
   * there is no chance for a page to be evicted. */
}

// TBD: the upper level logic should take care of page extending and prevent redundant new pages
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  frame_id_t frame_id = INVALID_FRAME_ID;
  page_id_t ret_page_id = INVALID_PAGE_ID;

  // try the free list at first
  frame_id = FreeListGetFrame(&ret_page_id);

  if (frame_id == INVALID_FRAME_ID) {
    // no available slots in the free list, try page replacement
    frame_id = ReplacerGetFrame(&ret_page_id);
  }

  if (frame_id != INVALID_FRAME_ID) {
    *page_id = ret_page_id;
    return pages_ + frame_id;
  }
  return nullptr;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  int old_pin_count = 0;

  {
    std::shared_lock lock(table_mutex_);
    if (page_table_.count(page_id) > 0) {
      // it's already there
      frame_id = page_table_[page_id];
      page = pages_ + frame_id;

      page->MetaLock();
      assert(page->GetPageId() == page_id);
      old_pin_count = page->pin_count_;
      page->pin_count_++;
      page->MetaUnLock();
    }
  }

  /* The replacer_.pin() lagged behind here may risk the frame being selected by page replacement
   * process. However, the replacement algorithm will recheck the pin_count before it decides a
   * victim. Hence, here it is.*/
  if (page != nullptr) {
    if (old_pin_count == 0) {
      replacer_->Pin(frame_id);
    }
    return page;
  }

  // the page doesn't exist and seek in the free list firstly
  frame_id = FreeListGetFrame(&page_id);

  if (frame_id == INVALID_FRAME_ID) {
    // no available slots in the free list, try page replacement
    frame_id = ReplacerGetFrame(&page_id);
  }

  if (frame_id != INVALID_FRAME_ID) {
    return pages_ + frame_id;
  }
  return nullptr;
}

// TBD: do we need to check and flush a potential dirty page?
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  int pin_count;

  {
    std::unique_lock lock(table_mutex_);
    if (page_table_.count(page_id) == 0) {
      // doesn't exist
      return false;
    }
    frame_id = page_table_[page_id];
    page = pages_ + frame_id;

    page->MetaLock();
    assert(page->GetPageId() == page_id);
    pin_count = page->pin_count_;
    // check dirty ?
    page->MetaUnLock();

    if (pin_count > 0) {
      // already in use
      return false;
    }

    // delete the page
    page_table_.erase(page_id);
    DeallocatePage(page_id);
  }
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;

  {
    std::shared_lock lock(table_mutex_);
    if (page_table_.count(page_id) == 0) {
      LOG_ERROR("Unpin a non-existing page! page id = %d.", page_id);
      return false;
    }
    frame_id = page_table_[page_id];
    page = pages_ + frame_id;
  }

  // Unpin is not so critical as pin, hence we can put it outside the scope of table lock
  int pin_count;
  page->MetaLock();
  assert(page->GetPageId() == page_id);
  if (page->pin_count_ <= 0) {
    LOG_ERROR("Trying to unpin a page with pin_count <= 0, page id = %d.", page_id);
    page->MetaUnLock();
    return false;
  }
  page->pin_count_--;
  page->is_dirty_ = is_dirty;
  page->just_dirtied = is_dirty;
  pin_count = page->pin_count_;
  page->MetaUnLock();

  if (pin_count == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

void BufferPoolManagerInstance::ResetPageMeta(Page *page, const page_id_t new_page_id) {
  page->page_id_ = new_page_id;
  page->is_dirty_ = false;
  page->just_dirtied = false;
  // The page is just created or victimed, so we don't need to call replacer_.Pin() cuz it's
  // definitely not in the replacer.
  page->pin_count_ = 1;
}

frame_id_t BufferPoolManagerInstance::FreeListGetFrame(page_id_t *page_id) {
  frame_id_t frame_id;
  {
    std::scoped_lock lock(latch_);
    if (free_list_.empty()) {
      return INVALID_FRAME_ID;
    }
    frame_id = free_list_.front();
    free_list_.pop_front();
  }

  Page *page = pages_ + frame_id;
  page_id_t new_page_id = *page_id;
  bool needs_io = true;
  {
    std::unique_lock lock(table_mutex_);
    // this depends on the logics of DeallocatePage()
    if (new_page_id == INVALID_PAGE_ID) {
      new_page_id = AllocatePage();
      *page_id = new_page_id;
      needs_io = false;
    }
    if (page_table_.count(new_page_id) == 0) {
      page_table_.emplace(new_page_id, frame_id);

      page->MetaLock();
      ResetPageMeta(page, new_page_id);
      page->MetaUnLock();

      // N.B. we unlatch it after we release the table lock
      page->WLatch();
    } else {
      // if someone has already done the same thing, then just insert the free frame back
      std::scoped_lock lock(latch_);
      free_list_.emplace_back(frame_id);
      return page_table_[new_page_id];
      // TBD: is it guaranteed this is not the NewPage() case? otherwise we will waste a page id.
    }
  }

  if (needs_io) {
    disk_manager_->ReadPage(new_page_id, page->GetData());
  } else {
    page->ResetMemory();
  }
  page->WUnlatch();
  return frame_id;
}

frame_id_t BufferPoolManagerInstance::ReplacerGetFrame(page_id_t *page_id) {
  page_id_t old_page_id;
  page_id_t new_page_id;

  frame_id_t frame_id = INVALID_FRAME_ID;
  Page *page = nullptr;
  bool already_exists = false;

  for (;;) {
    if (!replacer_->Victim(&frame_id)) {
      // no victims
      return INVALID_FRAME_ID;
    }

    new_page_id = *page_id;
    bool is_dirty = false;
    bool needs_io = true;
    // got one victim
    {
      std::unique_lock lock(table_mutex_);

      /* This depends on the logics of DeallocatePage(). If it can guarantee the atomicity, this can be moved
       * up above, out of the scope of table lock. */
      if (new_page_id == INVALID_PAGE_ID) {
        new_page_id = AllocatePage();
        *page_id = new_page_id;
        needs_io = false;
      }

      page = pages_ + frame_id;
      page->MetaLock();
      if (page->pin_count_ > 0) {
        // someone may have just re-pinned the frame before we get here, so give it up and retry
        page->MetaUnLock();
        continue;
      }

      already_exists = page_table_.count(new_page_id) != 0;
      if (already_exists) {
        // someone has already done the things we want
        page->MetaUnLock();
        break;
      }

      old_page_id = page->page_id_;
      is_dirty = page->is_dirty_;
      ResetPageMeta(page, new_page_id);
      page->MetaUnLock();

      // N.B. we unlatch it after we release the table lock
      page->WLatch();

      // it's finally safe to remove the victim here
      page_table_.erase(old_page_id);
      page_table_.emplace(new_page_id, frame_id);
    }

    if (is_dirty) {
      // need to flush it
      disk_manager_->WritePage(old_page_id, page->data_);
    }
    if (needs_io) {
      disk_manager_->ReadPage(new_page_id, page->GetData());
    } else {
      page->ResetMemory();
    }
    page->WUnlatch();

    return frame_id;
  }

  assert(already_exists);
  /* We have to re-insert the frame back into the lru list just in case the page will not be accessed in the future
   * and the replacer won't view it as a victim. The operation here may insert a frame whose corresponding
   * pin_count is greater than 0, but that's totally fine cuz the page replacement process will recheck it and act
   * accordingly. What's more, this operation can also be redundant because the page could be pinned and unpinned
   * just before we get here. In that extreme scenario, we still need to do it as the point here is to make sure the
   * replacer controls all the potential victims.
   */
  replacer_->Unpin(frame_id);
  return page_table_[new_page_id];
}

}  // namespace bustub
