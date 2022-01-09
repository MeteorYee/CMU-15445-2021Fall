//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <chrono>  // NOLINT
#include <iostream>
#include <string>
#include <thread>  // NOLINT
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  table_latch_.WLock();

  Page *dir_page_raw = NewPageHelper(&directory_page_id_);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  (void)NewPageHelper(&bucket_page_id);
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));

  dir_page_raw->WLatch();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
  dir_page->SetBucketPageId(0, bucket_page_id);
  // Unlatching is wrapped into the method below
  ReleasePage(dir_page_raw, directory_page_id_, true, true);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToBucketIndexByHighBit(uint32_t high_bit, KeyType key) {
  return (Hash(key) & ((high_bit << 1) - 1));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return FetchPageHelper(directory_page_id_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return FetchPageHelper(bucket_page_id);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::NewPageHelper(page_id_t *page_id) {
  Page *page = nullptr;
  while (true) {
    page = buffer_pool_manager_->NewPage(page_id);
    if (page != nullptr) {
      break;
    }
    LOG_WARN("Failed to create a page for the hash table, buffer pool is full right now.");
    // sleep and retry
    std::this_thread::sleep_for(std::chrono::milliseconds(TEM_MILLIS));
  }
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchPageHelper(page_id_t page_id) {
  assert(page_id != INVALID_PAGE_ID);
  Page *page = nullptr;
  while (true) {
    page = buffer_pool_manager_->FetchPage(page_id);
    if (page != nullptr) {
      break;
    }
    LOG_WARN("Failed to fetch the page for the hash table, buffer pool is full right now.");
    // sleep and retry
    std::this_thread::sleep_for(std::chrono::milliseconds(TEM_MILLIS));
  }
  return page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::BucketSplit(HASH_TABLE_BUCKET_TYPE *bucket_page, HASH_TABLE_BUCKET_TYPE *split_bucket_page,
                                      uint32_t high_bit, uint32_t split_image_idx) {
  uint32_t split_idx = 0;
  for (uint32_t idx = 0; idx < BUCKET_ARRAY_SIZE; idx++) {
    KeyType key = bucket_page->KeyAt(idx);
    if (KeyToBucketIndexByHighBit(high_bit, key) != split_image_idx) {
      continue;
    }
    bucket_page->RemoveAt(idx);
    split_bucket_page->InsertAt(split_idx, key, bucket_page->ValueAt(idx), comparator_);
    split_idx++;
  }
  return split_idx;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::AcquireDirPage(bool is_write_latch) {
  Page *dir_page_raw = FetchDirectoryPage();
  if (is_write_latch) {
    dir_page_raw->WLatch();
  } else {
    dir_page_raw->RLatch();
  }
  return dir_page_raw;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::AcquireBucketPage(page_id_t bucket_page_id, bool is_write_latch) {
  Page *bucket_page_raw = FetchBucketPage(bucket_page_id);
  if (is_write_latch) {
    bucket_page_raw->WLatch();
  } else {
    bucket_page_raw->RLatch();
  }
  return bucket_page_raw;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::ReleasePage(Page *page, page_id_t page_id, bool is_dirty, bool is_write_latch) {
  if (is_dirty) {
    assert(is_write_latch);
    page->MarkPageDirty();
  }
  if (is_write_latch) {
    page->WUnlatch();
  } else {
    page->RUnlatch();
  }
  assert(buffer_pool_manager_->UnpinPage(page_id, is_dirty));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  Page *dir_page_raw = AcquireDirPage(false);
  page_id_t bucket_page_id = KeyToPageId(key, reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData()));

  // crabbing protocol
  Page *bucket_page_raw = AcquireBucketPage(bucket_page_id, false);
  ReleasePage(dir_page_raw, directory_page_id_, false, false);

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
  bucket_page->GetValue(key, comparator_, result);
  ReleasePage(bucket_page_raw, bucket_page_id, false, false);

  table_latch_.RUnlock();
  return !result->empty();
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  bool has_inserted = false;
  bool need_split = false;

  Page *dir_page_raw = AcquireDirPage(false);
  page_id_t bucket_page_id = KeyToPageId(key, reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData()));

  // crabbing protocol
  Page *bucket_page_raw = AcquireBucketPage(bucket_page_id, true);
  ReleasePage(dir_page_raw, directory_page_id_, false, false);

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
  if (bucket_page->IsFull()) {
    need_split = true;
  } else {
    has_inserted = bucket_page->Insert(key, value, comparator_);
  }
  ReleasePage(bucket_page_raw, bucket_page_id, has_inserted, true);

  table_latch_.RUnlock();
  return need_split ? SplitInsert(transaction, key, value) : has_inserted;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  Page *dir_page_raw = AcquireDirPage(true);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());

  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);

  Page *bucket_page_raw = AcquireBucketPage(bucket_page_id, true);
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
  bool has_inserted = false;
  bool is_split_success = true;

  /*
   * 1. increase the gloabl depth if needed
   * 2. create a new bucket page
   * 3. increase the local depths for both
   * 4. redirect the dir pointer to the new bucket
   * 5. traverse all the existing pairs and try to split the bucket
   * 6.1 if the bucket is still full and the insertion fails, try to split once again
   * 6.2 if the split succeeds, then do the insertion accordingly
   */
  if (bucket_page->IsFull()) {
    // step 1
    if (dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth()) {
      // need to expand the directory array
      if (dir_page->IsFull()) {
        // the directory array can't grow anymore
        LOG_WARN("The directory array is full and the hash table cannot grow anymore.");
        ReleasePage(bucket_page_raw, bucket_page_id, false, true);
        ReleasePage(dir_page_raw, directory_page_id_, false, true);
        return false;
      }
      dir_page->IncrGlobalDepth();
    }
    // step 2
    page_id_t split_bucket_page_id = INVALID_PAGE_ID;
    Page *split_bucket_page_raw = NewPageHelper(&split_bucket_page_id);
    // step 3
    uint32_t high_bit = dir_page->GetLocalHighBit(bucket_idx);
    uint32_t split_image_idx = bucket_idx ^ high_bit;
    assert(bucket_idx != split_image_idx);
    dir_page->IncrLocalDepth(bucket_idx);
    dir_page->IncrLocalDepth(split_image_idx);
    // step 4
    dir_page->SetBucketPageId(split_image_idx, split_bucket_page_id);

    split_bucket_page_raw->WLatch();
    // it's time to release the dir page latch
    ReleasePage(dir_page_raw, directory_page_id_, true, true);
    auto split_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_bucket_page_raw->GetData());

    // step 5
    uint32_t split_count = BucketSplit(bucket_page, split_bucket_page, high_bit, split_image_idx);
    // step 6
    bool is_to_split_image = KeyToBucketIndexByHighBit(high_bit, key) == split_image_idx;
    if (is_to_split_image) {
      has_inserted = split_bucket_page->Insert(key, value, comparator_);
    } else if (split_count > 0) {
      // the key to be inserted is hashed into the pre-existing bucket
      has_inserted = bucket_page->Insert(key, value, comparator_);
    } else {
      // If nothing has been put into the split image and the key to be inserted is not hashed
      // into it, we need yet another run of split.
      is_split_success = false;
    }
    ReleasePage(split_bucket_page_raw, split_bucket_page_id, is_split_success || has_inserted, true);
    ReleasePage(bucket_page_raw, bucket_page_id, is_split_success, true);
  } else {  // someone has already done the things we want
    // release the dir_page latch firstly
    ReleasePage(dir_page_raw, directory_page_id_, false, true);
    has_inserted = bucket_page->Insert(key, value, comparator_);
    ReleasePage(bucket_page_raw, bucket_page_id, has_inserted, true);
  }

  table_latch_.RUnlock();
  return is_split_success ? has_inserted : SplitInsert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  bool has_removed = false;
  bool need_merge = false;

  Page *dir_page_raw = AcquireDirPage(false);
  page_id_t bucket_page_id = KeyToPageId(key, reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData()));

  // crabbing protocol
  Page *bucket_page_raw = AcquireBucketPage(bucket_page_id, true);
  ReleasePage(dir_page_raw, directory_page_id_, false, false);

  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
  has_removed = bucket_page->Remove(key, value, comparator_);
  need_merge = bucket_page->IsEmpty();
  ReleasePage(bucket_page_raw, bucket_page_id, has_removed, true);
  table_latch_.RUnlock();

  if (need_merge) {
    Merge(transaction, key, value);
  }
  return has_removed;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  bool has_modified = false;
  bool need_merge_again = false;
  Page *dir_page_raw = AcquireDirPage(true);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);

  /*
   * 1. if the local depth of the bucket is zero, then give up merging, else goto step 2
   * 2. if the bucket is not empty, then give up merging, else goto step 3
   * 3. find the split image
   * 4. if the local depths differ, then give up merging, else goto step 5
   * 5. decrease the local depths for all the split images (not just 2)
   * 6. redirect the dir pointer to the split image's page
   * 7. decrease the gloabl depth if needed
   * 8. delete the redundant page
   * 9. If the split image page is empty as well, continue the merge
   */
  do {
    // step 1
    if (local_depth == 0) {
      break;
    }
    // step 2
    Page *bucket_page_raw = AcquireBucketPage(bucket_page_id, false);
    auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
    bool is_empty = bucket_page->IsEmpty();
    ReleasePage(bucket_page_raw, bucket_page_id, false, false);
    if (!is_empty) {
      break;
    }
    // step 3
    // the high_bit needs to right shift one bit because we are merging the bucket
    uint32_t high_bit = dir_page->GetLocalHighBit(bucket_idx) >> 1;
    uint32_t split_bucket_idx = bucket_idx ^ high_bit;
    assert(bucket_idx != split_bucket_idx);
    page_id_t split_bucket_page_id = dir_page->GetBucketPageId(split_bucket_idx);

    // step 4
    if (local_depth != dir_page->GetLocalDepth(split_bucket_idx)) {
      break;
    }
    uint32_t new_depth = local_depth - 1;
    uint32_t idx_num = 0x1 << (dir_page->GetGlobalDepth() - new_depth);
    uint32_t mask = (0x1 << new_depth) - 1;
    for (uint32_t i = 0; i < idx_num; i++) {
      uint32_t idx = (i << new_depth) | (bucket_idx & mask);
      // step 5
      dir_page->DecrLocalDepth(idx);
      // step 6
      dir_page->SetBucketPageId(idx, split_bucket_page_id);
    }
    // step 7
    if (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
    has_modified = true;

    Page *split_bucket_page_raw = AcquireBucketPage(split_bucket_page_id, false);
    auto split_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(split_bucket_page_raw->GetData());
    need_merge_again = split_bucket_page->IsEmpty();
    ReleasePage(split_bucket_page_raw, split_bucket_page_id, false, false);
  } while (false);

  ReleasePage(dir_page_raw, directory_page_id_, has_modified, true);
  table_latch_.RUnlock();
  // step 8: delete the bucket page, it shall succeed perfectly cuz no one is able to find it right now
  if (has_modified) {
    assert(buffer_pool_manager_->DeletePage(bucket_page_id));
  }
  // step 9
  if (need_merge_again) {
    Merge(transaction, key, value);
  }
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH --- I touched it. XD
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();

  Page *dir_page_raw = AcquireDirPage(false);
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
  uint32_t global_depth = dir_page->GetGlobalDepth();
  ReleasePage(dir_page_raw, directory_page_id_, false, false);

  table_latch_.RUnlock();
  return global_depth;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrityAndPrint(size_t expected_size, bool print_info) {
  size_t count = 0;
  table_latch_.RLock();

  Page *dir_page_raw = AcquireDirPage(false);

  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
  if (print_info) {
    LOG_DEBUG("Directory size = %d", dir_page->Size());
    dir_page->PrintDirectory();
  }

  // traverse all the buckets
  std::unordered_set<page_id_t> bucket_set{};
  for (uint32_t bucket_idx = 0; bucket_idx < dir_page->Size(); bucket_idx++) {
    page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
    if (bucket_set.count(bucket_page_id) > 0) {
      // already printed
      continue;
    }
    bucket_set.insert(bucket_page_id);

    Page *bucket_page_raw = AcquireBucketPage(bucket_page_id, false);
    auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page_raw->GetData());
    if (print_info) {
      LOG_DEBUG("\n=========== Start of Bucket Page ID = %d =========", bucket_page_id);
      bucket_page->PrintBucket();
      LOG_DEBUG("========= End of Bucket Page ID = %d =========\n", bucket_page_id);
    }
    count += bucket_page->NumReadable();
    ReleasePage(bucket_page_raw, bucket_page_id, false, false);
  }

  dir_page->VerifyIntegrity();
  ReleasePage(dir_page_raw, directory_page_id_, false, false);

  table_latch_.RUnlock();
  assert(expected_size == count);
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH --- I touched it. XD
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();

  Page *dir_page_raw = FetchDirectoryPage();
  dir_page_raw->RLatch();
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(dir_page_raw->GetData());
  dir_page->VerifyIntegrity();
  dir_page_raw->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));

  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
