//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_page_test.cpp
//
// Identification: test/container/hash_table_page_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a directory page from the BufferPoolManager
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  EXPECT_EQ(0, directory_page->GetGlobalDepth());
  directory_page->SetPageId(10);
  EXPECT_EQ(10, directory_page->GetPageId());
  directory_page->SetLSN(100);
  EXPECT_EQ(100, directory_page->GetLSN());

  // expand the directory as per the fake 8 buckets below
  for (int i = 0; i < 3; i++) {
    directory_page->IncrGlobalDepth();
  }
  EXPECT_EQ(3, directory_page->GetGlobalDepth());
  EXPECT_EQ(0x07, directory_page->GetGlobalDepthMask());
  // add a few hypothetical bucket pages
  for (unsigned i = 0; i < 8; i++) {
    directory_page->SetBucketPageId(i, i);
    directory_page->SetLocalDepth(i, 3);
    EXPECT_EQ(3, directory_page->GetLocalDepth(i));
  }
  EXPECT_FALSE(directory_page->CanShrink());
  // verify it
  directory_page->VerifyIntegrity();

  // check for correct bucket page IDs
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(i, directory_page->GetBucketPageId(i));
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageSampleTest2) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a directory page from the BufferPoolManager
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  EXPECT_EQ(0, directory_page->GetGlobalDepth());
  directory_page->SetPageId(10);
  EXPECT_EQ(10, directory_page->GetPageId());
  directory_page->SetLSN(100);
  EXPECT_EQ(100, directory_page->GetLSN());

  // expand the directory as per the fake 8 buckets below
  for (int i = 0; i < 2; i++) {
    directory_page->IncrGlobalDepth();
  }
  EXPECT_EQ(2, directory_page->GetGlobalDepth());
  // add a few hypothetical bucket pages
  for (unsigned i = 0; i < 4; i++) {
    directory_page->SetBucketPageId(i, (i & 0x01));
    directory_page->IncrLocalDepth(i);
    EXPECT_EQ(1, directory_page->GetLocalDepth(i));
    directory_page->DecrLocalDepth(i);
    EXPECT_EQ(0, directory_page->GetLocalDepth(i));
    directory_page->SetLocalDepth(i, 1);
  }
  // verify it
  directory_page->VerifyIntegrity();
  // print the page directory
  directory_page->PrintDirectory();
  EXPECT_TRUE(directory_page->CanShrink());

  // decrease the global depth and test it
  directory_page->DecrGlobalDepth();
  EXPECT_EQ(1, directory_page->GetGlobalDepth());

  // unpin the directory page now that we are done
  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  // based on the size of HashTableBucketPage<int, int, IntComparator>
  const unsigned max_elements = 496;
  EXPECT_TRUE(bucket_page->IsEmpty());
  // insert a few (key, value) pairs
  for (unsigned i = 0; i < max_elements; i++) {
    EXPECT_FALSE(bucket_page->IsFull());
    EXPECT_EQ(i, bucket_page->NumReadable());
    assert(bucket_page->Insert(i, i, IntComparator()));
    EXPECT_FALSE(bucket_page->IsEmpty());
    EXPECT_EQ(i + 1, bucket_page->NumReadable());
  }

  // check for the inserted pairs
  for (unsigned i = 0; i < max_elements; i++) {
    EXPECT_EQ(i, bucket_page->KeyAt(i));
    EXPECT_EQ(i, bucket_page->ValueAt(i));
  }
  EXPECT_TRUE(bucket_page->IsFull());
  EXPECT_EQ(max_elements, bucket_page->NumReadable());

  // remove a few pairs
  for (unsigned i = 0; i < max_elements; i++) {
    if (i % 2 == 1) {
      assert(bucket_page->Remove(i, i, IntComparator()));
    }
  }
  EXPECT_FALSE(bucket_page->IsFull());

  // check for the flags
  unsigned removed = 0;
  for (unsigned i = 0; i < max_elements; i++) {
    EXPECT_TRUE(bucket_page->IsOccupied(i));
    if (i % 2 == 1) {
      EXPECT_FALSE(bucket_page->IsReadable(i));
      removed++;
    } else {
      EXPECT_TRUE(bucket_page->IsReadable(i));
    }
  }
  EXPECT_EQ(max_elements - removed, bucket_page->NumReadable());

  // try to remove the already-removed pairs, they should fail
  for (unsigned i = 0; i < max_elements; i++) {
    if (i % 2 == 1) {
      EXPECT_FALSE(bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // remove all the left pairs
  for (unsigned i = 0; i < max_elements; i++) {
    if (i % 2 == 0) {
      EXPECT_TRUE(bucket_page->Remove(i, i, IntComparator()));
    }
  }
  EXPECT_TRUE(bucket_page->IsEmpty());

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
