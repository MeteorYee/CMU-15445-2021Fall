//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_set>

#include <cstdio>
#include <random>
#include <string>
#include "buffer/buffer_pool_manager.h"
#include "buffer/parallel_buffer_pool_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
class ParallelBufferPoolManagerTest : public ::testing::Test {
 protected:
  std::string db_name_;
  size_t buffer_pool_size_;
  size_t num_instances_;
  int thread_num_;

  DiskManager *disk_manager_;
  BufferPoolManager *bpm_;

  void Initialize(std::string db_name, size_t buffer_pool_size, int num_instances, int thread_num) {
    db_name_ = std::move(db_name);
    buffer_pool_size_ = buffer_pool_size;
    num_instances_ = num_instances;
    thread_num_ = thread_num;
    disk_manager_ = new DiskManager(db_name_);
    bpm_ = new ParallelBufferPoolManager(num_instances_, buffer_pool_size_, disk_manager_);
    std::srand(std::time(nullptr));
  }

  // we don't use the provided SetUp() method here, cuz we wanna pass some parameters
  void SetUp() override {
    // nothing
  }

  void TearDown() override {
    // Shutdown the disk manager and remove the temporary file we created.
    disk_manager_->ShutDown();
    remove(db_name_.c_str());

    delete bpm_;
    delete disk_manager_;
  }

  void PageFetchCheckRoutine(const std::vector<page_id_t> &id_vec, bool dirty_check, bool dirty_flag) {
    Page *page = nullptr;
    for (const page_id_t page_id : id_vec) {
      page = bpm_->FetchPage(page_id);
      ASSERT_NE(nullptr, page);

      page_id_t buf_page_id = INVALID_PAGE_ID;
      int ret = 0;
      page->RLatch();
      // get the page id from page header
      ret = sscanf(page->GetData(), "%d", &buf_page_id);
      page->RUnlatch();

      if (buf_page_id == INVALID_PAGE_ID) {
        std::stringstream ss;
        ss << std::this_thread::get_id();
        LOG_ERROR("Thread: %s, wierd sscanf, ret = %d", ss.str().c_str(), ret);
      }
      ASSERT_EQ(page_id, buf_page_id);

      bool flag = true;
      page->MetaLock();
      if (page->GetPinCount() <= 0) {
        LOG_ERROR("the pin count = %d", page->GetPinCount());
        flag = false;
      }
      if (page_id != page->GetPageId()) {
        LOG_ERROR("expected page_id: %d, actual page_id in buffer: %d", page_id, page->GetPageId());
        flag = false;
      }
      if (dirty_check && dirty_flag != page->IsDirty()) {
        LOG_ERROR("expected is_dirty: %d, actual is_dirty in buffer: %d", dirty_flag, page->IsDirty());
        flag = false;
      }
      page->MetaUnLock();
      ASSERT_TRUE(flag);

      // unpin the page
      ASSERT_TRUE(bpm_->UnpinPage(page_id, false));
    }
  }

  void MultiThreadNewPage(int page_count_each, std::unordered_set<page_id_t> &expected_set) {
    std::mutex set_mutex;

    auto new_page_func = [this, &expected_set, &set_mutex](int page_count) {
      page_id_t page_id_temp;
      Page *page = nullptr;
      std::unordered_set<page_id_t> id_set{};

      for (int i = 0; i < page_count; i++) {
        page = bpm_->NewPage(&page_id_temp);
        ASSERT_NE(nullptr, page);
        ASSERT_EQ(0, id_set.count(page_id_temp));
        id_set.insert(page_id_temp);

        {
          std::scoped_lock lock(set_mutex);
          if (expected_set.count(page_id_temp) > 0) {
            expected_set.erase(page_id_temp);
          }
        }

        page->WLatch();
        // write the page id at the head of the page
        snprintf(page->GetData(), PAGE_SIZE, "%d", page_id_temp);
        page->MarkPageDirty();
        page->WUnlatch();

        bool flag = true;
        page->MetaLock();
        if (page->GetPinCount() != 1) {
          LOG_ERROR("the pin count = %d", page->GetPinCount());
          flag = false;
        }
        if (page_id_temp != page->GetPageId()) {
          LOG_ERROR("expected page_id: %d, actual page_id in buffer: %d", page_id_temp, page->GetPageId());
          flag = false;
        }
        page->MetaUnLock();
        ASSERT_TRUE(flag);

        // unpin all the pages
        ASSERT_TRUE(bpm_->UnpinPage(page_id_temp, true));
      }

      ASSERT_EQ(page_count, id_set.size());
    };

    std::thread workers[thread_num_];
    for (int i = 0; i < thread_num_; i++) {
      workers[i] = std::thread(new_page_func, page_count_each);
    }

    for (int i = 0; i < thread_num_; i++) {
      workers[i].join();
    }

    ASSERT_TRUE(expected_set.empty());
  }

  void MultiThreadFetchPageAll(int page_count_each, bool dirty_check, bool dirty_flag) {
    auto fetch_page_func = [this, dirty_check, dirty_flag](page_id_t start, int page_count) {
      page_id_t page_id = start;
      std::vector<page_id_t> id_vec{};

      // generate some random page ids
      for (int i = 0; i < page_count; i++) {
        id_vec.push_back(page_id);
        page_id++;
      }
      this->PageFetchCheckRoutine(id_vec, dirty_check, dirty_flag);
    };

    std::thread workers[thread_num_];
    for (int i = 0; i < thread_num_; i++) {
      workers[i] = std::thread(fetch_page_func, page_count_each * i, page_count_each);
    }

    for (int i = 0; i < thread_num_; i++) {
      workers[i].join();
    }
  }
};

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST_F(ParallelBufferPoolManagerTest, BinaryDataTest) {
  Initialize("test.db", 10, 5, 1);

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  page_id_t page_id_temp;
  auto *page0 = bpm_->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size_ * num_instances_; ++i) {
    EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size_; i < buffer_pool_size_ * num_instances_ * 2; ++i) {
    EXPECT_EQ(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm_->UnpinPage(i, true));
    bpm_->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
    bpm_->UnpinPage(page_id_temp, false);
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm_->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));
  EXPECT_EQ(true, bpm_->UnpinPage(0, true));
}

// NOLINTNEXTLINE
TEST_F(ParallelBufferPoolManagerTest, SampleTest) {
  Initialize("test.db", 10, 5, 1);

  page_id_t page_id_temp;
  auto *page0 = bpm_->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size_ * num_instances_; ++i) {
    EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size_; i < buffer_pool_size_ * num_instances_ * 2; ++i) {
    EXPECT_EQ(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Write world out to page 4
  auto page4 = bpm_->FetchPage(4);
  snprintf(page4->GetData(), PAGE_SIZE, "World");
  EXPECT_EQ(0, strcmp(page4->GetData(), "World"));
  page4->MarkPageDirty();
  bpm_->UnpinPage(4, true);

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning pages {0, 1, 2, 3},
  // there would still be one buffer page left for reading page 4.

  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm_->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm_->FetchPage(i));
  }

  // Scenario: If we make a new page, page 4 should be flushed to the disk
  EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  EXPECT_EQ(true, bpm_->UnpinPage(page_id_temp, true));

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page4 = bpm_->FetchPage(4);
  EXPECT_EQ(0, strcmp(page4->GetData(), "World"));
  EXPECT_EQ(true, bpm_->UnpinPage(4, true));

  ASSERT_TRUE(bpm_->DeletePage(4));
}

TEST_F(ParallelBufferPoolManagerTest, MultiThreadFlushAllTest) {
  Initialize("multithread_test.db", 1024, 5, 8);
  const size_t total_count = buffer_pool_size_ * num_instances_;
  const int page_count_each = static_cast<int>(total_count) / thread_num_;

  ASSERT_EQ(total_count, bpm_->GetPoolSize());

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < total_count; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }

  MultiThreadNewPage(page_count_each, expected_set);

  MultiThreadFetchPageAll(page_count_each, true, true);

  bpm_->FlushAllPages();

  MultiThreadFetchPageAll(page_count_each, true, false);

  for (size_t i = total_count; i < total_count + total_count / 2; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }
  // create another 512 pages
  MultiThreadNewPage(page_count_each / 2, expected_set);

  // some pages may not need flush but the routine still processes
  bpm_->FlushAllPages();
}

}  // namespace bustub
