//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
class BufferPoolManagerInstanceTest : public ::testing::Test {
 protected:
  std::string db_name_;
  size_t buffer_pool_size_;
  int thread_num_;

  DiskManager *disk_manager_;
  BufferPoolManagerInstance *bpm_;

  void Initialize(std::string db_name, size_t buffer_pool_size, int thread_num) {
    db_name_ = std::move(db_name);
    buffer_pool_size_ = buffer_pool_size;
    thread_num_ = thread_num;
    disk_manager_ = new DiskManager(db_name_);
    bpm_ = new BufferPoolManagerInstance(buffer_pool_size, disk_manager_);
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

  void PageFetchModifyRoutine(const std::vector<page_id_t> &id_vec, bool dirty_check, bool dirty_flag) {
    Page *page = nullptr;
    for (const page_id_t page_id : id_vec) {
      page = bpm_->FetchPage(page_id);
      ASSERT_NE(nullptr, page);

      page_id_t buf_page_id = INVALID_PAGE_ID;
      page->WLatch();
      // get the page id from page header
      sscanf(page->GetData(), "%d", &buf_page_id);
      // write some random data
      snprintf(page->GetData(), PAGE_SIZE, "%d %d", buf_page_id, std::rand());
      page->MarkPageDirty();
      page->WUnlatch();

      bpm_->UnpinPage(page_id, true);
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

    ASSERT_EQ(static_cast<size_t>(0), expected_set.size());
  }

  void MultiThreadFetchPageRandom(int page_count_each, bool dirty_check, bool dirty_flag) {
    auto fetch_page_func = [this, dirty_check, dirty_flag](int page_count) {
      const int total_page_num = page_count * thread_num_;
      std::vector<page_id_t> id_vec{};

      // generate some random page ids
      for (int i = 0; i < page_count; i++) {
        id_vec.push_back(static_cast<page_id_t>(std::rand() % total_page_num));
      }
      this->PageFetchCheckRoutine(id_vec, dirty_check, dirty_flag);
    };

    std::thread workers[thread_num_];
    for (int i = 0; i < thread_num_; i++) {
      workers[i] = std::thread(fetch_page_func, page_count_each);
    }

    for (int i = 0; i < thread_num_; i++) {
      workers[i].join();
    }
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

  void MultiThreadFlushPage(int page_count_each, bool flush_check) {
    auto flush_page_func = [this, flush_check](page_id_t start, int page_count) {
      page_id_t page_id = start;
      for (int i = 0; i < page_count; i++) {
        if (flush_check) {
          ASSERT_TRUE(bpm_->FlushPage(page_id));
        } else {
          bpm_->FlushPage(page_id);
        }
        page_id++;
      }
    };

    std::thread workers[thread_num_];
    for (int i = 0; i < thread_num_; i++) {
      workers[i] = std::thread(flush_page_func, page_count_each * i, page_count_each);
    }

    for (int i = 0; i < thread_num_; i++) {
      workers[i].join();
    }
  }

  void MultiThreadDeleteWithFetchPageRandom() {
    auto delete_page_func = [this]() {
      const int total_page_num = static_cast<int>(buffer_pool_size_) * thread_num_;
      page_id_t page_id = INVALID_PAGE_ID;

      // generate some random page ids
      for (size_t i = 0; i < buffer_pool_size_; i++) {
        page_id = static_cast<page_id_t>(std::rand() % total_page_num);
        (void)bpm_->DeletePage(page_id);
      }
    };
    auto fetch_page_func = [this]() {
      std::vector<page_id_t> id_vec{};

      // generate some random page ids
      for (size_t i = 0; i < buffer_pool_size_; i++) {
        id_vec.push_back(static_cast<page_id_t>(std::rand() % buffer_pool_size_));
      }
      this->PageFetchCheckRoutine(id_vec, false, false);
    };
    std::thread workers[thread_num_];
    for (int i = 0; i < thread_num_; i++) {
      if (i % 2 == 0) {
        workers[i] = std::thread(delete_page_func);
      } else {
        workers[i] = std::thread(fetch_page_func);
      }
    }

    for (int i = 0; i < thread_num_; i++) {
      workers[i].join();
    }
  }

  void MultiThreadDeleteAllPages() {
    auto delete_page_func = [this](page_id_t start) {
      page_id_t page_id = start;

      // generate some random page ids
      for (size_t i = 0; i < buffer_pool_size_; i++) {
        (void)bpm_->DeletePage(page_id);
        page_id++;
      }
    };
    std::thread workers[thread_num_];
    for (int i = 0; i < thread_num_; i++) {
      workers[i] = std::thread(delete_page_func, i * buffer_pool_size_);
    }

    for (int i = 0; i < thread_num_; i++) {
      workers[i].join();
    }
  }
};

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST_F(BufferPoolManagerInstanceTest, BinaryDataTest) {
  Initialize("test.db", 10, 1);

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  page_id_t page_id_temp;
  const page_id_t impossible_page_id = 0x7fffffff;
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
  for (size_t i = 1; i < buffer_pool_size_; ++i) {
    EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size_; i < buffer_pool_size_ * 2; ++i) {
    EXPECT_EQ(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // if we unpin some non-existing pages, it's not going to succeed
  EXPECT_EQ(false, bpm_->UnpinPage(INVALID_PAGE_ID, true));
  EXPECT_EQ(false, bpm_->UnpinPage(impossible_page_id, true));

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} we should be able to create 5 new pages
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm_->UnpinPage(i, true));
    // if we unpin it once again, it's not going to succeed
    EXPECT_EQ(false, bpm_->UnpinPage(i, true));
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
TEST_F(BufferPoolManagerInstanceTest, SampleTest) {
  Initialize("test.db", 10, 1);

  page_id_t page_id_temp;
  auto *page0 = bpm_->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size_; ++i) {
    EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size_; i < buffer_pool_size_ * 2; ++i) {
    EXPECT_EQ(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm_->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm_->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  std::vector<page_id_t> page_id_on_disk{1, 2, 3, 4};
  // Scenario: Once all the pages in the buffer pool are pinned, we should not be able to fetch pages from disk.
  for (page_id_t page_id : page_id_on_disk) {
    EXPECT_EQ(nullptr, bpm_->FetchPage(page_id));
  }

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm_->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm_->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm_->FetchPage(0));
}

TEST_F(BufferPoolManagerInstanceTest, MultiThreadFetchTest) {
  Initialize("multithread_test.db", 1024, 4);

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < buffer_pool_size_ * thread_num_; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }

  MultiThreadNewPage(buffer_pool_size_, expected_set);

  MultiThreadFetchPageRandom(buffer_pool_size_, false, true);
}

TEST_F(BufferPoolManagerInstanceTest, MultiThreadFlushPageTest) {
  Initialize("multithread_test.db", 1024, 4);
  const size_t page_count_each = buffer_pool_size_ / thread_num_;

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < buffer_pool_size_; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }

  MultiThreadNewPage(page_count_each, expected_set);

  MultiThreadFetchPageAll(page_count_each, true, true);

  MultiThreadFlushPage(page_count_each, true);

  MultiThreadFetchPageAll(page_count_each, true, false);

  for (size_t i = buffer_pool_size_; i < buffer_pool_size_ + 12; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }
  // create another 12 pages
  MultiThreadNewPage(3, expected_set);

  // some pages may not need flush and some may not exist in the buffer, but the routine still processes
  MultiThreadFlushPage(page_count_each, false);
}

TEST_F(BufferPoolManagerInstanceTest, MultiThreadFlushAllTest) {
  Initialize("multithread_test.db", 1024, 4);
  const int page_count_each = static_cast<int>(buffer_pool_size_) / thread_num_;

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < buffer_pool_size_; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }

  MultiThreadNewPage(page_count_each, expected_set);

  MultiThreadFetchPageAll(page_count_each, true, true);

  bpm_->FlushAllPages();

  MultiThreadFetchPageAll(page_count_each, true, false);

  for (size_t i = buffer_pool_size_; i < buffer_pool_size_ + buffer_pool_size_ / 2; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }
  // create another 512 pages
  MultiThreadNewPage(page_count_each / 2, expected_set);

  // some pages may not need flush but the routine still processes
  bpm_->FlushAllPages();
}

TEST_F(BufferPoolManagerInstanceTest, DeletePageBasicFunctionTest) {
  Initialize("test.db", 16, 1);

  const size_t page_count_each = buffer_pool_size_;

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < buffer_pool_size_; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }
  MultiThreadNewPage(page_count_each, expected_set);
  page_id_t page_id = 0;
  bool do_fetch = false;

  // generate some random page ids
  for (size_t i = 0; i < buffer_pool_size_; i++) {
    page_id = static_cast<page_id_t>(std::rand() % buffer_pool_size_);
    if (do_fetch) {
      do_fetch = false;
      ASSERT_NE(nullptr, bpm_->FetchPage(page_id));
      ASSERT_FALSE(bpm_->DeletePage(page_id));
      ASSERT_TRUE(bpm_->UnpinPage(page_id, false));
    } else {
      do_fetch = true;
      ASSERT_TRUE(bpm_->DeletePage(page_id));
    }
  }
}

TEST_F(BufferPoolManagerInstanceTest, MultiThreadDeleteWithFetchPageTest) {
  Initialize("multithread_test.db", 1024, 4);
  const size_t page_count_each = buffer_pool_size_;

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < buffer_pool_size_; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }

  MultiThreadNewPage(page_count_each, expected_set);
  MultiThreadDeleteWithFetchPageRandom();
}

TEST_F(BufferPoolManagerInstanceTest, MultiThreadDeleteAllPagesTest) {
  Initialize("multithread_test.db", 1024, 4);
  const size_t page_count_each = buffer_pool_size_;

  std::unordered_set<page_id_t> expected_set{};
  for (size_t i = 0; i < buffer_pool_size_; i++) {
    expected_set.insert(static_cast<page_id_t>(i));
  }

  MultiThreadNewPage(page_count_each, expected_set);
  MultiThreadFetchPageRandom(page_count_each, false, true);
  MultiThreadDeleteAllPages();
  // another fetching run will still work smoothly
  MultiThreadFetchPageRandom(buffer_pool_size_, false, true);
}

}  // namespace bustub
