//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUNode::LRUNode() { frame_id_ = -1; }

LRUReplacer::LRUNode::LRUNode(frame_id_t frm_id) : frame_id_{frm_id} {}

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_{num_pages} {
  dummy_ = new LRUNode();
  dummy_->next_ = dummy_;
  dummy_->prev_ = dummy_;

  frame_count_ = 0;
  lru_vec_.insert(lru_vec_.end(), num_pages, nullptr);
}

LRUReplacer::~LRUReplacer() {
  std::unique_lock lock(mutex_);
  // clear the lru vector
  lru_vec_.clear();

  // release the lru list
  while (dummy_->next_ != dummy_) {
    ListDelete(dummy_->next_);
  }

  // release the dummy head
  dummy_->next_ = nullptr;
  dummy_->prev_ = nullptr;
  delete dummy_;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::unique_lock lock(mutex_);

  if (frame_count_ == 0) {
    assert(dummy_->next_ == dummy_);
    assert(dummy_->prev_ == dummy_);
    LOG_DEBUG("Trying to get a victim out of an empty lru.");
    return false;
  }
  frame_id_t fid = dummy_->prev_->frame_id_;
  assert(lru_vec_[fid] != nullptr);

  // The following steps are basically the same with Pin() method, but we don't
  // want to reuse the method cuz Victim() is not equivalent to Pin(). Each
  // method would be better used in a single-responsibility way.
  LRUNode *node = lru_vec_[fid];
  lru_vec_[fid] = nullptr;
  ListDelete(node);
  frame_count_--;

  *frame_id = fid;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (!IsIdValid(static_cast<size_t>(frame_id))) {
    LOG_ERROR("Invalid frame id = %d!", frame_id);
    return;
  }

  std::unique_lock lock(mutex_);

  if (lru_vec_[frame_id] == nullptr) {
    LOG_DEBUG("Trying to pin a non-existing frame, id = %d, may have been already victimed", frame_id);
    // comment out the logging above to prevent spamming
    return;
  }
  LRUNode *node = lru_vec_[frame_id];
  // remove it from the vector first
  lru_vec_[frame_id] = nullptr;

  // then remove it from the list
  ListDelete(node);
  frame_count_--;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (!IsIdValid(static_cast<size_t>(frame_id))) {
    LOG_ERROR("Invalid frame id = %d!", frame_id);
    return;
  }

  std::unique_lock lock(mutex_);

  if (lru_vec_[frame_id] != nullptr) {
    LOG_DEBUG("Trying to unpin a frame (id = %d) multiple times", frame_id);
    // comment out the logging above to prevent spamming
    return;
  }
  // insert into the list first
  LRUNode *node = new LRUNode(frame_id);
  ListInsert(node);

  // then insert into the vector
  lru_vec_[frame_id] = node;
  frame_count_++;
}

size_t LRUReplacer::Size() {
  std::shared_lock lock(mutex_);
  return frame_count_;
}

void LRUReplacer::ListDelete(LRUNode *node) {
  node->next_->prev_ = node->prev_;
  node->prev_->next_ = node->next_;
  node->next_ = nullptr;
  node->prev_ = nullptr;
  delete node;
}

void LRUReplacer::ListInsert(LRUNode *node) {
  dummy_->next_->prev_ = node;
  node->next_ = dummy_->next_;
  dummy_->next_ = node;
  node->prev_ = dummy_;
}

bool LRUReplacer::IsIdValid(frame_id_t fid) { return fid >= 0 && static_cast<size_t>(fid) < num_pages_; }

}  // namespace bustub
