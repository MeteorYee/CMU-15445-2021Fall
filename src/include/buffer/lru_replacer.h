//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me! done

  /**
   * The node inside the lru list
   */
  struct LRUNode {
    LRUNode *prev;
    LRUNode *next;
    frame_id_t frame_id;

    LRUNode();
    LRUNode(frame_id_t frm_id);
  };

  size_t num_pages_;
  LRUNode *dummy_;
  std::unordered_map<frame_id_t, LRUNode *> lru_map_;

  mutable std::shared_mutex mutex_;

  /**
   * @brief Delete the node in the lru list.
   *
   * @param node the node to be deleted.
   */
  void ListDelete(LRUNode *node);

  /**
   * @brief Insert the node into the lru list.
   *
   * @param node the node to be inserted
   */
  void ListInsert(LRUNode *node);
};

}  // namespace bustub
