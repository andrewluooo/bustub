//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManager::is_all_pinned() {
  return free_list_.empty() && replacer_->Size() == 0;
}

void BufferPoolManager::init_new_page(frame_id_t frame_id, page_id_t page_id) {
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
}

frame_id_t BufferPoolManager::find_replace() {
  frame_id_t replace_id = -1;
  if (!free_list_.empty()) {
    replace_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Size() > 0) {
    replacer_->Victim(&replace_id);
    page_table_.erase(pages_[replace_id].page_id_);
    if (pages_[replace_id].IsDirty()) {
      disk_manager_->WritePage(pages_[replace_id].page_id_, pages_[replace_id].GetData());
    }
  }
  return replace_id;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock<std::mutex> lock(latch_);
//  std::scoped_lock bpm_slk{latch_};
  if (page_table_.find(page_id) != page_table_.end()) {

    auto frame_id = page_table_[page_id];

    if (pages_[frame_id].GetPinCount() == 0) {
      pages_[frame_id].pin_count_ = 1;
    }

    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  if (!is_all_pinned()) {

    frame_id_t replace_id = find_replace();
    page_table_.emplace(page_id, replace_id);
    init_new_page(replace_id, page_id);
    disk_manager_->ReadPage(page_id, pages_[replace_id].GetData());
    return &pages_[replace_id];
  }

  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto frame_id = iter->second;
  if (pages_[frame_id].pin_count_ > 0) {
    pages_[frame_id].pin_count_--;
  }

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  // important!
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock<std::mutex> lock(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::scoped_lock<std::mutex> lock(latch_);

  page_id_t id = disk_manager_->AllocatePage();

  if (is_all_pinned()) {
    return nullptr;
  }

  frame_id_t replace_id = find_replace();
  init_new_page(replace_id, id);
  pages_[replace_id].ResetMemory();
  page_table_.emplace(id, replace_id);
  *page_id = id;

  return &pages_[replace_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock<std::mutex> lock(latch_);
  disk_manager_->DeallocatePage(page_id);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = iter->second;
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  replacer_->Pin(frame_id);
  page_table_.erase(iter);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);

  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock<std::mutex> lock(latch_);

  for (auto iter : page_table_) {
    page_id_t pageId = iter.first;
    bool flushStatus = BufferPoolManager::FlushPageImpl(pageId);

    if (flushStatus != true) {
      LOG_INFO("Flush page id: %d failed", pageId);
    }
  }
}

}  // namespace bustub
