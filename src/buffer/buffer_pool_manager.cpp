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

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  latch_.lock();
  frame_id_t frameId = -1;
  if (page_table_.find(page_id) != page_table_.end()) {
    frameId = page_table_[page_id];
    replacer_->Pin(frameId);
    pages_[frameId].pin_count_ += 1;
    latch_.unlock();
    return &pages_[frameId];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    frameId = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frameId)) {
      latch_.unlock();
      return nullptr;
    }
    // 2.     If R is dirty, write it back to the disk.
    Page *replacedPage = &pages_[frameId];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(replacedPage->page_id_);
  }
  pages_[frameId].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[frameId].GetData());
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[frameId].page_id_ = page_id;
  page_table_.insert({page_id, frameId});
  pages_[frameId].pin_count_ = 1;
  replacer_->Pin(frameId);

  latch_.unlock();
  return &pages_[frameId];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  Page *reqdPg = &pages_[page_table_[page_id]];
  if (reqdPg->pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  reqdPg->pin_count_ -= 1;
  reqdPg->is_dirty_ = is_dirty;
  if (reqdPg->pin_count_ == 0) {
    replacer_->Unpin(page_table_[page_id]);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  Page *FlushPg = &pages_[page_table_[page_id]];

  if (FlushPg->is_dirty_) {
    disk_manager_->WritePage(FlushPg->page_id_, FlushPg->GetData());
    FlushPg->is_dirty_ = false;
  }

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  frame_id_t frameId = -1;
  if (!free_list_.empty()) {
    frameId = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&frameId)) {
      latch_.unlock();
      return nullptr;
    }

    Page *replacedPage = &pages_[frameId];
    if (replacedPage->is_dirty_) {
      FlushPageImpl(replacedPage->page_id_);
      replacedPage->is_dirty_ = false;
    }

    page_table_.erase(replacedPage->page_id_);
  }

  *page_id = disk_manager_->AllocatePage();
  pages_[frameId].page_id_ = *page_id;
  page_table_.insert({*page_id, frameId});
  pages_[frameId].ResetMemory();
  replacer_->Pin(frameId);
  pages_[frameId].pin_count_ = 1;
  pages_[frameId].is_dirty_ = false;

  latch_.unlock();
  return &pages_[frameId];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }
  frame_id_t framdeId = page_table_[page_id];
  Page *deletePg = &pages_[framdeId];

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (deletePg->GetPinCount() > 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  free_list_.emplace_back(page_table_[page_id]);
  page_table_.erase(deletePg->page_id_);
  deletePg->ResetMemory();
  deletePg->page_id_ = INVALID_PAGE_ID;
  deletePg->is_dirty_ = false;
  deletePg->pin_count_ = 0;
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPageImpl(pages_[i].page_id_);
  }
}

}  // namespace bustub
