#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

struct CacheEntry {
    std::vector<std::string> colNames;
    std::vector<std::vector<std::string>> rows;
};

class LRUCache {
    using ListNode = std::pair<std::string, CacheEntry>;
    using ListIt = std::list<ListNode>::iterator;

    size_t capacity_;
    mutable std::shared_mutex mtx_;
    std::list<ListNode> lruList_;
    std::unordered_map<std::string, ListIt> lruMap_;

    // Per-table index: upper-cased table name → set of cache keys that mention it.
    // Enables O(1) invalidation instead of O(n) scan of lruList_.
    std::unordered_map<std::string, std::unordered_set<std::string>> tableIndex_;

    static std::string toUpperStr(const std::string& s);
    // Extract upper-cased table names referenced by a cache key (the raw SQL).
    // Called under write lock, no allocation needed in common path.
    void indexKey_(const std::string& key);
    void evictOne_();  // evict LRU entry; caller holds write lock

public:
    explicit LRUCache(size_t capacity = 4096);
    std::optional<CacheEntry> get(const std::string& key);
    void put(const std::string& key, CacheEntry entry);
    void invalidateTable(const std::string& tableName);
    void clear();
    size_t size() const;
};
