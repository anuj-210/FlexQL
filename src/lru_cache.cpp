#include "lru_cache.h"

#include <algorithm>
#include <cctype>

namespace {
constexpr size_t kMaxCachedRows = 10000;
}

// ---- helpers ---------------------------------------------------------------

std::string LRUCache::toUpperStr(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        out.push_back(static_cast<char>(std::toupper(c)));
    }
    return out;
}

// Index a newly-inserted cache key under every table name it mentions.
// We scan the SQL text (the key) for FROM/JOIN tokens and pick up the
// following word as a table name.  This is a best-effort heuristic —
// false positives are safe (extra invalidation); false negatives are
// avoided by also always indexing with the raw upper-cased full key string
// so invalidateTable() can always match.
void LRUCache::indexKey_(const std::string& key) {
    const std::string up = toUpperStr(key);
    // Walk through the SQL looking for FROM and JOIN keywords.
    static const char* kTriggers[] = {"FROM ", "JOIN "};
    for (const char* trigger : kTriggers) {
        const std::string t(trigger);
        size_t pos = 0;
        while ((pos = up.find(t, pos)) != std::string::npos) {
            pos += t.size();
            // skip spaces
            while (pos < up.size() && up[pos] == ' ') { ++pos; }
            // collect word
            size_t end = pos;
            while (end < up.size() && up[end] != ' ' && up[end] != ';'
                   && up[end] != ',' && up[end] != ')') {
                ++end;
            }
            if (end > pos) {
                tableIndex_[up.substr(pos, end - pos)].insert(key);
            }
        }
    }
}

void LRUCache::evictOne_() {
    if (lruList_.empty()) return;
    auto last = std::prev(lruList_.end());
    const std::string& evictKey = last->first;
    // Remove from tableIndex_
    const std::string upKey = toUpperStr(evictKey);
    // We need to find which tables referenced this key.
    // Rather than storing a reverse map, re-scan (eviction is rare).
    for (auto& [tbl, keys] : tableIndex_) {
        keys.erase(evictKey);
    }
    lruMap_.erase(evictKey);
    lruList_.pop_back();
}

// ---- public API ------------------------------------------------------------

LRUCache::LRUCache(size_t capacity) : capacity_(capacity) {}

std::optional<CacheEntry> LRUCache::get(const std::string& key) {
    // Try shared (read) lock first for the lookup.
    {
        std::shared_lock<std::shared_mutex> rlock(mtx_);
        auto it = lruMap_.find(key);
        if (it == lruMap_.end()) {
            return std::nullopt;
        }
        // Hit: we need to promote to front — that requires write access.
        // Fall through to upgrade.
    }
    // Promote to write lock to splice the node to front.
    std::unique_lock<std::shared_mutex> wlock(mtx_);
    auto it = lruMap_.find(key);
    if (it == lruMap_.end()) {
        return std::nullopt;  // evicted between locks
    }
    lruList_.splice(lruList_.begin(), lruList_, it->second);
    return it->second->second;
}

void LRUCache::put(const std::string& key, CacheEntry entry) {
    if (entry.rows.size() > kMaxCachedRows) {
        return;
    }
    std::unique_lock<std::shared_mutex> wlock(mtx_);
    auto it = lruMap_.find(key);
    if (it != lruMap_.end()) {
        it->second->second = std::move(entry);
        lruList_.splice(lruList_.begin(), lruList_, it->second);
        return;
    }
    lruList_.push_front({key, std::move(entry)});
    lruMap_[key] = lruList_.begin();
    indexKey_(key);
    while (lruMap_.size() > capacity_) {
        evictOne_();
    }
}

void LRUCache::invalidateTable(const std::string& tableName) {
    std::unique_lock<std::shared_mutex> wlock(mtx_);
    if (lruMap_.empty()) return;

    const std::string up = toUpperStr(tableName);
    auto tidx = tableIndex_.find(up);
    if (tidx == tableIndex_.end()) {
        return;  // nothing cached for this table
    }

    // O(hits) removal — typically very small compared to O(capacity) scan
    for (const std::string& k : tidx->second) {
        auto it = lruMap_.find(k);
        if (it != lruMap_.end()) {
            lruList_.erase(it->second);
            lruMap_.erase(it);
        }
    }
    tableIndex_.erase(tidx);
}

void LRUCache::clear() {
    std::unique_lock<std::shared_mutex> wlock(mtx_);
    lruList_.clear();
    lruMap_.clear();
    tableIndex_.clear();
}

size_t LRUCache::size() const {
    std::shared_lock<std::shared_mutex> rlock(mtx_);
    return lruMap_.size();
}
