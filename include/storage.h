#pragma once

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "schema.h"

// Fast O(1) column lookup — built from TableSchema::columns
struct ColLookup {
    std::unordered_map<std::string, int> map;
    void build(const std::vector<ColumnDef>& cols) {
        map.clear();
        map.reserve(cols.size());
        for (size_t i = 0; i < cols.size(); ++i) {
            map[cols[i].name] = static_cast<int>(i);
        }
    }
    int find(const std::string& name) const {
        auto it = map.find(name);
        return it == map.end() ? -1 : it->second;
    }
};

using Field = std::optional<std::string>;

struct Row {
    std::vector<Field> fields;
    int64_t expiry = 0;
};

using PKIndex = std::unordered_map<std::string, size_t>;

struct Table {
    TableSchema schema;
    ColLookup colLookup;  // O(1) column-name → index lookup
    std::string schemaPath;
    std::string dataPath;
    std::string walPath;
    int dataFd = -1;
    int walFd = -1;
    std::vector<uint64_t> rowOffsets;
    PKIndex pkIdx;
    uint64_t fileEndOffset = 0;
    uint64_t walSize = 0;
    uint64_t unsyncedRows = 0;
    int64_t minExpiryMs = 0;
    mutable std::shared_mutex mtx;

    Table() = default;
    ~Table();
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;
    Table(Table&&) = delete;
    Table& operator=(Table&&) = delete;
};

class StorageEngine {
    mutable std::shared_mutex engineMtx_;
    std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
    std::string dataDir_;

public:
    StorageEngine();
    ~StorageEngine();
    bool createTable(const TableSchema&, bool ifNotExists = false);
    Table* getTable(const std::string& name);
    size_t insertRow(const std::string& table, Row row, std::string& err);
    size_t insertRows(const std::string& table, const std::vector<Row>& rows, std::string& err);
    size_t deleteRows(const std::string& table,
                      std::function<bool(const Row&)> pred,
                      std::string& err);
    std::vector<std::string> tableNames() const;
    size_t rowCount(const Table& tbl) const;
    bool readRow(const Table& tbl, size_t index, Row& row) const;
    bool scanRows(const Table& tbl,
                  const std::function<bool(const Row&, size_t)>& visitor) const;

private:
    enum class ReadStatus { OK, EOF_REACHED, TRUNCATED, CORRUPT };

    void loadTables();
    bool loadTableFromSchema(const std::string& schemaPath);
    bool parseSchemaFile(const std::string& schemaPath, TableSchema& schema) const;
    bool rebuildTableState(Table& tbl);
    void rebuildPkIndex(Table& tbl);
    bool writeSchemaFile(const TableSchema& schema, const std::string& path) const;
    void serializeRow(const Row& row, std::vector<char>& out) const;
    ReadStatus parseRowBuffer(const char* data, size_t len, Row& row) const;
    ReadStatus readRowRecordAt(int fd, uint64_t fileSize, uint64_t offset, Row& row, uint64_t& nextOffset) const;
    bool syncTable(Table& tbl, bool force) const;
    bool writeAllFd(int fd, const char* data, size_t len) const;
    bool writeWal(Table& tbl, const char* data, size_t len) const;
    bool rollbackWal(Table& tbl, uint64_t walStart) const;
    bool replayWal(Table& tbl);
};
