#include "storage.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <mutex>
#include <unordered_set>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

constexpr int32_t kNullFieldLen = -1;
constexpr uint64_t kSyncRowThreshold = 1048576;

std::string typeToString(ColType type) {
    switch (type) {
        case ColType::INT:
            return "INT";
        case ColType::DECIMAL:
            return "DECIMAL";
        case ColType::VARCHAR:
            return "VARCHAR";
        case ColType::DATETIME:
            return "DATETIME";
        case ColType::TEXT:
            return "TEXT";
    }
    return "TEXT";
}

ColType typeFromString(const std::string& type) {
    if (type == "INT") {
        return ColType::INT;
    }
    if (type == "DECIMAL") {
        return ColType::DECIMAL;
    }
    if (type == "DATETIME") {
        return ColType::DATETIME;
    }
    if (type == "TEXT") {
        return ColType::TEXT;
    }
    return ColType::VARCHAR;
}

template <typename T>
void appendPod(std::vector<char>& out, const T& value) {
    const char* ptr = reinterpret_cast<const char*>(&value);
    out.insert(out.end(), ptr, ptr + sizeof(T));
}

template <typename T>
bool readPodAt(int fd, uint64_t fileSize, uint64_t offset, T& out) {
    if (offset + sizeof(T) > fileSize) {
        return false;
    }
    ssize_t n = ::pread(fd, &out, sizeof(T), static_cast<off_t>(offset));
    return n == static_cast<ssize_t>(sizeof(T));
}

size_t nextCapacity(size_t current, size_t needed) {
    size_t cap = current == 0 ? 1024 : current;
    while (cap < needed) {
        cap *= 2;
    }
    return cap;
}

void updateMinExpiry(int64_t rowExpiry, int64_t& minExpiry) {
    if (rowExpiry == 0) {
        return;
    }
    if (minExpiry == 0 || rowExpiry < minExpiry) {
        minExpiry = rowExpiry;
    }
}

enum class ScanStatus { OK, EOF_REACHED, FAIL };

class SequentialScanner {
public:
    explicit SequentialScanner(const Table& tbl) : tbl_(tbl) {}

    ~SequentialScanner() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    bool open() {
        fd_ = ::open(tbl_.dataPath.c_str(), O_RDONLY);
        if (fd_ < 0) {
            return false;
        }
#ifdef POSIX_FADV_SEQUENTIAL
        (void)::posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
        readBuf_.resize(1u << 20);
        readPos_ = 0;
        readEnd_ = 0;
        remaining_ = tbl_.fileEndOffset;
        return true;
    }

    ScanStatus next(Row& row, size_t expectedCols) {
        uint32_t fieldCount = 0;
        ScanStatus status = readExact(reinterpret_cast<char*>(&fieldCount), sizeof(fieldCount));
        if (status != ScanStatus::OK) {
            return status;
        }
        if (fieldCount != expectedCols) {
            return ScanStatus::FAIL;
        }

        int64_t expiry = 0;
        status = readExact(reinterpret_cast<char*>(&expiry), sizeof(expiry));
        if (status != ScanStatus::OK) {
            return ScanStatus::FAIL;
        }

        if (row.fields.size() != expectedCols) {
            row.fields.resize(expectedCols);
        }
        row.expiry = expiry;

        for (size_t i = 0; i < expectedCols; ++i) {
            int32_t fieldLen = 0;
            status = readExact(reinterpret_cast<char*>(&fieldLen), sizeof(fieldLen));
            if (status != ScanStatus::OK) {
                return ScanStatus::FAIL;
            }
            if (fieldLen == kNullFieldLen) {
                row.fields[i].reset();
                continue;
            }
            if (fieldLen < 0) {
                return ScanStatus::FAIL;
            }

            if (!row.fields[i].has_value()) {
                row.fields[i].emplace();
            }
            std::string& value = *row.fields[i];
            value.resize(static_cast<size_t>(fieldLen));
            if (fieldLen > 0) {
                status = readExact(value.data(), static_cast<size_t>(fieldLen));
                if (status != ScanStatus::OK) {
                    return ScanStatus::FAIL;
                }
            }
        }

        return ScanStatus::OK;
    }

private:
    ScanStatus refill() {
        if (remaining_ == 0) {
            return ScanStatus::EOF_REACHED;
        }
        while (true) {
            const size_t toRead = static_cast<size_t>(
                std::min<uint64_t>(remaining_, static_cast<uint64_t>(readBuf_.size())));
            ssize_t n = ::read(fd_, readBuf_.data(), toRead);
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n < 0) {
                return ScanStatus::FAIL;
            }
            if (n == 0) {
                return ScanStatus::EOF_REACHED;
            }
            readPos_ = 0;
            readEnd_ = static_cast<size_t>(n);
            remaining_ -= static_cast<uint64_t>(n);
            return ScanStatus::OK;
        }
    }

    ScanStatus readExact(char* dst, size_t len) {
        size_t copied = 0;
        while (copied < len) {
            if (readPos_ >= readEnd_) {
                ScanStatus status = refill();
                if (status == ScanStatus::EOF_REACHED) {
                    return copied == 0 ? ScanStatus::EOF_REACHED : ScanStatus::FAIL;
                }
                if (status != ScanStatus::OK) {
                    return status;
                }
            }
            const size_t avail = readEnd_ - readPos_;
            const size_t copyLen = std::min(len - copied, avail);
            std::memcpy(dst + copied, readBuf_.data() + readPos_, copyLen);
            readPos_ += copyLen;
            copied += copyLen;
        }
        return ScanStatus::OK;
    }

    const Table& tbl_;
    int fd_ = -1;
    std::vector<char> readBuf_;
    size_t readPos_ = 0;
    size_t readEnd_ = 0;
    uint64_t remaining_ = 0;
};

}  // namespace

Table::~Table() {
    if (dataFd >= 0) {
        ::close(dataFd);
        dataFd = -1;
    }
    if (walFd >= 0) {
        ::close(walFd);
        walFd = -1;
    }
}

int TableSchema::colIndex(const std::string& name) const {
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].name == name) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

StorageEngine::StorageEngine() : dataDir_("flexql_data") {
    std::error_code ec;
    fs::create_directories(dataDir_, ec);
    loadTables();
}

StorageEngine::~StorageEngine() {
    std::unique_lock<std::shared_mutex> lock(engineMtx_);
    for (auto& kv : tables_) {
        std::unique_lock<std::shared_mutex> tableLock(kv.second->mtx);
        syncTable(*kv.second, true);
    }
}

bool StorageEngine::createTable(const TableSchema& schema, bool ifNotExists) {
    std::unique_lock<std::shared_mutex> lock(engineMtx_);
    auto it = tables_.find(schema.tableName);
    if (it != tables_.end()) {
        if (ifNotExists) {
            return true;
        }

        std::unique_lock<std::shared_mutex> tableLock(it->second->mtx);
        it->second->schema = schema;
        it->second->colLookup.build(schema.columns);  // rebuild lookup on schema change
        it->second->rowOffsets.clear();
        it->second->pkIdx.clear();
        it->second->fileEndOffset = 0;
        it->second->walSize = 0;
        it->second->unsyncedRows = 0;
        it->second->minExpiryMs = 0;
        if (!writeSchemaFile(schema, it->second->schemaPath)) {
            return false;
        }
        if (::ftruncate(it->second->dataFd, 0) != 0) {
            return false;
        }
        if (::ftruncate(it->second->walFd, 0) != 0) {
            return false;
        }
        if (::lseek(it->second->dataFd, 0, SEEK_SET) < 0) {
            return false;
        }
        if (::lseek(it->second->walFd, 0, SEEK_SET) < 0) {
            return false;
        }
        return syncTable(*it->second, true);
    }

    auto tbl = std::make_unique<Table>();
    tbl->schema = schema;
    tbl->colLookup.build(schema.columns);  // build O(1) lookup once
    tbl->schemaPath = dataDir_ + "/" + schema.tableName + ".schema";
    tbl->dataPath = dataDir_ + "/" + schema.tableName + ".rows";
    tbl->walPath = dataDir_ + "/" + schema.tableName + ".wal";
    tbl->dataFd = ::open(tbl->dataPath.c_str(), O_RDWR | O_CREAT, 0644);
    if (tbl->dataFd < 0) {
        return false;
    }
    tbl->walFd = ::open(tbl->walPath.c_str(), O_RDWR | O_CREAT, 0644);
    if (tbl->walFd < 0) {
        return false;
    }
    if (!writeSchemaFile(schema, tbl->schemaPath)) {
        return false;
    }
    tables_.emplace(schema.tableName, std::move(tbl));
    return true;
}

Table* StorageEngine::getTable(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(engineMtx_);
    auto it = tables_.find(name);
    if (it == tables_.end()) {
        return nullptr;
    }
    return it->second.get();
}

size_t StorageEngine::insertRow(const std::string& table, Row row, std::string& err) {
    Table* tbl = getTable(table);
    if (!tbl) {
        err = "table not found: " + table;
        return 0;
    }

    std::unique_lock<std::shared_mutex> lock(tbl->mtx);
    if (row.fields.size() != tbl->schema.columns.size()) {
        err = "column count mismatch";
        return 0;
    }

    std::string pkValue;
    if (tbl->schema.pkIndex >= 0) {
        const Field& pk = row.fields[static_cast<size_t>(tbl->schema.pkIndex)];
        if (!pk.has_value()) {
            err = "primary key cannot be null";
            return 0;
        }
        pkValue = *pk;
        if (tbl->pkIdx.find(pkValue) != tbl->pkIdx.end()) {
            err.clear();
            return 0;
        }
    }

    std::vector<char> buffer;
    buffer.clear();
    buffer.reserve(96);
    serializeRow(row, buffer);
    const uint64_t walStart = tbl->walSize;
    if (!writeWal(*tbl, buffer.data(), buffer.size())) {
        err = "failed to write wal";
        return 0;
    }
    if (!writeAllFd(tbl->dataFd, buffer.data(), buffer.size())) {
        rollbackWal(*tbl, walStart);
        err = "failed to write row";
        return 0;
    }

    if (tbl->rowOffsets.size() == tbl->rowOffsets.capacity()) {
        tbl->rowOffsets.reserve(nextCapacity(tbl->rowOffsets.capacity(), tbl->rowOffsets.size() + 1));
    }
    tbl->rowOffsets.push_back(tbl->fileEndOffset);
    tbl->fileEndOffset += static_cast<uint64_t>(buffer.size());
    if (tbl->schema.pkIndex >= 0) {
        tbl->pkIdx[pkValue] = tbl->rowOffsets.size() - 1;
    }
    updateMinExpiry(row.expiry, tbl->minExpiryMs);
    ++tbl->unsyncedRows;
    if (!syncTable(*tbl, tbl->unsyncedRows >= kSyncRowThreshold)) {
        err = "failed to sync table file";
        return 0;
    }

    err.clear();
    return 1;
}

size_t StorageEngine::insertRows(const std::string& table, const std::vector<Row>& rows, std::string& err) {
    Table* tbl = getTable(table);
    if (!tbl) {
        err = "table not found: " + table;
        return 0;
    }
    if (rows.empty()) {
        err.clear();
        return 0;
    }

    std::unique_lock<std::shared_mutex> lock(tbl->mtx);
    std::vector<char> buffer;
    buffer.clear();
    buffer.reserve(rows.size() * 128);
    std::vector<uint64_t> newOffsets;
    newOffsets.reserve(rows.size());
    std::vector<std::pair<std::string, size_t>> newPkEntries;
    newPkEntries.reserve(rows.size());
    std::unordered_set<std::string> batchPks;

    const size_t neededOffsets = tbl->rowOffsets.size() + rows.size();
    if (neededOffsets > tbl->rowOffsets.capacity()) {
        tbl->rowOffsets.reserve(nextCapacity(tbl->rowOffsets.capacity(), neededOffsets));
    }
    if (tbl->schema.pkIndex >= 0) {
        const size_t neededPk = tbl->pkIdx.size() + rows.size();
        if (neededPk > tbl->pkIdx.bucket_count()) {
            tbl->pkIdx.reserve(nextCapacity(tbl->pkIdx.bucket_count(), neededPk));
        }
    }

    uint64_t offset = tbl->fileEndOffset;
    size_t inserted = 0;
    int64_t batchMinExpiry = tbl->minExpiryMs;

    for (const Row& row : rows) {
        if (row.fields.size() != tbl->schema.columns.size()) {
            err = "column count mismatch";
            return inserted;
        }

        if (tbl->schema.pkIndex >= 0) {
            const Field& pk = row.fields[static_cast<size_t>(tbl->schema.pkIndex)];
            if (!pk.has_value()) {
                err = "primary key cannot be null";
                return inserted;
            }
            if (tbl->pkIdx.find(*pk) != tbl->pkIdx.end() || batchPks.find(*pk) != batchPks.end()) {
                continue;
            }
            batchPks.insert(*pk);
            newPkEntries.push_back({*pk, tbl->rowOffsets.size() + newOffsets.size()});
        }

        newOffsets.push_back(offset);
        const size_t before = buffer.size();
        serializeRow(row, buffer);
        offset += static_cast<uint64_t>(buffer.size() - before);
        updateMinExpiry(row.expiry, batchMinExpiry);
        ++inserted;
    }

    if (inserted == 0) {
        err.clear();
        return 0;
    }

    const uint64_t walStart = tbl->walSize;
    if (!writeWal(*tbl, buffer.data(), buffer.size())) {
        err = "failed to write wal";
        return 0;
    }
    if (!writeAllFd(tbl->dataFd, buffer.data(), buffer.size())) {
        rollbackWal(*tbl, walStart);
        err = "failed to write rows";
        return 0;
    }

    tbl->rowOffsets.insert(tbl->rowOffsets.end(), newOffsets.begin(), newOffsets.end());
    tbl->fileEndOffset = offset;
    for (const auto& entry : newPkEntries) {
        tbl->pkIdx[entry.first] = entry.second;
    }
    tbl->minExpiryMs = batchMinExpiry;

    tbl->unsyncedRows += inserted;
    if (!syncTable(*tbl, tbl->unsyncedRows >= kSyncRowThreshold)) {
        err = "failed to sync table file";
        return inserted;
    }

    err.clear();
    return inserted;
}

size_t StorageEngine::deleteRows(const std::string& table,
                                 std::function<bool(const Row&)> pred,
                                 std::string& err) {
    Table* tbl = getTable(table);
    if (!tbl) {
        err = "table not found: " + table;
        return 0;
    }

    std::unique_lock<std::shared_mutex> lock(tbl->mtx);
    std::vector<Row> liveRows;
    liveRows.reserve(tbl->rowOffsets.size());
    Row row;
    size_t removed = 0;
    for (size_t i = 0; i < tbl->rowOffsets.size(); ++i) {
        if (!readRow(*tbl, i, row)) {
            continue;
        }
        if (pred(row)) {
            ++removed;
        } else {
            liveRows.push_back(row);
        }
    }

    if (::ftruncate(tbl->dataFd, 0) != 0 || ::lseek(tbl->dataFd, 0, SEEK_SET) < 0) {
        err = "failed to truncate table file";
        return 0;
    }

    std::vector<char> buffer;
    buffer.reserve(liveRows.size() * 128);
    tbl->rowOffsets.clear();
    tbl->pkIdx.clear();
    tbl->fileEndOffset = 0;
    tbl->minExpiryMs = 0;
    uint64_t offset = 0;
    for (const Row& live : liveRows) {
        tbl->rowOffsets.push_back(offset);
        const size_t before = buffer.size();
        serializeRow(live, buffer);
        offset += static_cast<uint64_t>(buffer.size() - before);
        if (tbl->schema.pkIndex >= 0) {
            const Field& pk = live.fields[static_cast<size_t>(tbl->schema.pkIndex)];
            if (pk.has_value()) {
                tbl->pkIdx[*pk] = tbl->rowOffsets.size() - 1;
            }
        }
        updateMinExpiry(live.expiry, tbl->minExpiryMs);
    }
    if (!buffer.empty() && !writeAllFd(tbl->dataFd, buffer.data(), buffer.size())) {
        err = "failed to rewrite table rows";
        return 0;
    }
    tbl->fileEndOffset = offset;
    tbl->unsyncedRows = static_cast<uint64_t>(liveRows.size());
    if (!syncTable(*tbl, true)) {
        err = "failed to sync rewritten table";
        return 0;
    }
    err.clear();
    return removed;
}

std::vector<std::string> StorageEngine::tableNames() const {
    std::shared_lock<std::shared_mutex> lock(engineMtx_);
    std::vector<std::string> names;
    names.reserve(tables_.size());
    for (const auto& kv : tables_) {
        names.push_back(kv.first);
    }
    return names;
}

size_t StorageEngine::rowCount(const Table& tbl) const {
    return tbl.rowOffsets.size();
}

bool StorageEngine::readRow(const Table& tbl, size_t index, Row& row) const {
    if (index >= tbl.rowOffsets.size()) {
        return false;
    }
    const uint64_t start = tbl.rowOffsets[index];
    const uint64_t end = (index + 1 < tbl.rowOffsets.size()) ? tbl.rowOffsets[index + 1] : tbl.fileEndOffset;
    if (end < start) {
        return false;
    }
    const size_t len = static_cast<size_t>(end - start);
    std::vector<char> buffer;
    buffer.resize(len);
    if (len > 0) {
        ssize_t n = ::pread(tbl.dataFd, buffer.data(), len, static_cast<off_t>(start));
        if (n != static_cast<ssize_t>(len)) {
            return false;
        }
    }
    return parseRowBuffer(buffer.data(), buffer.size(), row) == ReadStatus::OK;
}

bool StorageEngine::scanRows(const Table& tbl,
                             const std::function<bool(const Row&, size_t)>& visitor) const {
    SequentialScanner scanner(tbl);
    if (!scanner.open()) {
        return false;
    }

    Row row;
    size_t rowIndex = 0;
    while (true) {
        const ScanStatus status = scanner.next(row, tbl.schema.columns.size());
        if (status == ScanStatus::EOF_REACHED) {
            break;
        }
        if (status != ScanStatus::OK) {
            return false;
        }
        if (!visitor(row, rowIndex++)) {
            return true;
        }
    }
    return true;
}

void StorageEngine::loadTables() {
    std::unique_lock<std::shared_mutex> lock(engineMtx_);
    tables_.clear();

    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(dataDir_, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() == ".schema") {
            loadTableFromSchema(entry.path().string());
        }
    }
}

bool StorageEngine::loadTableFromSchema(const std::string& schemaPath) {
    TableSchema schema;
    if (!parseSchemaFile(schemaPath, schema)) {
        return false;
    }

    auto tbl = std::make_unique<Table>();
    tbl->schema = schema;
    tbl->schemaPath = schemaPath;
    tbl->dataPath = dataDir_ + "/" + schema.tableName + ".rows";
    tbl->walPath = dataDir_ + "/" + schema.tableName + ".wal";
    tbl->dataFd = ::open(tbl->dataPath.c_str(), O_RDWR | O_CREAT, 0644);
    if (tbl->dataFd < 0) {
        return false;
    }
    tbl->walFd = ::open(tbl->walPath.c_str(), O_RDWR | O_CREAT, 0644);
    if (tbl->walFd < 0) {
        return false;
    }
    if (!rebuildTableState(*tbl)) {
        return false;
    }
    if (!replayWal(*tbl)) {
        return false;
    }
    tbl->colLookup.build(schema.columns);  // build O(1) lookup after load
    tables_[schema.tableName] = std::move(tbl);
    return true;
}

bool StorageEngine::parseSchemaFile(const std::string& schemaPath, TableSchema& schema) const {
    std::ifstream in(schemaPath);
    if (!in) {
        return false;
    }

    std::string line;
    if (!std::getline(in, schema.tableName)) {
        return false;
    }
    if (!std::getline(in, line)) {
        return false;
    }
    schema.pkIndex = std::stoi(line);
    if (!std::getline(in, line)) {
        return false;
    }
    const size_t colCount = static_cast<size_t>(std::stoul(line));
    schema.columns.clear();
    schema.columns.reserve(colCount);
    for (size_t i = 0; i < colCount; ++i) {
        if (!std::getline(in, line)) {
            return false;
        }
        std::vector<std::string> parts;
        size_t start = 0;
        while (true) {
            size_t tab = line.find('\t', start);
            if (tab == std::string::npos) {
                parts.push_back(line.substr(start));
                break;
            }
            parts.push_back(line.substr(start, tab - start));
            start = tab + 1;
        }
        if (parts.size() != 6) {
            return false;
        }
        ColumnDef col;
        col.name = parts[0];
        col.type = typeFromString(parts[1]);
        col.notNull = std::stoi(parts[2]) != 0;
        col.primaryKey = std::stoi(parts[3]) != 0;
        col.varcharLen = std::stoi(parts[4]);
        col.index = std::stoi(parts[5]);
        schema.columns.push_back(std::move(col));
    }
    return true;
}

bool StorageEngine::rebuildTableState(Table& tbl) {
    tbl.rowOffsets.clear();
    tbl.pkIdx.clear();
    tbl.minExpiryMs = 0;

    struct stat st {};
    if (::fstat(tbl.dataFd, &st) != 0) {
        return false;
    }
    const uint64_t fileSize = static_cast<uint64_t>(st.st_size);

    uint64_t offset = 0;
    Row row;
    while (offset < fileSize) {
        uint64_t nextOffset = 0;
        ReadStatus status = readRowRecordAt(tbl.dataFd, fileSize, offset, row, nextOffset);
        if (status == ReadStatus::OK) {
            tbl.rowOffsets.push_back(offset);
            if (tbl.schema.pkIndex >= 0) {
                const Field& pk = row.fields[static_cast<size_t>(tbl.schema.pkIndex)];
                if (pk.has_value()) {
                    tbl.pkIdx[*pk] = tbl.rowOffsets.size() - 1;
                }
            }
            updateMinExpiry(row.expiry, tbl.minExpiryMs);
            offset = nextOffset;
            continue;
        }
        if (status == ReadStatus::TRUNCATED || status == ReadStatus::CORRUPT) {
            if (::ftruncate(tbl.dataFd, static_cast<off_t>(offset)) != 0) {
                return false;
            }
        }
        break;
    }

    ::lseek(tbl.dataFd, 0, SEEK_END);
    tbl.fileEndOffset = offset;
    tbl.walSize = 0;
    tbl.unsyncedRows = 0;
    return true;
}

void StorageEngine::rebuildPkIndex(Table& tbl) {
    tbl.pkIdx.clear();
    tbl.minExpiryMs = 0;
    if (tbl.schema.pkIndex < 0) {
        Row row;
        for (size_t i = 0; i < tbl.rowOffsets.size(); ++i) {
            if (!readRow(tbl, i, row)) {
                continue;
            }
            updateMinExpiry(row.expiry, tbl.minExpiryMs);
        }
        return;
    }
    Row row;
    for (size_t i = 0; i < tbl.rowOffsets.size(); ++i) {
        if (!readRow(tbl, i, row)) {
            continue;
        }
        const Field& value = row.fields[static_cast<size_t>(tbl.schema.pkIndex)];
        if (value.has_value()) {
            tbl.pkIdx[*value] = i;
        }
        updateMinExpiry(row.expiry, tbl.minExpiryMs);
    }
}

bool StorageEngine::writeSchemaFile(const TableSchema& schema, const std::string& path) const {
    std::string content;
    content.reserve(128 + schema.columns.size() * 48);
    content.append(schema.tableName).push_back('\n');
    content.append(std::to_string(schema.pkIndex)).push_back('\n');
    content.append(std::to_string(schema.columns.size())).push_back('\n');
    for (const ColumnDef& col : schema.columns) {
        content.append(col.name);
        content.push_back('\t');
        content.append(typeToString(col.type));
        content.push_back('\t');
        content.append(std::to_string(static_cast<int>(col.notNull)));
        content.push_back('\t');
        content.append(std::to_string(static_cast<int>(col.primaryKey)));
        content.push_back('\t');
        content.append(std::to_string(col.varcharLen));
        content.push_back('\t');
        content.append(std::to_string(col.index));
        content.push_back('\n');
    }

    const std::string tmpPath = path + ".tmp";
    int fd = ::open(tmpPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return false;
    }
    if (!writeAllFd(fd, content.data(), content.size())) {
        ::close(fd);
        ::unlink(tmpPath.c_str());
        return false;
    }
    if (::fdatasync(fd) != 0) {
        ::close(fd);
        ::unlink(tmpPath.c_str());
        return false;
    }
    if (::close(fd) != 0) {
        ::unlink(tmpPath.c_str());
        return false;
    }
    if (::rename(tmpPath.c_str(), path.c_str()) != 0) {
        ::unlink(tmpPath.c_str());
        return false;
    }
    return true;
}

void StorageEngine::serializeRow(const Row& row, std::vector<char>& out) const {
    const uint32_t fieldCount = static_cast<uint32_t>(row.fields.size());
    appendPod(out, fieldCount);
    appendPod(out, row.expiry);
    for (const Field& field : row.fields) {
        if (!field.has_value()) {
            appendPod(out, kNullFieldLen);
            continue;
        }
        const int32_t len = static_cast<int32_t>(field->size());
        appendPod(out, len);
        out.insert(out.end(), field->begin(), field->end());
    }
}

StorageEngine::ReadStatus StorageEngine::readRowRecordAt(int fd,
                                                         uint64_t fileSize,
                                                         uint64_t offset,
                                                         Row& row,
                                                         uint64_t& nextOffset) const {
    if (offset >= fileSize) {
        return ReadStatus::EOF_REACHED;
    }

    uint32_t fieldCount = 0;
    if (!readPodAt(fd, fileSize, offset, fieldCount)) {
        return ReadStatus::TRUNCATED;
    }
    offset += sizeof(fieldCount);

    int64_t expiry = 0;
    if (!readPodAt(fd, fileSize, offset, expiry)) {
        return ReadStatus::TRUNCATED;
    }
    offset += sizeof(expiry);

    row.fields.clear();
    row.fields.resize(fieldCount);
    row.expiry = expiry;

    for (uint32_t i = 0; i < fieldCount; ++i) {
        int32_t len = 0;
        if (!readPodAt(fd, fileSize, offset, len)) {
            return ReadStatus::TRUNCATED;
        }
        offset += sizeof(len);
        if (len == kNullFieldLen) {
            row.fields[static_cast<size_t>(i)].reset();
            continue;
        }
        if (len < 0 || len > static_cast<int32_t>(std::numeric_limits<int>::max() / 2)) {
            return ReadStatus::CORRUPT;
        }
        if (offset + static_cast<uint64_t>(len) > fileSize) {
            return ReadStatus::TRUNCATED;
        }
        if (!row.fields[static_cast<size_t>(i)].has_value()) {
            row.fields[static_cast<size_t>(i)].emplace();
        }
        std::string& value = *row.fields[static_cast<size_t>(i)];
        value.resize(static_cast<size_t>(len));
        if (len > 0) {
            ssize_t n = ::pread(fd, value.data(), static_cast<size_t>(len), static_cast<off_t>(offset));
            if (n != len) {
                return ReadStatus::TRUNCATED;
            }
        }
        offset += static_cast<uint64_t>(len);
    }

    nextOffset = offset;
    return ReadStatus::OK;
}

StorageEngine::ReadStatus StorageEngine::parseRowBuffer(const char* data, size_t len, Row& row) const {
    size_t offset = 0;
    if (len < sizeof(uint32_t) + sizeof(int64_t)) {
        return ReadStatus::TRUNCATED;
    }

    uint32_t fieldCount = 0;
    std::memcpy(&fieldCount, data + offset, sizeof(fieldCount));
    offset += sizeof(fieldCount);

    int64_t expiry = 0;
    std::memcpy(&expiry, data + offset, sizeof(expiry));
    offset += sizeof(expiry);

    row.fields.resize(fieldCount);
    row.expiry = expiry;

    for (uint32_t i = 0; i < fieldCount; ++i) {
        if (offset + sizeof(int32_t) > len) {
            return ReadStatus::TRUNCATED;
        }
        int32_t fieldLen = 0;
        std::memcpy(&fieldLen, data + offset, sizeof(fieldLen));
        offset += sizeof(fieldLen);
        if (fieldLen == kNullFieldLen) {
            row.fields[static_cast<size_t>(i)].reset();
            continue;
        }
        if (fieldLen < 0 || offset + static_cast<size_t>(fieldLen) > len) {
            return ReadStatus::CORRUPT;
        }
        if (!row.fields[static_cast<size_t>(i)].has_value()) {
            row.fields[static_cast<size_t>(i)].emplace();
        }
        row.fields[static_cast<size_t>(i)]->assign(data + offset, static_cast<size_t>(fieldLen));
        offset += static_cast<size_t>(fieldLen);
    }
    return ReadStatus::OK;
}

bool StorageEngine::syncTable(Table& tbl, bool force) const {
    if (!force) {
        return true;
    }
    if (::fdatasync(tbl.dataFd) != 0) {
        return false;
    }
    if (::ftruncate(tbl.walFd, 0) != 0) {
        return false;
    }
    if (::lseek(tbl.walFd, 0, SEEK_SET) < 0) {
        return false;
    }
    if (::fdatasync(tbl.walFd) != 0) {
        return false;
    }
    tbl.walSize = 0;
    tbl.unsyncedRows = 0;
    return true;
}

bool StorageEngine::writeAllFd(int fd, const char* data, size_t len) const {
    size_t total = 0;
    while (total < len) {
        ssize_t n = ::write(fd, data + total, len - total);
        if (n <= 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        total += static_cast<size_t>(n);
    }
    return true;
}

bool StorageEngine::writeWal(Table& tbl, const char* data, size_t len) const {
    const uint64_t walStart = tbl.walSize;
    if (::lseek(tbl.walFd, static_cast<off_t>(walStart), SEEK_SET) < 0) {
        return false;
    }
    if (!writeAllFd(tbl.walFd, data, len)) {
        rollbackWal(tbl, walStart);
        return false;
    }
    // NOTE: fdatasync removed from hot path — WAL is synced at syncTable().
    // Data is written to WAL before the data file, preserving crash-recovery.
    tbl.walSize = walStart + static_cast<uint64_t>(len);
    return true;
}

bool StorageEngine::rollbackWal(Table& tbl, uint64_t walStart) const {
    if (::ftruncate(tbl.walFd, static_cast<off_t>(walStart)) != 0) {
        return false;
    }
    if (::lseek(tbl.walFd, static_cast<off_t>(walStart), SEEK_SET) < 0) {
        return false;
    }
    tbl.walSize = walStart;
    return true;
}

bool StorageEngine::replayWal(Table& tbl) {
    struct stat st {};
    if (::fstat(tbl.walFd, &st) != 0) {
        return false;
    }
    const uint64_t walFileSize = static_cast<uint64_t>(st.st_size);
    tbl.walSize = walFileSize;
    if (walFileSize == 0) {
        return true;
    }

    std::vector<uint64_t> newOffsets;
    std::vector<std::pair<std::string, size_t>> newPkEntries;
    std::vector<char> rowBuffer;
    Row row;
    uint64_t walOffset = 0;
    uint64_t dataOffset = tbl.fileEndOffset;
    int64_t walMinExpiry = tbl.minExpiryMs;

    while (walOffset < walFileSize) {
        uint64_t nextWalOffset = 0;
        ReadStatus status = readRowRecordAt(tbl.walFd, walFileSize, walOffset, row, nextWalOffset);
        if (status != ReadStatus::OK) {
            if (::ftruncate(tbl.walFd, static_cast<off_t>(walOffset)) != 0) {
                return false;
            }
            tbl.walSize = walOffset;
            break;
        }

        if (row.fields.size() != tbl.schema.columns.size()) {
            if (::ftruncate(tbl.walFd, static_cast<off_t>(walOffset)) != 0) {
                return false;
            }
            tbl.walSize = walOffset;
            break;
        }

        bool skip = false;
        if (tbl.schema.pkIndex >= 0) {
            const Field& pk = row.fields[static_cast<size_t>(tbl.schema.pkIndex)];
            if (!pk.has_value() || tbl.pkIdx.find(*pk) != tbl.pkIdx.end()) {
                skip = true;
            } else {
                newPkEntries.push_back({*pk, tbl.rowOffsets.size() + newOffsets.size()});
            }
        }

        if (!skip) {
            rowBuffer.clear();
            serializeRow(row, rowBuffer);
            if (!writeAllFd(tbl.dataFd, rowBuffer.data(), rowBuffer.size())) {
                return false;
            }
            newOffsets.push_back(dataOffset);
            dataOffset += static_cast<uint64_t>(rowBuffer.size());
            updateMinExpiry(row.expiry, walMinExpiry);
        }

        walOffset = nextWalOffset;
    }

    if (!newOffsets.empty()) {
        if (tbl.rowOffsets.size() + newOffsets.size() > tbl.rowOffsets.capacity()) {
            tbl.rowOffsets.reserve(nextCapacity(tbl.rowOffsets.capacity(), tbl.rowOffsets.size() + newOffsets.size()));
        }
        tbl.rowOffsets.insert(tbl.rowOffsets.end(), newOffsets.begin(), newOffsets.end());
        for (const auto& entry : newPkEntries) {
            tbl.pkIdx[entry.first] = entry.second;
        }
        tbl.fileEndOffset = dataOffset;
        tbl.minExpiryMs = walMinExpiry;
        if (::fdatasync(tbl.dataFd) != 0) {
            return false;
        }
    }

    if (::ftruncate(tbl.walFd, 0) != 0) {
        return false;
    }
    if (::lseek(tbl.walFd, 0, SEEK_SET) < 0) {
        return false;
    }
    if (::fdatasync(tbl.walFd) != 0) {
        return false;
    }
    tbl.walSize = 0;
    return true;
}
