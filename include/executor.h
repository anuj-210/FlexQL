#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "lru_cache.h"
#include "parser.h"
#include "storage.h"

using ExecCallback = std::function<int(int colCount, std::vector<std::string>& values, std::vector<std::string>& colNames)>;
using ExecStreamCallback =
    std::function<int(int colCount, const std::vector<std::string_view>& values, const std::vector<std::string>& colNames)>;

struct ExecResult {
    bool ok = true;
    std::string error;
};

class Executor {
    StorageEngine& engine_;
    LRUCache& cache_;
    Parser parser_;

public:
    Executor(StorageEngine&, LRUCache&);
    ExecResult execute(const std::string& sql, const ExecCallback& cb);
    ExecResult streamSelect(const std::string& sql,
                            const std::function<bool(const std::vector<std::string>& colNames, uint32_t nrows)>& onHeader,
                            const ExecStreamCallback& onRow);

private:
    ExecResult doCreate(const CreateTableStmt&);
    ExecResult doInsert(const InsertStmt&);
    ExecResult doInsertBatch(const InsertBatchStmt&);
    ExecResult doSelect(const SelectStmt&, const ExecCallback&, const std::string& rawSql);
    bool matchWhere(const Row&, const TableSchema&, const WhereClause&) const;
    bool matchWhereFast(const Row&, const Table&, const WhereClause&) const;  // uses O(1) colLookup
    bool tryFastInsert(const std::string& sql, ExecResult& result);
};
