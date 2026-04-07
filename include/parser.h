#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "schema.h"

enum class StmtType { CREATE_TABLE, INSERT, INSERT_BATCH, SELECT, DELETE, UNKNOWN };

struct WhereClause {
    std::string column;
    std::string op;
    std::string value;
};

struct JoinClause {
    std::string table2;
    std::string col1;
    std::string col2;
};

struct OrderByClause {
    std::string column;
    bool desc = false;
};

struct CreateTableStmt {
    TableSchema schema;
    bool ifNotExists = false;
};

struct InsertStmt {
    std::string table;
    std::vector<std::string> values;
    int64_t expiryMs = 0;
};

struct InsertBatchStmt {
    std::string table;
    std::vector<std::vector<std::string>> rows;
};

struct SelectStmt {
    std::string table;
    std::vector<std::string> columns;
    std::optional<WhereClause> where;
    std::optional<JoinClause> join;
    std::optional<OrderByClause> orderBy;
};

struct DeleteStmt {
    std::string table;
    std::optional<WhereClause> where;
};

using ParsedVariant = std::variant<std::monostate, CreateTableStmt, InsertStmt, InsertBatchStmt, SelectStmt, DeleteStmt>;

struct ParsedStmt {
    StmtType type = StmtType::UNKNOWN;
    ParsedVariant stmt;
    std::string error;
};

class Parser {
public:
    ParsedStmt parse(const std::string& sql) const;

private:
    ParsedStmt parseCreate(const std::string& sql) const;
    ParsedStmt parseInsert(const std::string& sql) const;
    ParsedStmt parseSelect(const std::string& sql) const;
    ParsedStmt parseDelete(const std::string& sql) const;
};
