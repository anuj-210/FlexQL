#include "executor.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cctype>
#include <ctime>
#include <iomanip>
#include <limits>
#include <shared_mutex>
#include <sstream>
#include <string_view>
#include <unordered_map>

namespace {

constexpr size_t kMaxCachedSelectRows = 10000;

std::string trimCopy(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

bool startsWithInsertInto(const std::string& sql) {
    static const std::string prefix = "INSERT INTO ";
    if (sql.size() < prefix.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(sql[i])) != prefix[i]) {
            return false;
        }
    }
    return true;
}

int64_t nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

bool isExpired(const Row& row) {
    return row.expiry != 0 && nowMs() > row.expiry;
}

std::string normalizeSql(const std::string& sql) {
    size_t start = 0;
    while (start < sql.size() && std::isspace(static_cast<unsigned char>(sql[start]))) {
        ++start;
    }
    size_t end = sql.size();
    while (end > start && std::isspace(static_cast<unsigned char>(sql[end - 1]))) {
        --end;
    }
    return sql.substr(start, end - start);
}

bool isNumericType(ColType type) {
    return type == ColType::INT || type == ColType::DECIMAL;
}

std::string fieldToString(const Field& field) {
    return field.has_value() ? *field : "";
}

std::string_view fieldToView(const Field& field) {
    static const std::string empty;
    return field.has_value() ? std::string_view(*field) : std::string_view(empty);
}

bool parseIntStrict(const std::string& value, int64_t& out) {
    if (value.empty()) {
        return false;
    }
    size_t consumed = 0;
    try {
        out = std::stoll(value, &consumed);
    } catch (...) {
        return false;
    }
    return consumed == value.size();
}

bool parseDecimalStrict(const std::string& value, double& out) {
    if (value.empty()) {
        return false;
    }
    size_t consumed = 0;
    try {
        out = std::stod(value, &consumed);
    } catch (...) {
        return false;
    }
    return consumed == value.size();
}

int64_t parseExpiryMs(const std::string& value) {
    try {
        double raw = std::stod(value);
        int64_t v = static_cast<int64_t>(raw);
        if (v > 0 && v < 1000000000000LL) {
            return v * 1000LL;
        }
        return v;
    } catch (...) {
        return 0;
    }
}

bool parseDateTimeFormat(const std::string& value, const char* fmt, int64_t& outMs) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, fmt);
    if (input.fail()) {
        return false;
    }
    char trailing = '\0';
    if (input >> trailing) {
        return false;
    }
    tm.tm_isdst = -1;
    std::time_t seconds = ::mktime(&tm);
    if (seconds == static_cast<std::time_t>(-1)) {
        return false;
    }
    outMs = static_cast<int64_t>(seconds) * 1000LL;
    return true;
}

bool parseDateTimeStrict(const std::string& value, int64_t& outMs) {
    double numeric = 0.0;
    if (parseDecimalStrict(value, numeric)) {
        outMs = parseExpiryMs(value);
        return outMs != 0;
    }
    return parseDateTimeFormat(value, "%Y-%m-%d %H:%M:%S", outMs) ||
           parseDateTimeFormat(value, "%Y-%m-%dT%H:%M:%S", outMs) ||
           parseDateTimeFormat(value, "%Y-%m-%d", outMs);
}

bool compareText(const std::string& lhs, const std::string& op, const std::string& rhs) {
    if (op == "=") {
        return lhs == rhs;
    }
    if (op == "<") {
        return lhs < rhs;
    }
    if (op == ">") {
        return lhs > rhs;
    }
    if (op == "<=") {
        return lhs <= rhs;
    }
    if (op == ">=") {
        return lhs >= rhs;
    }
    return false;
}

bool compareNumeric(double lhs, const std::string& op, double rhs) {
    if (op == "=") {
        return lhs == rhs;
    }
    if (op == "<") {
        return lhs < rhs;
    }
    if (op == ">") {
        return lhs > rhs;
    }
    if (op == "<=") {
        return lhs <= rhs;
    }
    if (op == ">=") {
        return lhs >= rhs;
    }
    return false;
}

int findExpiryColIndex(const TableSchema& schema) {
    for (size_t i = 0; i < schema.columns.size(); ++i) {
        std::string upperName = schema.columns[i].name;
        std::transform(upperName.begin(), upperName.end(), upperName.begin(), [](unsigned char ch) {
            return static_cast<char>(std::toupper(ch));
        });
        if (upperName == "EXPIRES_AT") {
            return static_cast<int>(i);
        }
    }
    return -1;
}

bool validateAndAppendField(const ColumnDef& col,
                            const std::string& value,
                            bool isExpiryCol,
                            Row& row,
                            std::string& err) {
    if (value.empty() && col.notNull) {
        err = "NOT NULL column cannot be empty: " + col.name;
        return false;
    }

    switch (col.type) {
        case ColType::INT: {
            int64_t parsed = 0;
            if (!parseIntStrict(value, parsed)) {
                err = "invalid INT value for column " + col.name + ": " + value;
                return false;
            }
            if (isExpiryCol) {
                row.expiry = parseExpiryMs(value);
            }
            break;
        }
        case ColType::DECIMAL: {
            double parsed = 0.0;
            if (!parseDecimalStrict(value, parsed)) {
                err = "invalid DECIMAL value for column " + col.name + ": " + value;
                return false;
            }
            if (isExpiryCol) {
                row.expiry = parseExpiryMs(value);
            }
            break;
        }
        case ColType::VARCHAR:
            if (col.varcharLen > 0 && static_cast<int>(value.size()) > col.varcharLen) {
                err = "value too long for column " + col.name + " (VARCHAR(" + std::to_string(col.varcharLen) + "))";
                return false;
            }
            break;
        case ColType::DATETIME: {
            int64_t parsedMs = 0;
            if (!parseDateTimeStrict(value, parsedMs)) {
                err = "invalid DATETIME value for column " + col.name + ": " + value;
                return false;
            }
            if (isExpiryCol) {
                row.expiry = parsedMs;
            }
            break;
        }
        case ColType::TEXT:
            break;
    }

    row.fields.push_back(value);
    return true;
}

bool buildValidatedRow(const TableSchema& schema,
                       const std::vector<std::string>& values,
                       Row& row,
                       std::string& err) {
    if (values.size() != schema.columns.size()) {
        err = "column count mismatch";
        return false;
    }
    const int expiryColIndex = findExpiryColIndex(schema);
    if (expiryColIndex < 0) {
        err = "table schema must include EXPIRES_AT";
        return false;
    }

    row.fields.clear();
    row.fields.reserve(values.size());
    row.expiry = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!validateAndAppendField(schema.columns[i], values[i], static_cast<int>(i) == expiryColIndex, row, err)) {
            return false;
        }
    }
    if (row.expiry == 0) {
        err = "invalid expiration timestamp for EXPIRES_AT";
        return false;
    }
    return true;
}

struct Projection {
    std::string name;
    int tableId = 1;
    int colIndex = -1;
};

void skipSpaces(const std::string& text, size_t& pos) {
    while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos]))) {
        ++pos;
    }
}

template <typename Visitor>
bool visitFastInsertValues(const std::string& text, Visitor&& visitor) {
    size_t pos = 0;
    skipSpaces(text, pos);
    bool sawRow = false;
    while (pos < text.size()) {
        if (text[pos] != '(') {
            return false;
        }
        ++pos;

        std::vector<std::string> row;
        std::string current;
        bool inSingle = false;
        bool inDouble = false;

        while (pos < text.size()) {
            const char ch = text[pos++];
            if (ch == '\'' && !inDouble) {
                inSingle = !inSingle;
                continue;
            }
            if (ch == '"' && !inSingle) {
                inDouble = !inDouble;
                continue;
            }
            if (!inSingle && !inDouble) {
                if (ch == ',') {
                    row.push_back(trimCopy(current));
                    current.clear();
                    continue;
                }
                if (ch == ')') {
                    row.push_back(trimCopy(current));
                    current.clear();
                    break;
                }
            }
            current.push_back(ch);
        }

        if (inSingle || inDouble || row.empty()) {
            return false;
        }
        sawRow = true;
        if (!visitor(row)) {
            return false;
        }

        skipSpaces(text, pos);
        if (pos >= text.size()) {
            break;
        }
        if (text[pos] != ',') {
            return false;
        }
        ++pos;
        skipSpaces(text, pos);
    }
    return sawRow;
}

bool hasMultipleValueGroups(const std::string& text) {
    bool inSingle = false;
    bool inDouble = false;
    int depth = 0;
    for (size_t i = 0; i < text.size(); ++i) {
        const char ch = text[i];
        if (ch == '\'' && !inDouble) {
            inSingle = !inSingle;
            continue;
        }
        if (ch == '"' && !inSingle) {
            inDouble = !inDouble;
            continue;
        }
        if (inSingle || inDouble) {
            continue;
        }
        if (ch == '(') {
            ++depth;
            continue;
        }
        if (ch == ')' && depth > 0) {
            --depth;
            size_t next = i + 1;
            skipSpaces(text, next);
            if (depth == 0 && next < text.size() && text[next] == ',') {
                ++next;
                skipSpaces(text, next);
                if (next < text.size() && text[next] == '(') {
                    return true;
                }
            }
        }
    }
    return false;
}

bool parseSingleRowValues(const std::string& valuesPart, std::vector<std::string>& values) {
    if (valuesPart.size() < 2 || valuesPart.front() != '(' || valuesPart.back() != ')') {
        return false;
    }

    const std::string inner = valuesPart.substr(1, valuesPart.size() - 2);
    values.clear();
    std::string current;
    bool inSingle = false;
    bool inDouble = false;
    for (char ch : inner) {
        if (ch == '\'' && !inDouble) {
            inSingle = !inSingle;
            continue;
        }
        if (ch == '"' && !inSingle) {
            inDouble = !inDouble;
            continue;
        }
        if (ch == ',' && !inSingle && !inDouble) {
            values.push_back(trimCopy(current));
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    if (inSingle || inDouble) {
        return false;
    }
    values.push_back(trimCopy(current));
    return true;
}

}  // namespace

Executor::Executor(StorageEngine& engine, LRUCache& cache) : engine_(engine), cache_(cache) {}

ExecResult Executor::execute(const std::string& sql, const ExecCallback& cb) {
    (void)cb;
    ExecResult fastResult;
    if (tryFastInsert(sql, fastResult)) {
        return fastResult;
    }

    ParsedStmt parsed = parser_.parse(sql);
    if (!parsed.error.empty()) {
        return ExecResult{false, parsed.error};
    }

    switch (parsed.type) {
        case StmtType::CREATE_TABLE:
            return doCreate(std::get<CreateTableStmt>(parsed.stmt));
        case StmtType::INSERT:
            return doInsert(std::get<InsertStmt>(parsed.stmt));
        case StmtType::INSERT_BATCH:
            return doInsertBatch(std::get<InsertBatchStmt>(parsed.stmt));
        case StmtType::SELECT:
            return doSelect(std::get<SelectStmt>(parsed.stmt), cb, sql);
        case StmtType::DELETE:
            return ExecResult{false, "DELETE is not supported"};
        default:
            return ExecResult{false, "unsupported statement"};
    }
}

bool Executor::tryFastInsert(const std::string& sql, ExecResult& result) {
    if (!startsWithInsertInto(sql)) {
        return false;
    }

    const size_t valuesPos = sql.find(" VALUES ");
    if (valuesPos == std::string::npos) {
        return false;
    }
    const std::string table = trimCopy(sql.substr(std::string("INSERT INTO ").size(),
                                                  valuesPos - std::string("INSERT INTO ").size()));
    if (table.empty()) {
        return false;
    }

    std::string valuesPart = trimCopy(sql.substr(valuesPos + std::string(" VALUES ").size()));
    if (!valuesPart.empty() && valuesPart.back() == ';') {
        valuesPart.pop_back();
        valuesPart = trimCopy(valuesPart);
    }
    if (valuesPart.size() < 2 || valuesPart.front() != '(' || valuesPart.back() != ')') {
        return false;
    }

    Table* tbl = engine_.getTable(table);
    if (!tbl) {
        result = ExecResult{false, "table not found: " + table};
        return true;
    }

    if (!hasMultipleValueGroups(valuesPart)) {
        std::vector<std::string> values;
        if (!parseSingleRowValues(valuesPart, values)) {
            return false;
        }
        Row row;
        std::string validationErr;
        if (!buildValidatedRow(tbl->schema, values, row, validationErr)) {
            result = ExecResult{false, validationErr};
            return true;
        }

        std::string err;
        const size_t inserted = engine_.insertRow(table, std::move(row), err);
        if (!err.empty()) {
            result = ExecResult{false, err};
            return true;
        }
        if (inserted > 0) {
            cache_.invalidateTable(table);
        }
        result = ExecResult{};
        return true;
    }

    constexpr size_t kFastInsertChunkRows = 256;
    std::vector<Row> rows;
    rows.clear();
    rows.reserve(kFastInsertChunkRows);
    size_t totalInserted = 0;
    std::string err;

    auto flushRows = [&]() -> bool {
        if (rows.empty()) {
            return true;
        }
        const size_t inserted = engine_.insertRows(table, rows, err);
        if (!err.empty()) {
            result = ExecResult{false, err};
            return false;
        }
        totalInserted += inserted;
        rows.clear();
        return true;
    };

    const bool parsedOk = visitFastInsertValues(valuesPart, [&](const std::vector<std::string>& values) {
        Row row;
        std::string validationErr;
        if (!buildValidatedRow(tbl->schema, values, row, validationErr)) {
            result = ExecResult{false, validationErr};
            return false;
        }
        rows.push_back(std::move(row));
        if (rows.size() >= kFastInsertChunkRows) {
            return flushRows();
        }
        return true;
    });

    if (!parsedOk) {
        if (!result.ok) {
            return true;
        }
        return false;
    }

    if (!flushRows()) {
        return true;
    }
    if (totalInserted > 0) {
        cache_.invalidateTable(table);
    }
    result = ExecResult{};
    return true;
}

ExecResult Executor::doCreate(const CreateTableStmt& stmt) {
    if (stmt.schema.columns.empty()) {
        return ExecResult{false, "table must have at least one column"};
    }
    if (!engine_.createTable(stmt.schema, stmt.ifNotExists)) {
        return ExecResult{false, "table already exists: " + stmt.schema.tableName};
    }
    return ExecResult{};
}

ExecResult Executor::doInsertBatch(const InsertBatchStmt& stmt) {
    Table* tbl = engine_.getTable(stmt.table);
    if (!tbl) {
        return ExecResult{false, "table not found: " + stmt.table};
    }

    std::vector<Row> rows;
    rows.reserve(stmt.rows.size());
    for (const auto& values : stmt.rows) {
        Row row;
        std::string validationErr;
        if (!buildValidatedRow(tbl->schema, values, row, validationErr)) {
            return ExecResult{false, validationErr};
        }
        rows.push_back(std::move(row));
    }

    std::string err;
    const size_t inserted = engine_.insertRows(stmt.table, rows, err);
    if (!err.empty()) {
        return ExecResult{false, err};
    }
    if (inserted > 0) {
        cache_.invalidateTable(stmt.table);
    }
    return ExecResult{};
}

ExecResult Executor::doInsert(const InsertStmt& stmt) {
    Table* tbl = engine_.getTable(stmt.table);
    if (!tbl) {
        return ExecResult{false, "table not found: " + stmt.table};
    }
    Row row;
    std::string validationErr;
    if (!buildValidatedRow(tbl->schema, stmt.values, row, validationErr)) {
        return ExecResult{false, validationErr};
    }
    if (stmt.expiryMs != 0) {
        row.expiry = stmt.expiryMs;
    }

    std::string err;
    const size_t inserted = engine_.insertRow(stmt.table, std::move(row), err);
    if (!err.empty()) {
        return ExecResult{false, err};
    }
    if (inserted > 0) {
        cache_.invalidateTable(stmt.table);
    }
    return ExecResult{};
}

ExecResult Executor::streamSelect(
    const std::string& sql,
    const std::function<bool(const std::vector<std::string>& colNames, uint32_t nrows)>& onHeader,
    const ExecStreamCallback& onRow) {
    ParsedStmt parsed = parser_.parse(sql);
    if (!parsed.error.empty()) {
        return ExecResult{false, parsed.error};
    }
    if (parsed.type != StmtType::SELECT) {
        return ExecResult{false, "unsupported statement"};
    }

    const SelectStmt& stmt = std::get<SelectStmt>(parsed.stmt);
    const std::string cacheKey = normalizeSql(sql);

    if (!stmt.join.has_value()) {
        if (auto cached = cache_.get(cacheKey)) {
            if (!onHeader(cached->colNames, static_cast<uint32_t>(cached->rows.size()))) {
                return ExecResult{false, "failed to write response header"};
            }
            if (onRow) {
                std::vector<std::string> cols = cached->colNames;
                std::vector<std::string_view> rowViews;
                for (const auto& row : cached->rows) {
                    rowViews.clear();
                    rowViews.reserve(row.size());
                    for (const auto& value : row) {
                        rowViews.emplace_back(value);
                    }
                    if (onRow(static_cast<int>(rowViews.size()), rowViews, cols) == 1) {
                        break;
                    }
                }
            }
            return ExecResult{};
        }

        Table* tbl = engine_.getTable(stmt.table);
        if (!tbl) {
            return ExecResult{false, "table not found: " + stmt.table};
        }

        std::shared_lock<std::shared_mutex> lock(tbl->mtx);
        std::vector<int> projIndices;
        std::vector<std::string> colNames;
        if (stmt.columns.size() == 1 && stmt.columns[0] == "*") {
            projIndices.reserve(tbl->schema.columns.size());
            colNames.reserve(tbl->schema.columns.size());
            for (size_t i = 0; i < tbl->schema.columns.size(); ++i) {
                projIndices.push_back(static_cast<int>(i));
                colNames.push_back(tbl->schema.columns[i].name);
            }
        } else {
            projIndices.reserve(stmt.columns.size());
            colNames.reserve(stmt.columns.size());
            for (const std::string& col : stmt.columns) {
                const std::string bare = col.substr(col.rfind('.') == std::string::npos ? 0 : col.rfind('.') + 1);
                int idx = tbl->colLookup.find(bare);
                if (idx < 0) {
                    return ExecResult{false, "unknown column: " + bare};
                }
                projIndices.push_back(idx);
                colNames.push_back(bare);
            }
        }

        auto rowMatches = [&](const Row& row) -> bool {
            if (isExpired(row)) {
                return false;
            }
            if (stmt.where.has_value() && !matchWhereFast(row, *tbl, *stmt.where)) {
                return false;
            }
            return true;
        };

        bool usedPkLookup = false;
        bool countPkHit = false;
        size_t pkRowIndex = 0;
        size_t matchedRows = 0;
        const bool canUseRowCountFastPath =
            !stmt.where.has_value() && (tbl->minExpiryMs == 0 || nowMs() <= tbl->minExpiryMs);
        if (stmt.where.has_value() && stmt.where->op == "=" && tbl->schema.pkIndex >= 0 &&
            tbl->schema.columns[static_cast<size_t>(tbl->schema.pkIndex)].name == stmt.where->column) {
            usedPkLookup = true;
            auto it = tbl->pkIdx.find(stmt.where->value);
            if (it != tbl->pkIdx.end() && it->second < engine_.rowCount(*tbl)) {
                Row row;
                if (!engine_.readRow(*tbl, it->second, row)) {
                    return ExecResult{false, "failed to read row from storage"};
                }
                if (rowMatches(row)) {
                    countPkHit = true;
                    pkRowIndex = it->second;
                    matchedRows = 1;
                }
            }
        }

        if (!usedPkLookup && canUseRowCountFastPath) {
            matchedRows = engine_.rowCount(*tbl);
        } else if (!usedPkLookup) {
            if (!engine_.scanRows(*tbl, [&](const Row& row, size_t) {
                if (rowMatches(row)) {
                    ++matchedRows;
                }
                return true;
            })) {
                return ExecResult{false, "failed to scan rows"};
            }
        }

        if (matchedRows > std::numeric_limits<uint32_t>::max()) {
            return ExecResult{false, "result set too large"};
        }
        if (!onHeader(colNames, static_cast<uint32_t>(matchedRows))) {
            return ExecResult{false, "failed to write response header"};
        }

        bool stopRequested = false;
        bool cacheable = matchedRows <= kMaxCachedSelectRows;
        CacheEntry cacheEntry;
        if (cacheable) {
            cacheEntry.colNames = colNames;
            cacheEntry.rows.reserve(matchedRows);
        }
        std::vector<std::string> callbackCols = colNames;
        std::vector<std::string_view> values;
        values.reserve(projIndices.size());
        auto emitProjected = [&](const Row& row) -> bool {
            values.clear();
            for (int idx : projIndices) {
                values.push_back(fieldToView(row.fields[static_cast<size_t>(idx)]));
            }
            if (cacheable) {
                auto& cachedRow = cacheEntry.rows.emplace_back();
                cachedRow.reserve(values.size());
                for (const auto& value : values) {
                    cachedRow.emplace_back(value);
                }
            }
            if (onRow && onRow(static_cast<int>(values.size()), values, callbackCols) == 1) {
                stopRequested = true;
                return false;
            }
            return true;
        };

        if (usedPkLookup) {
            if (countPkHit) {
                Row row;
                if (!engine_.readRow(*tbl, pkRowIndex, row)) {
                    return ExecResult{false, "failed to read row from storage"};
                }
                if (rowMatches(row)) {
                    emitProjected(row);
                }
            }
        } else {
            if (!engine_.scanRows(*tbl, [&](const Row& row, size_t) {
                if (!rowMatches(row)) {
                    return true;
                }
                return emitProjected(row);
            })) {
                if (!stopRequested) {
                    return ExecResult{false, "failed to scan rows"};
                }
            }
        }

        if (!stopRequested && cacheable) {
            cache_.put(cacheKey, std::move(cacheEntry));
        }
        return ExecResult{};
    }

    const JoinClause& join = *stmt.join;
    Table* t1 = engine_.getTable(stmt.table);
    Table* t2 = engine_.getTable(join.table2);
    if (!t1) {
        return ExecResult{false, "table not found: " + stmt.table};
    }
    if (!t2) {
        return ExecResult{false, "table not found: " + join.table2};
    }

    Table* first = t1;
    Table* second = t2;
    if (t2->schema.tableName < t1->schema.tableName) {
        first = t2;
        second = t1;
    }
    std::shared_lock<std::shared_mutex> lock1(first->mtx);
    std::shared_lock<std::shared_mutex> lock2(second->mtx);

    const std::string joinLeftBare = join.col1.substr(join.col1.rfind('.') == std::string::npos ? 0 : join.col1.rfind('.') + 1);
    const std::string joinRightBare = join.col2.substr(join.col2.rfind('.') == std::string::npos ? 0 : join.col2.rfind('.') + 1);
    const int leftJoinIdx = t1->schema.colIndex(joinLeftBare);
    const int rightJoinIdx = t2->schema.colIndex(joinRightBare);
    if (leftJoinIdx < 0 || rightJoinIdx < 0) {
        return ExecResult{false, "invalid JOIN columns"};
    }

    std::vector<Row> rightRows;
    rightRows.reserve(engine_.rowCount(*t2));
    std::unordered_map<std::string, std::vector<size_t>> rightHash;
    if (!engine_.scanRows(*t2, [&](const Row& row, size_t) {
        if (isExpired(row)) {
            return true;
        }
        const Field& key = row.fields[static_cast<size_t>(rightJoinIdx)];
        rightRows.push_back(row);
        if (key.has_value()) {
            rightHash[*key].push_back(rightRows.size() - 1);
        }
        return true;
    })) {
        return ExecResult{false, "failed to scan join table"};
    }

    auto resolveProjection = [&](const std::string& requested, Projection& out) -> bool {
        if (requested == "*") {
            return false;
        }
        size_t dot = requested.find('.');
        if (dot != std::string::npos) {
            std::string tbl = requested.substr(0, dot);
            std::string col = requested.substr(dot + 1);
            if (tbl == t1->schema.tableName) {
                int idx = t1->schema.colIndex(col);
                if (idx >= 0) {
                    out = Projection{col, 1, idx};
                    return true;
                }
            }
            if (tbl == t2->schema.tableName) {
                int idx = t2->schema.colIndex(col);
                if (idx >= 0) {
                    out = Projection{col, 2, idx};
                    return true;
                }
            }
            return false;
        }
        int idx = t1->schema.colIndex(requested);
        if (idx >= 0) {
            out = Projection{requested, 1, idx};
            return true;
        }
        idx = t2->schema.colIndex(requested);
        if (idx >= 0) {
            out = Projection{requested, 2, idx};
            return true;
        }
        return false;
    };

    std::vector<Projection> projections;
    std::vector<std::string> colNames;
    for (const std::string& col : stmt.columns) {
        Projection proj;
        if (!resolveProjection(col, proj)) {
            return ExecResult{false, "unknown column: " + col};
        }
        projections.push_back(proj);
        colNames.push_back(proj.name);
    }

    auto whereMatchesJoin = [&](const Row& leftRow, const Row& rightRow, bool& unknownCol) -> bool {
        if (!stmt.where.has_value()) {
            return true;
        }
        const WhereClause& where = *stmt.where;
        int idx = t1->schema.colIndex(where.column);
        if (idx >= 0) {
            return matchWhere(leftRow, t1->schema, where);
        }
        idx = t2->schema.colIndex(where.column);
        if (idx >= 0) {
            return matchWhere(rightRow, t2->schema, where);
        }
        unknownCol = true;
        return false;
    };

    bool joinWhereUnknown = false;
    size_t matchedRows = 0;
    if (!engine_.scanRows(*t1, [&](const Row& leftRow, size_t) {
        if (isExpired(leftRow)) {
            return true;
        }
        const Field& joinValue = leftRow.fields[static_cast<size_t>(leftJoinIdx)];
        if (!joinValue.has_value()) {
            return true;
        }
        auto hit = rightHash.find(*joinValue);
        if (hit == rightHash.end()) {
            return true;
        }
        for (size_t rightOffset : hit->second) {
            bool unknown = false;
            if (!whereMatchesJoin(leftRow, rightRows[rightOffset], unknown)) {
                if (unknown) {
                    joinWhereUnknown = true;
                    return false;
                }
                continue;
            }
            ++matchedRows;
        }
        return true;
    })) {
        if (joinWhereUnknown) {
            return ExecResult{false, "unknown column in WHERE: " + stmt.where->column};
        }
        return ExecResult{false, "failed to scan left join table"};
    }

    if (matchedRows > std::numeric_limits<uint32_t>::max()) {
        return ExecResult{false, "result set too large"};
    }
    if (!onHeader(colNames, static_cast<uint32_t>(matchedRows))) {
        return ExecResult{false, "failed to write response header"};
    }

    bool stopRequested = false;
    std::vector<std::string> callbackCols = colNames;
    std::vector<std::string_view> values;
    values.reserve(projections.size());
    if (!engine_.scanRows(*t1, [&](const Row& leftRow, size_t) {
        if (isExpired(leftRow)) {
            return true;
        }
        const Field& joinValue = leftRow.fields[static_cast<size_t>(leftJoinIdx)];
        if (!joinValue.has_value()) {
            return true;
        }
        auto hit = rightHash.find(*joinValue);
        if (hit == rightHash.end()) {
            return true;
        }
        for (size_t rightOffset : hit->second) {
            const Row& rightRow = rightRows[rightOffset];
            bool unknown = false;
            if (!whereMatchesJoin(leftRow, rightRow, unknown)) {
                if (unknown) {
                    joinWhereUnknown = true;
                    return false;
                }
                continue;
            }
            values.clear();
            for (const Projection& proj : projections) {
                if (proj.tableId == 1) {
                    values.push_back(fieldToView(leftRow.fields[static_cast<size_t>(proj.colIndex)]));
                } else {
                    values.push_back(fieldToView(rightRow.fields[static_cast<size_t>(proj.colIndex)]));
                }
            }
            if (onRow && onRow(static_cast<int>(values.size()), values, callbackCols) == 1) {
                stopRequested = true;
                return false;
            }
        }
        return true;
    })) {
        if (joinWhereUnknown) {
            return ExecResult{false, "unknown column in WHERE: " + stmt.where->column};
        }
        if (!stopRequested) {
            return ExecResult{false, "failed to scan left join table"};
        }
    }

    return ExecResult{};
}

bool Executor::matchWhere(const Row& row, const TableSchema& schema, const WhereClause& where) const {
    const int colIdx = schema.colIndex(where.column);
    if (colIdx < 0 || static_cast<size_t>(colIdx) >= row.fields.size()) {
        return false;
    }
    const Field& field = row.fields[static_cast<size_t>(colIdx)];
    if (!field.has_value()) {
        return false;
    }

    const ColumnDef& col = schema.columns[static_cast<size_t>(colIdx)];
    if (isNumericType(col.type)) {
        try {
            double lhs = std::stod(*field);
            double rhs = std::stod(where.value);
            return compareNumeric(lhs, where.op, rhs);
        } catch (...) {
            return false;
        }
    }
    return compareText(*field, where.op, where.value);
}

// Fast version that uses prebuilt O(1) column lookup instead of linear scan.
bool Executor::matchWhereFast(const Row& row, const Table& tbl, const WhereClause& where) const {
    const int colIdx = tbl.colLookup.find(where.column);
    if (colIdx < 0 || static_cast<size_t>(colIdx) >= row.fields.size()) {
        return false;
    }
    const Field& field = row.fields[static_cast<size_t>(colIdx)];
    if (!field.has_value()) {
        return false;
    }
    const ColumnDef& col = tbl.schema.columns[static_cast<size_t>(colIdx)];
    if (isNumericType(col.type)) {
        try {
            double lhs = std::stod(*field);
            double rhs = std::stod(where.value);
            return compareNumeric(lhs, where.op, rhs);
        } catch (...) {
            return false;
        }
    }
    return compareText(*field, where.op, where.value);
}

ExecResult Executor::doSelect(const SelectStmt& stmt, const ExecCallback& cb, const std::string& rawSql) {
    if (!stmt.join.has_value()) {
        const std::string cacheKey = normalizeSql(rawSql);
        if (auto cached = cache_.get(cacheKey)) {
            if (cb) {
                for (auto row : cached->rows) {
                    std::vector<std::string> cols = cached->colNames;
                    if (cb(static_cast<int>(row.size()), row, cols) == 1) {
                        break;
                    }
                }
            }
            return ExecResult{};
        }

        Table* tbl = engine_.getTable(stmt.table);
        if (!tbl) {
            return ExecResult{false, "table not found: " + stmt.table};
        }

        std::shared_lock<std::shared_mutex> lock(tbl->mtx);
        std::vector<int> projIndices;
        std::vector<std::string> colNames;
        if (stmt.columns.size() == 1 && stmt.columns[0] == "*") {
            for (size_t i = 0; i < tbl->schema.columns.size(); ++i) {
                projIndices.push_back(static_cast<int>(i));
                colNames.push_back(tbl->schema.columns[i].name);
            }
        } else {
            for (const std::string& col : stmt.columns) {
                const std::string bare = col.substr(col.rfind('.') == std::string::npos ? 0 : col.rfind('.') + 1);
                // Use O(1) colLookup instead of O(n) colIndex()
                int idx = tbl->colLookup.find(bare);
                if (idx < 0) {
                    return ExecResult{false, "unknown column: " + bare};
                }
                projIndices.push_back(idx);
                colNames.push_back(bare);
            }
        }

        std::vector<std::vector<std::string>> cacheRows;
        cacheRows.reserve(std::min(kMaxCachedSelectRows, static_cast<size_t>(1024)));
        bool cacheable = true;
        bool stopRequested = false;
        // colNamesCopy is passed by ref into cb — avoid re-copying on every row.
        std::vector<std::string> colNamesCopy = colNames;
        auto emitRow = [&](const Row& row) {
            std::vector<std::string> values;
            values.reserve(projIndices.size());
            for (int idx : projIndices) {
                values.push_back(fieldToString(row.fields[static_cast<size_t>(idx)]));
            }

            if (cb) {
                if (cb(static_cast<int>(values.size()), values, colNamesCopy) == 1) {
                    stopRequested = true;
                    return false;
                }
            }

            if (cacheable) {
                if (cacheRows.size() < kMaxCachedSelectRows) {
                    cacheRows.push_back(values);
                } else {
                    cacheable = false;
                    cacheRows.clear();
                }
            }
            return true;
        };

        bool usedPkLookup = false;
        if (stmt.where.has_value() && stmt.where->op == "=" && tbl->schema.pkIndex >= 0 &&
            tbl->schema.columns[static_cast<size_t>(tbl->schema.pkIndex)].name == stmt.where->column) {
            auto it = tbl->pkIdx.find(stmt.where->value);
            usedPkLookup = true;
            if (it != tbl->pkIdx.end() && it->second < engine_.rowCount(*tbl)) {
                Row row;
                if (!engine_.readRow(*tbl, it->second, row)) {
                    return ExecResult{false, "failed to read row from storage"};
                }
                if (!isExpired(row) && matchWhere(row, tbl->schema, *stmt.where)) {
                    emitRow(row);
                }
            }
        }

        if (!usedPkLookup) {
            if (!engine_.scanRows(*tbl, [&](const Row& row, size_t) {
                if (isExpired(row)) {
                    return true;
                }
                // Use fast O(1) colLookup version in the full-scan hot path
                if (stmt.where.has_value() && !matchWhereFast(row, *tbl, *stmt.where)) {
                    return true;
                }
                return emitRow(row);
            })) {
                if (stopRequested) {
                    return ExecResult{};
                }
                return ExecResult{false, "failed to scan rows"};
            }
        }

        if (stopRequested) {
            return ExecResult{};
        }
        if (cacheable) {
            cache_.put(cacheKey, CacheEntry{colNames, std::move(cacheRows)});
        }
        return ExecResult{};
    }

    const JoinClause& join = *stmt.join;
    Table* t1 = engine_.getTable(stmt.table);
    Table* t2 = engine_.getTable(join.table2);
    if (!t1) {
        return ExecResult{false, "table not found: " + stmt.table};
    }
    if (!t2) {
        return ExecResult{false, "table not found: " + join.table2};
    }

    Table* first = t1;
    Table* second = t2;
    if (t2->schema.tableName < t1->schema.tableName) {
        first = t2;
        second = t1;
    }
    std::shared_lock<std::shared_mutex> lock1(first->mtx);
    std::shared_lock<std::shared_mutex> lock2(second->mtx);

    const std::string joinLeftBare = join.col1.substr(join.col1.rfind('.') == std::string::npos ? 0 : join.col1.rfind('.') + 1);
    const std::string joinRightBare = join.col2.substr(join.col2.rfind('.') == std::string::npos ? 0 : join.col2.rfind('.') + 1);
    const int leftJoinIdx = t1->schema.colIndex(joinLeftBare);
    const int rightJoinIdx = t2->schema.colIndex(joinRightBare);
    if (leftJoinIdx < 0 || rightJoinIdx < 0) {
        return ExecResult{false, "invalid JOIN columns"};
    }

    std::vector<Row> rightRows;
    rightRows.reserve(engine_.rowCount(*t2));
    std::unordered_map<std::string, std::vector<size_t>> rightHash;
    if (!engine_.scanRows(*t2, [&](const Row& row, size_t) {
        if (isExpired(row)) {
            return true;
        }
        const Field& key = row.fields[static_cast<size_t>(rightJoinIdx)];
        rightRows.push_back(row);
        if (key.has_value()) {
            rightHash[*key].push_back(rightRows.size() - 1);
        }
        return true;
    })) {
        return ExecResult{false, "failed to scan join table"};
    }

    auto resolveProjection = [&](const std::string& requested, Projection& out) -> bool {
        if (requested == "*") {
            return false;
        }
        size_t dot = requested.find('.');
        if (dot != std::string::npos) {
            std::string tbl = requested.substr(0, dot);
            std::string col = requested.substr(dot + 1);
            if (tbl == t1->schema.tableName) {
                int idx = t1->schema.colIndex(col);
                if (idx >= 0) {
                    out = Projection{col, 1, idx};
                    return true;
                }
            }
            if (tbl == t2->schema.tableName) {
                int idx = t2->schema.colIndex(col);
                if (idx >= 0) {
                    out = Projection{col, 2, idx};
                    return true;
                }
            }
            return false;
        }
        int idx = t1->schema.colIndex(requested);
        if (idx >= 0) {
            out = Projection{requested, 1, idx};
            return true;
        }
        idx = t2->schema.colIndex(requested);
        if (idx >= 0) {
            out = Projection{requested, 2, idx};
            return true;
        }
        return false;
    };

    std::vector<Projection> projections;
    std::vector<std::string> colNames;
    for (const std::string& col : stmt.columns) {
        Projection proj;
        if (!resolveProjection(col, proj)) {
            return ExecResult{false, "unknown column: " + col};
        }
        projections.push_back(proj);
        colNames.push_back(proj.name);
    }

    struct JoinedRow {
        std::vector<std::string> values;
        std::string orderValue;
    };
    std::vector<JoinedRow> joinedRows;

    bool joinWhereUnknown = false;
    if (!engine_.scanRows(*t1, [&](const Row& leftRow, size_t) {
        if (isExpired(leftRow)) {
            return true;
        }
        const Field& joinValue = leftRow.fields[static_cast<size_t>(leftJoinIdx)];
        if (!joinValue.has_value()) {
            return true;
        }
        auto hit = rightHash.find(*joinValue);
        if (hit == rightHash.end()) {
            return true;
        }
        for (size_t rightOffset : hit->second) {
            const Row& rightRow = rightRows[rightOffset];
            if (stmt.where.has_value()) {
                const WhereClause& where = *stmt.where;
                int idx = t1->schema.colIndex(where.column);
                bool matched = false;
                if (idx >= 0) {
                    matched = matchWhere(leftRow, t1->schema, where);
                } else {
                    idx = t2->schema.colIndex(where.column);
                    if (idx >= 0) {
                        matched = matchWhere(rightRow, t2->schema, where);
                    } else {
                        joinWhereUnknown = true;
                        return false;
                    }
                }
                if (!matched) {
                    continue;
                }
            }

            JoinedRow out;
            out.values.reserve(projections.size());
            for (const Projection& proj : projections) {
                if (proj.tableId == 1) {
                    out.values.push_back(fieldToString(leftRow.fields[static_cast<size_t>(proj.colIndex)]));
                } else {
                    out.values.push_back(fieldToString(rightRow.fields[static_cast<size_t>(proj.colIndex)]));
                }
            }
            joinedRows.push_back(std::move(out));
        }
        return true;
    })) {
        if (joinWhereUnknown) {
            return ExecResult{false, "unknown column in WHERE: " + stmt.where->column};
        }
        return ExecResult{false, "failed to scan left join table"};
    }

    if (cb) {
        for (auto& row : joinedRows) {
            std::vector<std::string> cols = colNames;
            if (cb(static_cast<int>(row.values.size()), row.values, cols) == 1) {
                break;
            }
        }
    }
    return ExecResult{};
}
