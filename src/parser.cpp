#include "parser.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <optional>
#include <sstream>

namespace {

std::string trim(const std::string& s) {
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

std::string stripTrailingSemicolons(std::string s) {
    s = trim(s);
    while (!s.empty() && s.back() == ';') {
        s.pop_back();
        s = trim(s);
    }
    return s;
}

std::string toUpper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return s;
}

bool isWordBoundary(const std::string& s, size_t pos, size_t len) {
    const bool leftOk = pos == 0 || std::isspace(static_cast<unsigned char>(s[pos - 1]));
    const size_t end = pos + len;
    const bool rightOk = end >= s.size() || std::isspace(static_cast<unsigned char>(s[end]));
    return leftOk && rightOk;
}

size_t findKeywordCI(const std::string& sql, const std::string& keyword, size_t start = 0) {
    const std::string upperSql = toUpper(sql);
    const std::string upperKey = toUpper(keyword);
    size_t pos = upperSql.find(upperKey, start);
    while (pos != std::string::npos) {
        if (isWordBoundary(upperSql, pos, upperKey.size())) {
            return pos;
        }
        pos = upperSql.find(upperKey, pos + 1);
    }
    return std::string::npos;
}

std::string stripQuotes(const std::string& s) {
    std::string out = trim(s);
    if (out.size() >= 2) {
        if ((out.front() == '\'' && out.back() == '\'') || (out.front() == '"' && out.back() == '"')) {
            return out.substr(1, out.size() - 2);
        }
    }
    return out;
}

std::string stripTablePrefix(const std::string& name) {
    const std::string t = trim(name);
    size_t dot = t.rfind('.');
    if (dot == std::string::npos) {
        return t;
    }
    return trim(t.substr(dot + 1));
}

std::vector<std::string> splitCSV(const std::string& input) {
    std::vector<std::string> parts;
    std::string cur;
    bool inSingle = false;
    bool inDouble = false;
    int parenDepth = 0;
    for (char ch : input) {
        if (ch == '\'' && !inDouble) {
            inSingle = !inSingle;
            cur.push_back(ch);
            continue;
        }
        if (ch == '"' && !inSingle) {
            inDouble = !inDouble;
            cur.push_back(ch);
            continue;
        }
        if (!inSingle && !inDouble) {
            if (ch == '(') {
                ++parenDepth;
            } else if (ch == ')' && parenDepth > 0) {
                --parenDepth;
            } else if (ch == ',' && parenDepth == 0) {
                parts.push_back(trim(cur));
                cur.clear();
                continue;
            }
        }
        cur.push_back(ch);
    }
    if (!cur.empty()) {
        parts.push_back(trim(cur));
    }
    return parts;
}

std::vector<std::string> splitValues(const std::string& input) {
    std::vector<std::string> values;
    for (const std::string& part : splitCSV(input)) {
        values.push_back(stripQuotes(part));
    }
    return values;
}

std::vector<std::string> splitValueGroups(const std::string& input) {
    std::vector<std::string> groups;
    bool inSingle = false;
    bool inDouble = false;
    int depth = 0;
    std::string cur;
    for (char ch : input) {
        if (ch == '\'' && !inDouble) {
            inSingle = !inSingle;
        } else if (ch == '"' && !inSingle) {
            inDouble = !inDouble;
        }
        if (!inSingle && !inDouble) {
            if (ch == '(') {
                if (depth == 0) {
                    cur.clear();
                } else {
                    cur.push_back(ch);
                }
                ++depth;
                continue;
            }
            if (ch == ')' && depth > 0) {
                --depth;
                if (depth == 0) {
                    groups.push_back(trim(cur));
                    cur.clear();
                    continue;
                }
            }
            if (depth == 0) {
                continue;
            }
        }
        if (depth > 0) {
            cur.push_back(ch);
        }
    }
    return groups;
}

bool parseWhere(const std::string& clause, WhereClause& where) {
    static const char* ops[] = {"<=", ">=", "<", ">", "="};
    const std::string expr = trim(clause);
    for (const char* op : ops) {
        size_t pos = expr.find(op);
        if (pos != std::string::npos) {
            where.column = stripTablePrefix(expr.substr(0, pos));
            where.op = op;
            where.value = stripQuotes(expr.substr(pos + std::strlen(op)));
            return !where.column.empty();
        }
    }
    return false;
}

std::optional<ColType> parseType(const std::string& s) {
    std::string upper = toUpper(trim(s));
    if (upper == "INT") {
        return ColType::INT;
    }
    if (upper == "DECIMAL") {
        return ColType::DECIMAL;
    }
    if (upper == "DATETIME") {
        return ColType::DATETIME;
    }
    if (upper.rfind("VARCHAR", 0) == 0) {
        return ColType::VARCHAR;
    }
    return std::nullopt;
}

}  // namespace

ParsedStmt Parser::parse(const std::string& rawSql) const {
    const std::string sql = stripTrailingSemicolons(rawSql);
    const std::string upper = toUpper(sql);
    if (upper.rfind("CREATE TABLE", 0) == 0) {
        return parseCreate(sql);
    }
    if (upper.rfind("INSERT INTO", 0) == 0) {
        return parseInsert(sql);
    }
    if (upper.rfind("SELECT", 0) == 0) {
        return parseSelect(sql);
    }
    if (upper.rfind("DELETE FROM", 0) == 0) {
        return ParsedStmt{StmtType::UNKNOWN, std::monostate{}, "unsupported statement"};
    }
    return ParsedStmt{StmtType::UNKNOWN, std::monostate{}, "unsupported statement"};
}

ParsedStmt Parser::parseCreate(const std::string& sql) const {
    ParsedStmt parsed;
    parsed.type = StmtType::CREATE_TABLE;

    const size_t open = sql.find('(');
    const size_t close = sql.rfind(')');
    if (open == std::string::npos || close == std::string::npos || close <= open) {
        parsed.error = "invalid CREATE TABLE syntax";
        return parsed;
    }

    std::string head = trim(sql.substr(0, open));
    const std::string upperHead = toUpper(head);
    const std::string prefix = "CREATE TABLE";
    if (upperHead.rfind(prefix, 0) != 0) {
        parsed.error = "invalid CREATE TABLE syntax";
        return parsed;
    }

    CreateTableStmt stmt;
    std::string remainder = trim(head.substr(prefix.size()));
    const std::string ifNotExists = "IF NOT EXISTS";
    if (toUpper(remainder).rfind(ifNotExists, 0) == 0) {
        stmt.ifNotExists = true;
        remainder = trim(remainder.substr(ifNotExists.size()));
    }
    stmt.schema.tableName = remainder;
    if (stmt.schema.tableName.empty()) {
        parsed.error = "missing table name";
        return parsed;
    }

    const std::string colsPart = sql.substr(open + 1, close - open - 1);
    for (const std::string& colSpec : splitCSV(colsPart)) {
        std::istringstream iss(colSpec);
        ColumnDef col;
        std::string typeToken;
        if (!(iss >> col.name >> typeToken)) {
            parsed.error = "invalid column definition";
            return parsed;
        }
        const std::string upperType = toUpper(typeToken);
        auto parsedType = parseType(typeToken);
        if (!parsedType.has_value()) {
            parsed.error = "unsupported column type: " + typeToken;
            return parsed;
        }
        col.type = *parsedType;
        if (upperType.rfind("VARCHAR", 0) == 0) {
            size_t lp = typeToken.find('(');
            size_t rp = typeToken.find(')');
            if (lp != std::string::npos && rp != std::string::npos && rp > lp + 1) {
                col.varcharLen = std::stoi(typeToken.substr(lp + 1, rp - lp - 1));
                if (col.varcharLen <= 0) {
                    parsed.error = "VARCHAR length must be positive";
                    return parsed;
                }
            } else {
                parsed.error = "VARCHAR requires a length";
                return parsed;
            }
        }
        std::string extra;
        std::vector<std::string> extras;
        while (iss >> extra) {
            extras.push_back(toUpper(extra));
        }
        for (size_t i = 0; i < extras.size(); ++i) {
            if (extras[i] == "NOT" && i + 1 < extras.size() && extras[i + 1] == "NULL") {
                col.notNull = true;
                ++i;
            } else if (extras[i] == "PRIMARY" && i + 1 < extras.size() && extras[i + 1] == "KEY") {
                col.primaryKey = true;
                ++i;
            }
        }
        col.index = static_cast<int>(stmt.schema.columns.size());
        if (col.primaryKey) {
            stmt.schema.pkIndex = col.index;
        }
        stmt.schema.columns.push_back(col);
    }

    int expiryIdx = -1;
    for (size_t i = 0; i < stmt.schema.columns.size(); ++i) {
        if (toUpper(stmt.schema.columns[i].name) == "EXPIRES_AT") {
            expiryIdx = static_cast<int>(i);
            const ColType expiryType = stmt.schema.columns[i].type;
            if (expiryType != ColType::INT && expiryType != ColType::DECIMAL &&
                expiryType != ColType::DATETIME) {
                parsed.error = "EXPIRES_AT must be INT, DECIMAL, or DATETIME";
                return parsed;
            }
            break;
        }
    }
    if (expiryIdx < 0) {
        parsed.error = "table schema must include EXPIRES_AT";
        return parsed;
    }

    parsed.stmt = stmt;
    return parsed;
}

ParsedStmt Parser::parseInsert(const std::string& sql) const {
    ParsedStmt parsed;

    const size_t valuesPos = findKeywordCI(sql, "VALUES");
    if (valuesPos == std::string::npos) {
        parsed.error = "missing VALUES clause";
        return parsed;
    }

    std::string table = trim(sql.substr(std::string("INSERT INTO").size(), valuesPos - std::string("INSERT INTO").size()));
    std::string valuesPart = trim(sql.substr(valuesPos + std::string("VALUES").size()));
    if (table.empty() || valuesPart.empty()) {
        parsed.error = "invalid INSERT syntax";
        return parsed;
    }

    std::vector<std::string> groups = splitValueGroups(valuesPart);
    if (groups.empty()) {
        parsed.error = "invalid VALUES groups";
        return parsed;
    }

    if (groups.size() > 1) {
        InsertBatchStmt stmt;
        stmt.table = table;
        for (const std::string& group : groups) {
            stmt.rows.push_back(splitValues(group));
        }
        parsed.type = StmtType::INSERT_BATCH;
        parsed.stmt = stmt;
        return parsed;
    }

    InsertStmt stmt;
    stmt.table = table;
    stmt.values = splitValues(groups.front());
    parsed.type = StmtType::INSERT;
    parsed.stmt = stmt;
    return parsed;
}

ParsedStmt Parser::parseSelect(const std::string& sql) const {
    ParsedStmt parsed;
    parsed.type = StmtType::SELECT;

    const size_t fromPos = findKeywordCI(sql, "FROM");
    if (fromPos == std::string::npos) {
        parsed.error = "missing FROM clause";
        return parsed;
    }

    SelectStmt stmt;
    const std::string colsPart = trim(sql.substr(std::string("SELECT").size(), fromPos - std::string("SELECT").size()));
    stmt.columns = splitCSV(colsPart);

    std::string tail = trim(sql.substr(fromPos + std::string("FROM").size()));
    const std::string tailUpper = toUpper(tail);
    const size_t joinPos = tailUpper.find(" INNER JOIN ");
    const size_t wherePos = tailUpper.find(" WHERE ");
    const size_t orderPos = tailUpper.find(" ORDER BY ");

    size_t tableEnd = tail.size();
    for (size_t pos : {joinPos, wherePos, orderPos}) {
        if (pos != std::string::npos && pos < tableEnd) {
            tableEnd = pos;
        }
    }
    stmt.table = trim(tail.substr(0, tableEnd));
    if (stmt.table.empty()) {
        parsed.error = "missing table name";
        return parsed;
    }

    if (joinPos != std::string::npos) {
        size_t joinStart = joinPos + std::string(" INNER JOIN ").size();
        size_t joinEnd = tail.size();
        for (size_t pos : {wherePos, orderPos}) {
            if (pos != std::string::npos && pos > joinPos && pos < joinEnd) {
                joinEnd = pos;
            }
        }
        std::string joinPart = trim(tail.substr(joinStart, joinEnd - joinStart));
        size_t onPos = findKeywordCI(joinPart, "ON");
        if (onPos == std::string::npos) {
            parsed.error = "missing ON clause";
            return parsed;
        }
        JoinClause join;
        join.table2 = trim(joinPart.substr(0, onPos));
        std::string cond = trim(joinPart.substr(onPos + std::string("ON").size()));
        size_t eqPos = cond.find('=');
        if (eqPos == std::string::npos) {
            parsed.error = "invalid JOIN condition";
            return parsed;
        }
        join.col1 = trim(cond.substr(0, eqPos));
        join.col2 = trim(cond.substr(eqPos + 1));
        stmt.join = join;
    }

    if (wherePos != std::string::npos) {
        size_t whereStart = wherePos + std::string(" WHERE ").size();
        size_t whereEnd = (orderPos != std::string::npos && orderPos > wherePos) ? orderPos : tail.size();
        WhereClause where;
        if (!parseWhere(tail.substr(whereStart, whereEnd - whereStart), where)) {
            parsed.error = "invalid WHERE clause";
            return parsed;
        }
        stmt.where = where;
    }

    if (orderPos != std::string::npos) {
        parsed.error = "ORDER BY is not supported";
        return parsed;
    }

    parsed.stmt = stmt;
    return parsed;
}

ParsedStmt Parser::parseDelete(const std::string& sql) const {
    ParsedStmt parsed;
    parsed.type = StmtType::DELETE;

    std::string tail = trim(sql.substr(std::string("DELETE FROM").size()));
    const std::string tailUpper = toUpper(tail);
    const size_t wherePos = tailUpper.find(" WHERE ");

    DeleteStmt stmt;
    if (wherePos == std::string::npos) {
        stmt.table = trim(tail);
    } else {
        stmt.table = trim(tail.substr(0, wherePos));
        WhereClause where;
        if (!parseWhere(tail.substr(wherePos + std::string(" WHERE ").size()), where)) {
            parsed.error = "invalid WHERE clause";
            return parsed;
        }
        stmt.where = where;
    }

    if (stmt.table.empty()) {
        parsed.error = "missing table name";
        return parsed;
    }

    parsed.stmt = stmt;
    return parsed;
}
