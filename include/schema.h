#pragma once

#include <cstdint>
#include <string>
#include <vector>

enum class ColType : uint8_t { INT, DECIMAL, VARCHAR, DATETIME, TEXT };

struct ColumnDef {
    std::string name;
    ColType type;
    bool notNull = false;
    bool primaryKey = false;
    int varcharLen = 0;
    int index = -1;
};

struct TableSchema {
    std::string tableName;
    std::vector<ColumnDef> columns;
    int pkIndex = -1;

    int colIndex(const std::string& name) const;
};
