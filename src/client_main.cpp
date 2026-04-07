#include <iostream>
#include <string>
#include <vector>

#include "flexql.h"

int main(int argc, char** argv) {
    const char* host = "127.0.0.1";
    int port = 9000;
    if (argc > 1) {
        host = argv[1];
    }
    if (argc > 2) {
        port = std::stoi(argv[2]);
    }

    FlexQL* db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "failed to connect\n";
        return 1;
    }

    std::string line;
    while (true) {
        std::cout << "flexql> " << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line == ".exit" || line == "quit" || line == "exit") {
            break;
        }

        char* err = nullptr;
        int rc = flexql_exec(
            db,
            line.c_str(),
            [](void*, int argc, char** argv, char** colNames) {
                for (int i = 0; i < argc; ++i) {
                    if (i > 0) {
                        std::cout << " | ";
                    }
                    std::cout << colNames[i] << "=" << (argv[i] ? argv[i] : "NULL");
                }
                std::cout << "\n";
                return 0;
            },
            nullptr,
            &err);

        if (rc != FLEXQL_OK) {
            std::cerr << "error: " << (err ? err : "unknown error") << "\n";
            if (err) {
                flexql_free(err);
            }
        }
    }

    flexql_close(db);
    return 0;
}
