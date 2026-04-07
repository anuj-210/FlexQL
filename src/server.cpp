#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cctype>
#include <cstring>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

#include "protocol.h"


namespace {

void appendU32(std::vector<char>& out, uint32_t value) {
    uint32_t net = htonl(value);
    const char* p = reinterpret_cast<const char*>(&net);
    out.insert(out.end(), p, p + sizeof(net));
}

void appendString(std::vector<char>& out, const std::string& s) {
    appendU32(out, static_cast<uint32_t>(s.size()));
    out.insert(out.end(), s.begin(), s.end());
}

void appendStringView(std::vector<char>& out, std::string_view s) {
    appendU32(out, static_cast<uint32_t>(s.size()));
    out.insert(out.end(), s.begin(), s.end());
}

const std::vector<char>& emptyOkResponse() {
    static const std::vector<char> resp = [] {
        std::vector<char> out;
        out.reserve(sizeof(uint32_t) * 5);
        appendU32(out, PROTO_RESP_MAGIC);
        appendU32(out, 0u);
        appendU32(out, 0u);
        appendU32(out, 0u);
        appendU32(out, 0u);
        return out;
    }();
    return resp;
}

bool looksLikeSelectSql(const std::string& sql) {
    size_t i = 0;
    while (i < sql.size() && std::isspace(static_cast<unsigned char>(sql[i]))) {
        ++i;
    }
    static const char* kSelect = "SELECT";
    for (size_t j = 0; kSelect[j] != '\0'; ++j) {
        if (i + j >= sql.size()) {
            return false;
        }
        if (std::toupper(static_cast<unsigned char>(sql[i + j])) != kSelect[j]) {
            return false;
        }
    }
    return true;
}

}  // namespace

Server::Server(int port, int threadPoolSize)
    : port_(port),
      threadPoolSize_(threadPoolSize == 0 ? static_cast<int>(std::thread::hardware_concurrency()) : threadPoolSize) {}

bool Server::writeAll(int fd, const void* data, size_t len) {
    const char* ptr = static_cast<const char*>(data);
    size_t total = 0;
    while (total < len) {
        ssize_t n = ::write(fd, ptr + total, len - total);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return false;
        }
        total += static_cast<size_t>(n);
    }
    return true;
}

bool Server::readAll(int fd, void* data, size_t len) {
    char* ptr = static_cast<char*>(data);
    size_t total = 0;
    while (total < len) {
        ssize_t n = ::read(fd, ptr + total, len - total);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return false;
        }
        total += static_cast<size_t>(n);
    }
    return true;
}

bool Server::run() {
    listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) {
        std::cerr << "failed to create socket\n";
        return false;
    }

    int one = 1;
    ::setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port_));

    if (::bind(listenFd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "failed to bind port " << port_ << ": " << std::strerror(errno) << "\n";
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }
    if (::listen(listenFd_, 128) != 0) {
        std::cerr << "failed to listen on port " << port_ << ": " << std::strerror(errno) << "\n";
        ::close(listenFd_);
        listenFd_ = -1;
        return false;
    }

    running_.store(true);
    workers_.clear();
    workers_.reserve(static_cast<size_t>(std::max(1, threadPoolSize_)));
    for (int i = 0; i < std::max(1, threadPoolSize_); ++i) {
        workers_.emplace_back(&Server::workerLoop, this);
    }
    acceptLoop();
    queueCv_.notify_all();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    return true;
}

void Server::stop() {
    running_.store(false);
    if (listenFd_ >= 0) {
        ::close(listenFd_);
        listenFd_ = -1;
    }
    queueCv_.notify_all();
}

void Server::acceptLoop() {
    while (running_.load()) {
        int fd = ::accept(listenFd_, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (running_.load()) {
                continue;
            }
            break;
        }
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        {
            std::lock_guard<std::mutex> lock(queueMtx_);
            clientQueue_.push(fd);
        }
        queueCv_.notify_one();
    }
}

void Server::workerLoop() {
    while (true) {
        int fd = -1;
        {
            std::unique_lock<std::mutex> lock(queueMtx_);
            queueCv_.wait(lock, [&] { return !running_.load() || !clientQueue_.empty(); });
            if (clientQueue_.empty()) {
                if (!running_.load()) {
                    return;
                }
                continue;
            }
            fd = clientQueue_.front();
            clientQueue_.pop();
        }
        if (fd >= 0) {
            handleClient(fd);
        }
    }
}

void Server::handleClient(int fd) {
    Executor exec(engine_, cache_);
    std::vector<char> resp;

    while (true) {
        uint32_t reqHeader[2] = {};
        if (!readAll(fd, reqHeader, sizeof(reqHeader))) {
            break;
        }

        const uint32_t magic = ntohl(reqHeader[0]);
        const uint32_t sqlLen = ntohl(reqHeader[1]);
        if (magic != PROTO_REQ_MAGIC) {
            break;
        }

        std::string sql(sqlLen, '\0');
        if (sqlLen > 0 && !readAll(fd, sql.data(), sqlLen)) {
            break;
        }

        auto sendErrorResponse = [&](const std::string& error) -> bool {
            resp.clear();
            resp.reserve(sizeof(uint32_t) * 5 + error.size());
            appendU32(resp, PROTO_RESP_MAGIC);
            appendU32(resp, 1u);
            appendU32(resp, 0u);
            appendU32(resp, 0u);
            appendString(resp, error);
            return writeAll(fd, resp.data(), resp.size());
        };

        if (looksLikeSelectSql(sql)) {
            constexpr size_t kRowChunkBytes = 1u << 20;
            bool headerWritten = false;
            bool writeFailed = false;
            std::vector<char> chunk;
            chunk.reserve(kRowChunkBytes);

            auto flushChunk = [&]() -> bool {
                if (chunk.empty()) {
                    return true;
                }
                if (!writeAll(fd, chunk.data(), chunk.size())) {
                    writeFailed = true;
                    return false;
                }
                chunk.clear();
                return true;
            };

            ExecResult result = exec.streamSelect(
                sql,
                [&](const std::vector<std::string>& colNames, uint32_t nrows) -> bool {
                    resp.clear();
                    size_t headerBytes = sizeof(uint32_t) * 4;
                    for (const std::string& name : colNames) {
                        headerBytes += sizeof(uint32_t) + name.size();
                    }
                    resp.reserve(headerBytes);
                    appendU32(resp, PROTO_RESP_MAGIC);
                    appendU32(resp, 0u);
                    appendU32(resp, nrows);
                    appendU32(resp, static_cast<uint32_t>(colNames.size()));
                    for (const std::string& name : colNames) {
                        appendString(resp, name);
                    }
                    if (!writeAll(fd, resp.data(), resp.size())) {
                        writeFailed = true;
                        return false;
                    }
                    headerWritten = true;
                    return true;
                },
                [&](int colCount, const std::vector<std::string_view>& values, const std::vector<std::string>&) -> int {
                    for (int i = 0; i < colCount; ++i) {
                        appendStringView(chunk, values[static_cast<size_t>(i)]);
                    }
                    if (chunk.size() >= kRowChunkBytes && !flushChunk()) {
                        return 1;
                    }
                    return 0;
                });

            if (!result.ok) {
                if (writeFailed) {
                    break;
                }
                if (headerWritten) {
                    break;
                }
                if (!sendErrorResponse(result.error)) {
                    break;
                }
                continue;
            }

            if (writeFailed) {
                break;
            }
            if (!headerWritten) {
                if (!sendErrorResponse("failed to stream SELECT response")) {
                    break;
                }
                continue;
            }
            appendString(chunk, result.error);
            if (!flushChunk()) {
                break;
            }
            continue;
        }

        std::vector<std::string> colNames;
        std::vector<std::vector<std::string>> rows;
        ExecResult result = exec.execute(sql, [&](int colCount, std::vector<std::string>& values, std::vector<std::string>& names) {
            if (colNames.empty() && !names.empty()) {
                colNames = names;
            }
            if (colCount == 0) {
                rows.emplace_back();
            } else {
                rows.push_back(values);
            }
            return 0;
        });

        if (result.ok && rows.empty() && colNames.empty()) {
            const std::vector<char>& r = emptyOkResponse();
            if (!writeAll(fd, r.data(), r.size())) {
                break;
            }
            continue;
        }

        size_t respBytes = sizeof(uint32_t) * 4 + result.error.size() + sizeof(uint32_t);
        for (const std::string& name : colNames) {
            respBytes += sizeof(uint32_t) + name.size();
        }
        for (const auto& row : rows) {
            for (const std::string& value : row) {
                respBytes += sizeof(uint32_t) + value.size();
            }
        }

        resp.clear();
        resp.reserve(respBytes);
        appendU32(resp, PROTO_RESP_MAGIC);
        appendU32(resp, result.ok ? 0u : 1u);
        appendU32(resp, static_cast<uint32_t>(rows.size()));
        appendU32(resp, static_cast<uint32_t>(colNames.size()));
        for (const std::string& name : colNames) {
            appendString(resp, name);
        }
        for (const auto& row : rows) {
            for (const std::string& value : row) {
                appendString(resp, value);
            }
        }
        appendString(resp, result.error);

        if (!writeAll(fd, resp.data(), resp.size())) {
            break;
        }
    }

    ::close(fd);
}
