#include "flexql.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <string>
#include <vector>

#include "protocol.h"

struct FlexQL {
    int fd = -1;
};

namespace {

struct RxBuf {
    int fd = -1;
    std::vector<char> buf;
    size_t pos = 0;
    size_t end = 0;
};

bool rxRead(RxBuf& rx, void* data, size_t len) {
    char* dst = static_cast<char*>(data);
    while (len > 0) {
        if (rx.pos >= rx.end) {
            ssize_t n = ::read(rx.fd, rx.buf.data(), rx.buf.size());
            if (n < 0 && errno == EINTR) {
                continue;
            }
            if (n <= 0) {
                return false;
            }
            rx.pos = 0;
            rx.end = static_cast<size_t>(n);
        }

        const size_t avail = rx.end - rx.pos;
        const size_t copy = std::min(len, avail);
        std::memcpy(dst, rx.buf.data() + rx.pos, copy);
        dst += copy;
        rx.pos += copy;
        len -= copy;
    }
    return true;
}

bool rxU32(RxBuf& rx, uint32_t& out) {
    uint32_t net = 0;
    if (!rxRead(rx, &net, sizeof(net))) {
        return false;
    }
    out = ntohl(net);
    return true;
}

bool rxStringInto(RxBuf& rx, std::string& out) {
    uint32_t len = 0;
    if (!rxU32(rx, len)) {
        return false;
    }
    out.resize(len);
    if (len > 0 && !rxRead(rx, out.data(), len)) {
        return false;
    }
    return true;
}

bool writeAllVec(int fd, iovec* iov, int iovcnt) {
    int current = 0;
    while (current < iovcnt) {
        ssize_t n = ::writev(fd, iov + current, iovcnt - current);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            return false;
        }
        ssize_t remaining = n;
        while (current < iovcnt && remaining >= static_cast<ssize_t>(iov[current].iov_len)) {
            remaining -= static_cast<ssize_t>(iov[current].iov_len);
            ++current;
        }
        if (current < iovcnt && remaining > 0) {
            iov[current].iov_base = static_cast<char*>(iov[current].iov_base) + remaining;
            iov[current].iov_len -= static_cast<size_t>(remaining);
        }
    }
    return true;
}

void setErr(char** errmsg, const std::string& msg) {
    if (errmsg) {
        *errmsg = ::strdup(msg.c_str());
    }
}

}  // namespace

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) {
        return FLEXQL_ERROR;
    }
    *db = nullptr;

    int fd = -1;
    constexpr int kConnectRetries = 40;
    in_addr ipv4Addr{};
    if (::inet_pton(AF_INET, host, &ipv4Addr) == 1) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        addr.sin_addr = ipv4Addr;
        for (int attempt = 0; attempt < kConnectRetries && fd < 0; ++attempt) {
            fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd >= 0) {
                int one = 1;
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
                    break;
                }
                ::close(fd);
                fd = -1;
            }
            if (fd < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    } else {
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        addrinfo* res = nullptr;
        const std::string portStr = std::to_string(port);
        if (::getaddrinfo(host, portStr.c_str(), &hints, &res) != 0) {
            return FLEXQL_ERROR;
        }

        for (int attempt = 0; attempt < kConnectRetries && fd < 0; ++attempt) {
            for (addrinfo* cur = res; cur != nullptr; cur = cur->ai_next) {
                fd = ::socket(cur->ai_family, cur->ai_socktype, cur->ai_protocol);
                if (fd < 0) {
                    continue;
                }
                int one = 1;
                ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                if (::connect(fd, cur->ai_addr, cur->ai_addrlen) == 0) {
                    break;
                }
                ::close(fd);
                fd = -1;
            }
            if (fd < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }

        ::freeaddrinfo(res);
    }

    if (fd < 0) {
        return FLEXQL_ERROR;
    }

    FlexQL* handle = new FlexQL();
    handle->fd = fd;
    *db = handle;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) {
        return FLEXQL_ERROR;
    }
    if (db->fd >= 0) {
        ::close(db->fd);
    }
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(FlexQL *db,
                const char *sql,
                int (*callback)(void*, int, char**, char**),
                void *arg,
                char **errmsg) {
    if (errmsg) {
        *errmsg = nullptr;
    }
    if (!db || db->fd < 0 || !sql) {
        setErr(errmsg, "invalid database handle or sql");
        return FLEXQL_ERROR;
    }

    const uint32_t sqlLen = static_cast<uint32_t>(std::strlen(sql));
    uint32_t magic = htonl(PROTO_REQ_MAGIC);
    uint32_t lenNet = htonl(sqlLen);
    iovec iov[3];
    iov[0].iov_base = &magic;
    iov[0].iov_len = sizeof(magic);
    iov[1].iov_base = &lenNet;
    iov[1].iov_len = sizeof(lenNet);
    iov[2].iov_base = const_cast<char*>(sql);
    iov[2].iov_len = sqlLen;

    if (!writeAllVec(db->fd, iov, 3)) {
        setErr(errmsg, "failed to send request");
        return FLEXQL_ERROR;
    }

    RxBuf rx;
    rx.fd = db->fd;
    rx.buf.resize(1u << 16);

    uint32_t respMagic = 0;
    uint32_t status = 0;
    uint32_t nrows = 0;
    uint32_t ncols = 0;
    if (!rxU32(rx, respMagic) || !rxU32(rx, status) || !rxU32(rx, nrows) || !rxU32(rx, ncols)) {
        setErr(errmsg, "failed to read response header");
        return FLEXQL_ERROR;
    }

    if (respMagic != PROTO_RESP_MAGIC) {
        setErr(errmsg, "invalid response magic");
        return FLEXQL_ERROR;
    }

    std::vector<std::string> colNames(ncols);
    std::vector<char*> namePtrs(ncols);
    for (uint32_t i = 0; i < ncols; ++i) {
        if (!rxStringInto(rx, colNames[i])) {
            setErr(errmsg, "failed to read column name");
            return FLEXQL_ERROR;
        }
        namePtrs[i] = colNames[i].empty() ? const_cast<char*>("") : colNames[i].data();
    }

    std::vector<std::string> values(ncols);
    std::vector<char*> valuePtrs(ncols);
    for (uint32_t r = 0; r < nrows; ++r) {
        for (uint32_t c = 0; c < ncols; ++c) {
            if (!rxStringInto(rx, values[c])) {
                setErr(errmsg, "failed to read value");
                return FLEXQL_ERROR;
            }
            valuePtrs[c] = values[c].empty() ? const_cast<char*>("") : values[c].data();
        }

        if (callback) {
            int cbRc = callback(arg,
                                static_cast<int>(ncols),
                                ncols == 0 ? nullptr : valuePtrs.data(),
                                ncols == 0 ? nullptr : namePtrs.data());
            if (cbRc == 1) {
                for (uint32_t rr = r + 1; rr < nrows; ++rr) {
                    for (uint32_t cc = 0; cc < ncols; ++cc) {
                        uint32_t discardLen = 0;
                        if (!rxU32(rx, discardLen)) {
                            setErr(errmsg, "failed while discarding rows");
                            return FLEXQL_ERROR;
                        }
                        std::string discard(discardLen, '\0');
                        if (discardLen > 0 && !rxRead(rx, discard.data(), discardLen)) {
                            setErr(errmsg, "failed while discarding row data");
                            return FLEXQL_ERROR;
                        }
                    }
                }
                break;
            }
        }
    }

    uint32_t errLen = 0;
    if (!rxU32(rx, errLen)) {
        setErr(errmsg, "failed to read error length");
        return FLEXQL_ERROR;
    }
    std::string err(errLen, '\0');
    if (errLen > 0 && !rxRead(rx, err.data(), errLen)) {
        setErr(errmsg, "failed to read error message");
        return FLEXQL_ERROR;
    }

    if (status != 0) {
        setErr(errmsg, err.empty() ? "server error" : err);
        return FLEXQL_ERROR;
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    std::free(ptr);
}
