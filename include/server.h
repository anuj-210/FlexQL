#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "executor.h"

class Server {
    int port_;
    int listenFd_ = -1;
    int threadPoolSize_;
    std::atomic<bool> running_{false};
    StorageEngine engine_;
    LRUCache cache_{4096};
    std::mutex queueMtx_;
    std::condition_variable queueCv_;
    std::queue<int> clientQueue_;
    std::vector<std::thread> workers_;

public:
    Server(int port, int threadPoolSize = 0);
    bool run();
    void stop();
    static bool writeAll(int fd, const void* data, size_t len);
    static bool readAll(int fd, void* data, size_t len);

private:
    void acceptLoop();
    void workerLoop();
    void handleClient(int fd);
};
