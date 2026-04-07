#include <cstdlib>
#include <iostream>
#include <csignal>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "server.h"

namespace {

Server* g_server = nullptr;

void handleSignal(int) {
    if (g_server) {
        g_server->stop();
    }
}

}  // namespace

int main(int argc, char** argv) {
    int port = 9000;
    if (argc > 1) {
        port = std::atoi(argv[1]);
        if (port <= 0) {
            std::cerr << "invalid port\n";
            return 1;
        }
    }

    Server server(port);
    g_server = &server;

#ifdef __linux__
    ::prctl(PR_SET_PDEATHSIG, SIGTERM);
#endif

    std::signal(SIGINT, handleSignal);
    std::signal(SIGTERM, handleSignal);
    std::signal(SIGHUP, handleSignal);

    if (!server.run()) {
        return 1;
    }
    return 0;
}
