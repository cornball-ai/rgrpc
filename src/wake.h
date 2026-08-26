// Portable wake handle for the R-facing event queues.
//
// Contract, shared by client and server: one signal per queued event, a
// full drain under the queue lock, and a re-signal whenever the queue
// is left non-empty, so the handle is readable exactly while events are
// queued. Under that protocol readability is boolean -- the drain
// always empties the handle and the re-arm restores it -- so nothing
// depends on how many signals a single read absorbs. That is what lets
// one contract cover three implementations: eventfd counts, a pipe
// accumulates bytes until full, a socket pair batches them, and all
// three answer the only question asked, "did anything arrive".
//
// Linux and macOS use a self-pipe (eventfd exists only on Linux).
// Windows uses a loopback TCP socket pair, because the handle is also
// handed to R for external event loops and a pollable descriptor on
// Windows must be a socket.

#ifndef GRPC_R_WAKE_H
#define GRPC_R_WAKE_H

#ifdef _WIN32
#include <cstring>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#endif

struct wake_handle {
#ifdef _WIN32
    SOCKET r = INVALID_SOCKET;  // drained and polled; exposed to R
    SOCKET w = INVALID_SOCKET;  // signaled by the completion thread
#else
    int r = -1;
    int w = -1;
#endif
};

#ifndef _WIN32

inline bool wake_open(wake_handle *h) {
    int fds[2];
    if (pipe(fds) != 0) return false;
    for (int i = 0; i < 2; ++i) {
        fcntl(fds[i], F_SETFL, fcntl(fds[i], F_GETFL) | O_NONBLOCK);
        fcntl(fds[i], F_SETFD, fcntl(fds[i], F_GETFD) | FD_CLOEXEC);
    }
    h->r = fds[0];
    h->w = fds[1];
    return true;
}

inline void wake_signal(const wake_handle &h) {
    char one = 1;
    ssize_t n = write(h.w, &one, 1);
    (void) n;  // EAGAIN means the pipe is full, hence already readable
}

inline void wake_drain(const wake_handle &h) {
    char buf[256];
    while (read(h.r, buf, sizeof buf) > 0) {
    }
}

// poll() semantics: >0 readable, 0 timed out, <0 interrupted or error.
// timeout_ms < 0 waits without a deadline.
inline int wake_poll(const wake_handle &h, int timeout_ms) {
    struct pollfd pfd = {h.r, POLLIN, 0};
    return poll(&pfd, 1, timeout_ms);
}

inline void wake_close(wake_handle *h) {
    if (h->r >= 0) close(h->r);
    if (h->w >= 0) close(h->w);
    h->r = h->w = -1;
}

inline int wake_fd(const wake_handle &h) { return h.r; }

#else  // _WIN32

inline bool wake_open(wake_handle *h) {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return false;
    SOCKET listener = socket(AF_INET, SOCK_STREAM, 0);
    SOCKET w = INVALID_SOCKET;
    SOCKET r = INVALID_SOCKET;
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    int len = sizeof addr;
    const bool ok =
        listener != INVALID_SOCKET &&
        bind(listener, (sockaddr *) &addr, sizeof addr) == 0 &&
        listen(listener, 1) == 0 &&
        getsockname(listener, (sockaddr *) &addr, &len) == 0 &&
        (w = socket(AF_INET, SOCK_STREAM, 0)) != INVALID_SOCKET &&
        connect(w, (sockaddr *) &addr, sizeof addr) == 0 &&
        (r = accept(listener, nullptr, nullptr)) != INVALID_SOCKET;
    if (listener != INVALID_SOCKET) closesocket(listener);
    if (!ok) {
        if (w != INVALID_SOCKET) closesocket(w);
        WSACleanup();
        return false;
    }
    u_long nb = 1;
    ioctlsocket(r, FIONBIO, &nb);
    nb = 1;
    ioctlsocket(w, FIONBIO, &nb);
    BOOL nd = TRUE;
    setsockopt(w, IPPROTO_TCP, TCP_NODELAY, (const char *) &nd, sizeof nd);
    SetHandleInformation((HANDLE) r, HANDLE_FLAG_INHERIT, 0);
    SetHandleInformation((HANDLE) w, HANDLE_FLAG_INHERIT, 0);
    h->r = r;
    h->w = w;
    return true;
}

inline void wake_signal(const wake_handle &h) {
    char one = 1;
    send(h.w, &one, 1, 0);  // WSAEWOULDBLOCK means already readable
}

inline void wake_drain(const wake_handle &h) {
    char buf[256];
    while (recv(h.r, buf, sizeof buf, 0) > 0) {
    }
}

inline int wake_poll(const wake_handle &h, int timeout_ms) {
    WSAPOLLFD pfd = {h.r, POLLRDNORM, 0};
    return WSAPoll(&pfd, 1, timeout_ms);
}

inline void wake_close(wake_handle *h) {
    if (h->r == INVALID_SOCKET && h->w == INVALID_SOCKET) return;
    if (h->r != INVALID_SOCKET) closesocket(h->r);
    if (h->w != INVALID_SOCKET) closesocket(h->w);
    h->r = h->w = INVALID_SOCKET;
    WSACleanup();
}

inline int wake_fd(const wake_handle &h) { return (int) h.r; }

#endif  // _WIN32

#endif  // GRPC_R_WAKE_H
