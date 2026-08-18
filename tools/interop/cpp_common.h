// Shared helpers for the C++ interop peers. Kept tiny on purpose: these
// peers exist to be the reference implementation on the other end of the
// wire, so the less machinery of our own they carry the better.
#ifndef INTEROP_CPP_COMMON_H
#define INTEROP_CPP_COMMON_H

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>

#include <string>
#include <vector>
#include <iostream>

namespace interop_util {

inline grpc::ByteBuffer to_buffer(const std::string &data) {
    grpc::Slice slice(data.data(), data.size());
    return grpc::ByteBuffer(&slice, 1);
}

inline std::string from_buffer(const grpc::ByteBuffer &buf) {
    std::vector<grpc::Slice> slices;
    if (!buf.Dump(&slices).ok()) return std::string();
    std::string out;
    for (const auto &s : slices) {
        out.append(reinterpret_cast<const char *>(s.begin()), s.size());
    }
    return out;
}

// Results are printed rather than asserted so the shell driver can grep
// them, and every check names itself: a peer that dies halfway through
// must not look like a peer that passed.
struct Checker {
    int failures = 0;
    template <class T>
    void check(const std::string &name, const T &got, const T &want) {
        const bool ok = (got == want);
        if (!ok) ++failures;
        std::cout << "CPP " << (ok ? "ok" : "FAIL") << " " << name
                  << " got=" << got << " want=" << want << std::endl;
    }
    int finish(const char *who) {
        if (failures) {
            std::cout << "CPP FAILED: " << failures << " check(s) in " << who
                      << std::endl;
            return 1;
        }
        std::cout << "CPP all ok" << std::endl;
        return 0;
    }
};

}  // namespace interop_util

#endif
