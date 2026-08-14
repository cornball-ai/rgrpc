// Shared conversion helpers for client.cpp and server.cpp.
//
// The *_to_r converters allocate R objects and may only run on the R
// main thread. grpc_byte_buffer_to_string is plain C++ and is safe on
// completion threads.

#ifndef GRPC_R_COMMON_H
#define GRPC_R_COMMON_H

#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/slice.h>
#include <grpcpp/support/string_ref.h>

#ifndef R_NO_REMAP
#define R_NO_REMAP
#endif
#include <R.h>
#include <Rinternals.h>

inline SEXP grpc_metadata_to_r(
    const std::multimap<grpc::string_ref, grpc::string_ref> &md) {
    const R_xlen_t n = static_cast<R_xlen_t>(md.size());
    SEXP values = PROTECT(Rf_allocVector(STRSXP, n));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n));
    R_xlen_t i = 0;
    for (const auto &kv : md) {
        SET_STRING_ELT(names, i, Rf_mkCharLen(kv.first.data(), (int) kv.first.size()));
        SET_STRING_ELT(values, i, Rf_mkCharLen(kv.second.data(), (int) kv.second.size()));
        ++i;
    }
    Rf_setAttrib(values, R_NamesSymbol, names);
    UNPROTECT(2);
    return values;
}

inline SEXP grpc_pairs_to_r(
    const std::vector<std::pair<std::string, std::string>> &md) {
    const R_xlen_t n = static_cast<R_xlen_t>(md.size());
    SEXP values = PROTECT(Rf_allocVector(STRSXP, n));
    SEXP names = PROTECT(Rf_allocVector(STRSXP, n));
    for (R_xlen_t i = 0; i < n; ++i) {
        SET_STRING_ELT(names, i, Rf_mkCharLen(md[i].first.data(), (int) md[i].first.size()));
        SET_STRING_ELT(values, i, Rf_mkCharLen(md[i].second.data(), (int) md[i].second.size()));
    }
    Rf_setAttrib(values, R_NamesSymbol, names);
    UNPROTECT(2);
    return values;
}

inline bool grpc_byte_buffer_to_string(const grpc::ByteBuffer &bb, std::string *out) {
    std::vector<grpc::Slice> slices;
    if (!bb.Dump(&slices).ok()) return false;
    size_t total = 0;
    for (const auto &s : slices) total += s.size();
    out->clear();
    out->reserve(total);
    for (const auto &s : slices)
        out->append(reinterpret_cast<const char *>(s.begin()), s.size());
    return true;
}

inline SEXP grpc_byte_buffer_to_raw(const grpc::ByteBuffer &bb) {
    std::vector<grpc::Slice> slices;
    if (!bb.Dump(&slices).ok()) return R_NilValue;
    size_t total = 0;
    for (const auto &s : slices) total += s.size();
    SEXP out = Rf_allocVector(RAWSXP, (R_xlen_t) total);
    unsigned char *p = RAW(out);
    for (const auto &s : slices) {
        std::memcpy(p, s.begin(), s.size());
        p += s.size();
    }
    return out;
}

#endif  // GRPC_R_COMMON_H
