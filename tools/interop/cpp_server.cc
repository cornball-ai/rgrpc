// C++ interop peer, server side. Driven by tools/interop/interop.sh.
//
// AsyncGenericService, for the same reason cpp_client.cc uses
// GenericStub: libgrpc++-dev ships no grpc_cpp_plugin, and the generic
// path is the same core a generated service wraps. Payloads come from
// protoc-generated message classes, so proto encoding is real.
//
// Calls are served one at a time. The R client under test issues three
// sequential RPCs, so a concurrent dispatcher would add machinery
// without adding coverage.
#include "cpp_common.h"
#include "interop.pb.h"

#include <grpcpp/generic/async_generic_service.h>

#include <memory>
#include <string>

using interop_util::to_buffer;
using interop_util::from_buffer;

namespace {

bool next_is(grpc::ServerCompletionQueue *cq, void *expect) {
    void *got = nullptr;
    bool ok = false;
    if (!cq->Next(&got, &ok)) return false;
    return ok && got == expect;
}

bool ends_with(const std::string &s, const std::string &suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "usage: cpp_server <addr>" << std::endl;
        return 2;
    }
    const std::string addr = argv[1];

    grpc::AsyncGenericService service;
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterAsyncGenericService(&service);
    auto cq = builder.AddCompletionQueue();
    auto server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "failed to bind " << addr << std::endl;
        return 1;
    }
    std::cout << "ready" << std::endl;

    void *const kAccept = reinterpret_cast<void *>(1);
    void *const kOp = reinterpret_cast<void *>(2);

    for (;;) {
        grpc::GenericServerContext ctx;
        grpc::GenericServerAsyncReaderWriter stream(&ctx);
        service.RequestCall(&ctx, &stream, cq.get(), cq.get(), kAccept);
        if (!next_is(cq.get(), kAccept)) break;

        const std::string method = ctx.method();
        grpc::ByteBuffer in;
        stream.Read(&in, kOp);
        if (!next_is(cq.get(), kOp)) continue;

        interop::v1::EchoRequest req;
        req.ParseFromString(from_buffer(in));

        if (ends_with(method, "/Echo")) {
            interop::v1::EchoReply reply;
            reply.set_text(req.text());
            *reply.mutable_items() = req.items();
            *reply.mutable_labels() = req.labels();
            reply.set_responder("cpp");
            std::string wire;
            reply.SerializeToString(&wire);
            grpc::ByteBuffer out = to_buffer(wire);
            stream.WriteAndFinish(out, grpc::WriteOptions(), grpc::Status::OK,
                                  kOp);
            next_is(cq.get(), kOp);
        } else if (ends_with(method, "/EchoStream")) {
            bool ok = true;
            for (int i = 0; ok && i < req.number(); ++i) {
                interop::v1::EchoReply reply;
                reply.set_text(req.text() + "-" + std::to_string(i));
                reply.set_responder("cpp");
                std::string wire;
                reply.SerializeToString(&wire);
                grpc::ByteBuffer out = to_buffer(wire);
                stream.Write(out, kOp);
                ok = next_is(cq.get(), kOp);
            }
            stream.Finish(grpc::Status::OK, kOp);
            next_is(cq.get(), kOp);
        } else if (ends_with(method, "/Fail")) {
            stream.Finish(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                       "interop failure"),
                          kOp);
            next_is(cq.get(), kOp);
        } else {
            stream.Finish(grpc::Status(grpc::StatusCode::UNIMPLEMENTED, method),
                          kOp);
            next_is(cq.get(), kOp);
        }
    }

    server->Shutdown();
    cq->Shutdown();
    void *drain = nullptr;
    bool dok = false;
    while (cq->Next(&drain, &dok)) {}
    return 0;
}
