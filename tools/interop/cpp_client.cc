// C++ interop peer, client side. Driven by tools/interop/interop.sh.
//
// Uses the async generic API (GenericStub + CompletionQueue) rather than
// a codegen'd stub, because libgrpc++-dev ships no grpc_cpp_plugin. That
// changes nothing on the wire -- a generated stub is a wrapper over this
// same core -- and the protobuf payloads still come from protoc-generated
// message classes, so proto encoding is exercised for real.
#include "cpp_common.h"
#include "interop.pb.h"

#include <grpcpp/generic/generic_stub.h>

#include <memory>
#include <string>

using interop_util::to_buffer;
using interop_util::from_buffer;

namespace {

interop::v1::EchoRequest make_request() {
    interop::v1::EchoRequest req;
    req.set_text("hello");
    auto *a = req.add_items();
    a->set_name("a");
    a->set_count(1);
    auto *b = req.add_items();
    b->set_name("b");
    b->set_count(2);
    (*req.mutable_labels())["k"] = "v";
    req.set_number(3);
    return req;
}

// Block on one completion and return whether it succeeded. Every call
// below is the only thing outstanding on its queue, so matching tags is
// unnecessary; asserting the tag anyway keeps a silent mismatch from
// reading as success.
bool await_tag(grpc::CompletionQueue *cq, void *expect) {
    void *got = nullptr;
    bool ok = false;
    if (!cq->Next(&got, &ok)) return false;
    return ok && got == expect;
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 2) {
        std::cerr << "usage: cpp_client <target>" << std::endl;
        return 2;
    }
    const std::string target = argv[1];
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    grpc::GenericStub stub(channel);
    interop_util::Checker c;

    const interop::v1::EchoRequest req = make_request();
    std::string wire;
    req.SerializeToString(&wire);

    // ---- unary ----
    {
        grpc::CompletionQueue cq;
        grpc::ClientContext ctx;
        grpc::ByteBuffer send = to_buffer(wire);
        auto reader = stub.PrepareUnaryCall(
            &ctx, "/interop.v1.Interop/Echo", send, &cq);
        reader->StartCall();
        grpc::ByteBuffer recv;
        grpc::Status status;
        void *tag = reinterpret_cast<void *>(1);
        reader->Finish(&recv, &status, tag);
        const bool done = await_tag(&cq, tag);
        c.check<std::string>("unary.completed", done ? "yes" : "no", "yes");
        c.check<int>("unary.status", status.error_code(), grpc::StatusCode::OK);
        interop::v1::EchoReply reply;
        reply.ParseFromString(from_buffer(recv));
        c.check<std::string>("unary.text", reply.text(), "hello");
        c.check<std::string>("unary.responder", reply.responder(), "r");
        c.check<int>("unary.items", reply.items_size(), 2);
        if (reply.items_size() == 2) {
            c.check<std::string>("unary.item0", reply.items(0).name(), "a");
            c.check<int>("unary.count1", static_cast<int>(reply.items(1).count()), 2);
        }
        const auto &labels = reply.labels();
        auto it = labels.find("k");
        c.check<std::string>("unary.label",
                             it == labels.end() ? "<missing>" : it->second, "v");
        cq.Shutdown();
        void *drain = nullptr;
        bool dok = false;
        while (cq.Next(&drain, &dok)) {}
    }

    // ---- server streaming ----
    {
        grpc::CompletionQueue cq;
        grpc::ClientContext ctx;
        auto rw = stub.PrepareCall(&ctx, "/interop.v1.Interop/EchoStream", &cq);
        void *t = reinterpret_cast<void *>(2);
        rw->StartCall(t);
        bool ok = await_tag(&cq, t);
        grpc::ByteBuffer send = to_buffer(wire);
        rw->Write(send, t);
        ok = ok && await_tag(&cq, t);
        rw->WritesDone(t);
        ok = ok && await_tag(&cq, t);
        c.check<std::string>("stream.started", ok ? "yes" : "no", "yes");

        std::string joined;
        for (;;) {
            grpc::ByteBuffer msg;
            rw->Read(&msg, t);
            if (!await_tag(&cq, t)) break;
            interop::v1::EchoReply reply;
            reply.ParseFromString(from_buffer(msg));
            if (!joined.empty()) joined += ",";
            joined += reply.text();
        }
        grpc::Status status;
        rw->Finish(&status, t);
        await_tag(&cq, t);
        c.check<std::string>("stream.texts", joined, "hello-0,hello-1,hello-2");
        c.check<int>("stream.status", status.error_code(), grpc::StatusCode::OK);
        cq.Shutdown();
        void *drain = nullptr;
        bool dok = false;
        while (cq.Next(&drain, &dok)) {}
    }

    // ---- non-OK status ----
    {
        grpc::CompletionQueue cq;
        grpc::ClientContext ctx;
        grpc::ByteBuffer send = to_buffer(wire);
        auto reader = stub.PrepareUnaryCall(
            &ctx, "/interop.v1.Interop/Fail", send, &cq);
        reader->StartCall();
        grpc::ByteBuffer recv;
        grpc::Status status;
        void *tag = reinterpret_cast<void *>(3);
        reader->Finish(&recv, &status, tag);
        await_tag(&cq, tag);
        c.check<int>("fail.status", status.error_code(),
                     grpc::StatusCode::FAILED_PRECONDITION);
        c.check<std::string>("fail.message", status.error_message(),
                             "interop failure");
        cq.Shutdown();
        void *drain = nullptr;
        bool dok = false;
        while (cq.Next(&drain, &dok)) {}
    }

    return c.finish("cpp_client");
}
