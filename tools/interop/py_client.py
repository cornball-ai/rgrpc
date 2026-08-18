"""Python interop peer, client side. Driven by tools/interop/interop.sh.

Calls the R server and prints one PY line per assertion so the driver can
grep results rather than parse exit codes. Exits nonzero if any check
fails, so a silent misparse cannot read as a pass.
"""
import sys
import os

import grpc

sys.path.insert(0, os.environ["INTEROP_GEN"])
import interop_pb2  # noqa: E402
import interop_pb2_grpc  # noqa: E402

failures = []


def check(name, got, want):
    ok = got == want
    if not ok:
        failures.append(name)
    print("PY %s %s got=%r want=%r" % ("ok" if ok else "FAIL", name, got, want),
          flush=True)


def main():
    addr = sys.argv[1]
    with grpc.insecure_channel(addr) as chan:
        stub = interop_pb2_grpc.InteropStub(chan)

        req = interop_pb2.EchoRequest(
            text="hello",
            items=[interop_pb2.Item(name="a", count=1),
                   interop_pb2.Item(name="b", count=2)],
            labels={"k": "v"},
            number=3,
        )

        reply = stub.Echo(req)
        check("unary.text", reply.text, "hello")
        check("unary.responder", reply.responder, "r")
        check("unary.items", [(i.name, i.count) for i in reply.items],
              [("a", 1), ("b", 2)])
        check("unary.labels", dict(reply.labels), {"k": "v"})

        got = [r.text for r in stub.EchoStream(req)]
        check("stream.texts", got, ["hello-0", "hello-1", "hello-2"])

        try:
            stub.Fail(req)
            check("fail.status", "no error", "FAILED_PRECONDITION")
        except grpc.RpcError as e:
            check("fail.status", e.code(), grpc.StatusCode.FAILED_PRECONDITION)
            check("fail.message", e.details(), "interop failure")

    if failures:
        print("PY FAILED: %s" % ", ".join(failures), flush=True)
        sys.exit(1)
    print("PY all ok", flush=True)


if __name__ == "__main__":
    main()
