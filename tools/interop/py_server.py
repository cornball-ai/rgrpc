"""Python interop peer, server side. Driven by tools/interop/interop.sh.

Reference implementation for the R client to talk to: ordinary grpcio
with generated stubs, which is what a real Python user has. Generated
code is produced into a scratch directory by the driver and put on
sys.path there, so nothing is committed pre-generated and nothing is
installed into the system interpreter.
"""
import sys
import os
from concurrent import futures

import grpc

sys.path.insert(0, os.environ["INTEROP_GEN"])
import interop_pb2  # noqa: E402
import interop_pb2_grpc  # noqa: E402


class Interop(interop_pb2_grpc.InteropServicer):
    def Echo(self, request, context):
        return interop_pb2.EchoReply(
            text=request.text,
            items=request.items,
            labels=request.labels,
            responder="python",
        )

    def EchoStream(self, request, context):
        # request.number carries how many replies to emit, so the R side
        # can assert an exact count rather than "some".
        for i in range(request.number):
            yield interop_pb2.EchoReply(
                text="%s-%d" % (request.text, i), responder="python"
            )

    def Fail(self, request, context):
        context.abort(grpc.StatusCode.FAILED_PRECONDITION, "interop failure")


def main():
    addr = sys.argv[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    interop_pb2_grpc.add_InteropServicer_to_server(Interop(), server)
    server.add_insecure_port(addr)
    server.start()
    print("ready", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
