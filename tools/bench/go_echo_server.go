// Go echo server for tools/bench/bench.sh. The reference-implementation
// server the R client is measured against.
//
// No protoc codegen: grpc.UnknownServiceHandler plus a pass-through
// codec accepts any method and echoes raw bytes. That is deliberate as
// well as convenient — the benchmark is measuring transport and
// event-loop cost, so a server that does no marshalling keeps the
// comparison about the thing being compared. It also registers under
// codec name "proto", so the content-subtype matches exactly what an
// ordinary gRPC client sends.
package main

import (
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	b, ok := v.(*[]byte)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	*b = append((*b)[:0], data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

// One handler covers both shapes under test: a unary call is a stream
// that carries exactly one message before half-close, so the same
// receive-echo loop terminates after one round trip.
func echo(srv interface{}, stream grpc.ServerStream) error {
	for {
		var buf []byte
		if err := stream.RecvMsg(&buf); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if err := stream.SendMsg(&buf); err != nil {
			return err
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go_echo_server <addr>   (unix:/path or host:port)")
	}
	addr := os.Args[1]
	network := "tcp"
	if len(addr) > 5 && addr[:5] == "unix:" {
		network = "unix"
		addr = addr[5:]
		_ = os.Remove(addr)
	}
	lis, err := net.Listen(network, addr)
	if err != nil {
		log.Fatalf("listen %s %s: %v", network, addr, err)
	}
	s := grpc.NewServer(
		grpc.UnknownServiceHandler(echo),
		grpc.ForceServerCodec(rawCodec{}),
	)
	// Printed only once the listener is up, so the driver can wait on
	// this line instead of racing the socket.
	log.SetFlags(0)
	log.Println("ready")

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
		<-c
		s.Stop()
	}()
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
