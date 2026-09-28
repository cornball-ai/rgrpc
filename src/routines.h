// The .Call routine table, as one list of (name, arity) pairs.
//
// init.cpp expands it into prototypes and the registration table;
// stub.cpp expands it into definitions that error. Both builds register
// the same routines, so the R code is identical with or without gRPC.

#ifndef GRPC_R_ROUTINES_H
#define GRPC_R_ROUTINES_H

#define RGRPC_ROUTINES(X)              \
    X(grpc_r_available, 0)             \
    RGRPC_GRPC_ROUTINES(X)

// The routines that need gRPC; the stub defines each as an error.
#define RGRPC_GRPC_ROUTINES(X)         \
    X(grpc_r_version, 0)               \
    X(grpc_r_channel_create, 1)        \
    X(grpc_r_channel_destroy, 1)       \
    X(grpc_r_cq_create, 0)             \
    X(grpc_r_cq_destroy, 1)            \
    X(grpc_r_server_create, 1)         \
    X(grpc_r_server_port, 1)           \
    X(grpc_r_server_destroy, 1)        \
    X(grpc_r_client_create, 8)         \
    X(grpc_r_client_state, 1)          \
    X(grpc_r_client_close, 1)          \
    X(grpc_r_client_fd, 1)             \
    X(grpc_r_client_pending, 1)        \
    X(grpc_r_client_poll, 4)           \
    X(grpc_r_call_start, 6)            \
    X(grpc_r_call_cancel, 2)           \
    X(grpc_r_server2_create, 11)       \
    X(grpc_r_server2_close, 1)         \
    X(grpc_r_server2_fd, 1)            \
    X(grpc_r_server2_port, 1)          \
    X(grpc_r_server2_pending, 1)       \
    X(grpc_r_server2_reply, 6)         \
    X(grpc_r_server2_poll, 4)          \
    X(grpc_r_stream_start, 7)          \
    X(grpc_r_stream_send, 3)           \
    X(grpc_r_stream_writes_done, 2)    \
    X(grpc_r_server2_read, 2)          \
    X(grpc_r_server2_cancel, 2)        \
    X(grpc_r_server2_send, 3)          \
    X(grpc_r_server2_finish, 6)

#define RGRPC_ARGS_0 void
#define RGRPC_ARGS_1 SEXP
#define RGRPC_ARGS_2 SEXP, SEXP
#define RGRPC_ARGS_3 SEXP, SEXP, SEXP
#define RGRPC_ARGS_4 SEXP, SEXP, SEXP, SEXP
#define RGRPC_ARGS_6 SEXP, SEXP, SEXP, SEXP, SEXP, SEXP
#define RGRPC_ARGS_7 SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP
#define RGRPC_ARGS_8 SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP
#define RGRPC_ARGS_11 SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, SEXP, \
    SEXP, SEXP, SEXP

#endif
