# List pod sandboxes and images from a live containerd over CRI.
#
# Needs read/write access to the containerd socket and the CRI plugin
# enabled (containerd config: remove "cri" from disabled_plugins).
#
#   Rscript --vanilla cri-list-pods.R [socket-path]

library(grpc)
args <- commandArgs(trailingOnly = TRUE)
socket <- if (length(args)) args[[1]] else "/run/containerd/containerd.sock"

proto_root <- system.file("proto", "cri", package = "grpc")
RProtoBuf::readProtoFiles2("api.proto", protoPath = proto_root)
rt <- grpc_service("runtime.v1.VersionRequest", "RuntimeService")

cl <- grpc_client(sprintf("unix://%s", socket))

call <- grpc_call(cl, grpc_method(rt, "Version"),
                  RProtoBuf::P("runtime.v1.VersionRequest")$new(version = "v1"),
                  deadline_ms = 5000)
v <- grpc_await(call, timeout_ms = 5000)[[1]]
stopifnot(v$status_name == "OK")
cat("runtime:", v$response_message$runtime_name,
    v$response_message$runtime_version, "\n")

call <- grpc_call(cl, grpc_method(rt, "ListPodSandbox"),
                  RProtoBuf::P("runtime.v1.ListPodSandboxRequest")$new(),
                  deadline_ms = 5000)
p <- grpc_await(call, timeout_ms = 5000)[[1]]
stopifnot(p$status_name == "OK")
pods <- p$response_message$items
cat(length(pods), "pod sandbox(es)\n")
for (s in pods) {
  cat(" -", s$metadata$namespace, "/", s$metadata$name, ":",
      RProtoBuf::name(s$state), "\n")
}

grpc_close(cl)
