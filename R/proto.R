## RProtoBuf integration (increment 4): resolve service and method
## descriptors from RProtoBuf's runtime descriptor pool, validate typed
## requests, and decode typed responses. The transport itself stays
## byte-oriented; everything here is a layer over it, and RProtoBuf
## remains a Suggests.
##
## Service resolution goes through the FileDescriptorProto (obtained via
## as(fileDescriptor(anchor), "Message")) rather than RProtoBuf's
## ServiceDescriptor accessors, which are broken in 0.4.27; see
## https://github.com/eddelbuettel/rprotobuf/issues/116.

.needs_rprotobuf <- function() {
    if (!requireNamespace("RProtoBuf", quietly = TRUE)) {
        stop("this feature requires the 'RProtoBuf' package")
    }
}

#' Resolve a gRPC service from the RProtoBuf descriptor pool
#'
#' Looks up the service definitions in the \code{.proto} file that
#' defines \code{anchor}, a message type from that file (schemas are
#' loaded at runtime with \code{RProtoBuf::readProtoFiles2()}; there is
#' no generated code). Returns the service with its methods resolved:
#' full method paths, input and output message types, and streaming
#' flags.
#'
#' @param anchor A message type from the \code{.proto} file that defines
#'   the service: a type name like \code{"runtime.v1.VersionRequest"}, an
#'   \code{RProtoBuf} \code{Descriptor}, or a \code{Message}.
#' @param service Service to select, by short or fully qualified name.
#'   May be omitted when the file defines exactly one service.
#' @return An object of class \code{"grpc_service"}: \code{name},
#'   \code{package}, and a named list \code{methods} of
#'   \code{"grpc_method"} objects.
#' @examples
#' \dontrun{
#' RProtoBuf::readProtoFiles2("api.proto", protoPath = proto_dir)
#' svc <- grpc_service("runtime.v1.VersionRequest", "RuntimeService")
#' names(svc$methods)
#' }
#' @export
grpc_service <- function(anchor, service = NULL) {
    .needs_rprotobuf()
    if (is.character(anchor)) {
        anchor <- RProtoBuf::P(anchor)
    }
    fd <- RProtoBuf::fileDescriptor(anchor)
    fdp <- methods::as(fd, "Message")
    pkg <- fdp$package
    svcs <- fdp$service
    if (length(svcs) == 0L) {
        stop("no services defined in '", RProtoBuf::name(fd), "'")
    }
    nms <- vapply(svcs, function(s) s$name, "")
    fq <- if (nzchar(pkg)) paste0(pkg, ".", nms) else nms
    if (is.null(service)) {
        if (length(svcs) > 1L) {
            stop("file defines several services (",
                 paste(fq, collapse = ", "), "); pick one with `service`")
        }
        idx <- 1L
    } else {
        idx <- match(service, fq)
        if (is.na(idx)) idx <- match(service, nms)
        if (is.na(idx)) {
            stop("service '", service, "' not found; available: ",
                 paste(fq, collapse = ", "))
        }
    }
    s <- svcs[[idx]]
    methods <- lapply(s$method, function(m) {
        structure(list(name = m$name,
                       path = paste0("/", fq[idx], "/", m$name),
                       input_type = sub("^\\.", "", m$input_type),
                       output_type = sub("^\\.", "", m$output_type),
                       client_streaming = isTRUE(m$client_streaming),
                       server_streaming = isTRUE(m$server_streaming)),
                  class = "grpc_method")
    })
    names(methods) <- vapply(s$method, function(m) m$name, "")
    structure(list(name = fq[idx], package = pkg, methods = methods),
              class = "grpc_service")
}

#' Look up a method in a resolved service
#'
#' @param service A \code{"grpc_service"} from \code{\link{grpc_service}}.
#' @param name Method name, e.g. \code{"Version"}.
#' @return A \code{"grpc_method"} object: \code{name}, \code{path},
#'   \code{input_type}, \code{output_type}, \code{client_streaming},
#'   \code{server_streaming}. Pass it as the \code{method} argument of
#'   \code{\link{grpc_call}} for typed calls.
#' @examples
#' \dontrun{m <- grpc_method(svc, "Version")}
#' @export
grpc_method <- function(service, name) {
    stopifnot(inherits(service, "grpc_service"))
    m <- service$methods[[name]]
    if (is.null(m)) {
        stop("method '", name, "' not in service '", service$name,
             "'; available: ", paste(names(service$methods), collapse = ", "))
    }
    m
}

#' Decode protocol buffer bytes to a message
#'
#' Thin wrapper over \code{RProtoBuf::read()} for decoding a request or
#' response payload against a type from the runtime descriptor pool.
#'
#' @param bytes Raw vector, e.g. the \code{request} field of a server
#'   event or the \code{response} field of a client completion.
#' @param type Fully qualified message type name, e.g. an
#'   \code{input_type} from a \code{"grpc_method"}.
#' @return An \code{RProtoBuf} \code{Message}.
#' @examples
#' \dontrun{req <- grpc_decode(ev$request, m$input_type)}
#' @export
grpc_decode <- function(bytes, type) {
    .needs_rprotobuf()
    stopifnot(is.raw(bytes), is.character(type), length(type) == 1L)
    RProtoBuf::read(RProtoBuf::P(type), bytes)
}
