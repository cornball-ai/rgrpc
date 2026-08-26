## Fork-safety probe. Driven by tools/fork-probe.sh.
##
## R users fork constantly -- parallel::mclapply, anything mirai-adjacent
## -- and gRPC's fork support is fragile. This does not try to make
## forking work; it establishes what actually happens so the failure mode
## can be documented instead of discovered. Three cases, because they
## fail differently and only one of them is hopeless:
##
##   reuse   channel opened in the parent, used in the child. The
##           completion thread does not survive fork, so nothing is left
##           to reap completions in the child.
##   parent  does the parent's own channel still work after forking?
##           This is the case that decides whether mclapply() poisons the
##           session or only the workers.
##   fresh   child opens its own channel after the fork. If this works,
##           the documented rule can be "open after forking" rather than
##           "never fork".
##
## Every child is given a hard deadline and killed, because the expected
## failure is a hang rather than an error: a probe that inherits the hang
## reports nothing at all.
##
## argv: sock case timeout_s
library(rgrpc)
library(parallel)
sock    <- argv[[1L]]
case    <- argv[[2L]]
limit   <- if (length(argv) >= 3L) as.numeric(argv[[3L]]) else 10
tgt     <- sprintf("unix://%s", sock)

cat(sprintf("FORK case=%s fork_support=%s\n", case,
            Sys.getenv("GRPC_ENABLE_FORK_SUPPORT", "unset")))
flush(stdout())

## One unary round trip, with a deadline so a wedged transport reports
## instead of hanging. Returns a short verdict string.
one_call <- function(cl, tag) {
    verdict <- tryCatch({
        call <- grpc_call(cl, "/fork.v1.Echo/Ping", as.raw(1L),
                          deadline_ms = 2000L)
        ## grpc_await() returns a batch, not an event. Taking [[1]] is
        ## right here only because the batch is already filtered to this
        ## one call and a unary call has exactly one terminal event.
        evs <- grpc_await(call, timeout_ms = 3000L)
        if (!length(evs)) "TIMEOUT"
        else if (is.null(evs[[1L]]$status_name))
            paste0("NO_STATUS(kind=", evs[[1L]]$kind, ")")
        else evs[[1L]]$status_name
    }, error = function(e) paste0("ERROR:", conditionMessage(e)))
    ## A zero-length or NA verdict must not vanish: sprintf() on
    ## character(0) yields character(0) and cat() then prints nothing at
    ## all, so the probe would report a hang as silence. That is exactly
    ## how the first version of this file failed.
    if (length(verdict) != 1L || is.na(verdict)) verdict <- "UNKNOWN"
    sprintf("%s=%s", tag, verdict)
}

## Long wait, to establish whether the child's call is merely late or
## never completes at all. The difference decides the documented advice:
## a bounded error is survivable, an unbounded wait is the mystery hang
## the plan asks to have written down.
long_call <- function(cl) {
    call <- grpc_call(cl, "/fork.v1.Echo/Ping", as.raw(1L))
    evs <- grpc_await(call, timeout_ms = 30000L)
    if (!length(evs)) "TIMEOUT_30s" else "COMPLETED"
}

if (identical(case, "reuse") || identical(case, "parent") ||
    identical(case, "reuse_block")) {
    cl <- grpc_client(tgt)
    cat(sprintf("FORK pre_fork %s\n", one_call(cl, "parent_before")))
    flush(stdout())

    ## mcparallel rather than a bare fork(): it is what mclapply uses,
    ## so the observed behaviour is the one a real caller will hit.
    child <- mcparallel({
        if (identical(case, "reuse")) one_call(cl, "child_reused")
        else if (identical(case, "reuse_block")) long_call(cl)
        else "child_did_nothing"
    })
    res <- mccollect(child, wait = FALSE, timeout = limit)
    if (is.null(res)) {
        cat(sprintf("FORK child_result=HUNG_%.0fs\n", limit))
        tools::pskill(child$pid, tools::SIGKILL)
        mccollect(child)  # reap
    } else {
        cat(sprintf("FORK child_result=%s\n", as.character(res[[1L]])))
    }
    flush(stdout())

    ## The question that decides whether forking is survivable: is the
    ## parent's own channel still usable once a child has forked off it?
    cat(sprintf("FORK post_fork %s\n", one_call(cl, "parent_after")))
    grpc_close(cl)
} else if (identical(case, "fresh")) {
    child <- mcparallel({
        cl2 <- grpc_client(tgt)
        r <- one_call(cl2, "child_fresh")
        grpc_close(cl2)
        r
    })
    res <- mccollect(child, wait = FALSE, timeout = limit)
    if (is.null(res)) {
        cat(sprintf("FORK child_result=HUNG_%.0fs\n", limit))
        tools::pskill(child$pid, tools::SIGKILL)
        mccollect(child)
    } else {
        cat(sprintf("FORK child_result=%s\n", as.character(res[[1L]])))
    }
} else {
    cat(sprintf("FORK ABORT unknown case %s\n", case))
    quit(status = 2L)
}
cat("FORK done\n")
flush(stdout())
