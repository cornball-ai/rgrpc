## Install the CRAN packages that link the protobuf and absl recipes from
## source against whatever is in /opt/R/$arch, then load each one.
## Dependencies come from CRAN binaries so only the targets are compiled.
##
##   Rscript check-dependents.R <outdir>

args <- commandArgs(trailingOnly = TRUE)
outdir <- if (length(args)) args[[1L]] else "dependents"
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

repos <- c(CRAN = "https://cloud.r-project.org")
targets <- c("RProtoBuf", "protolite", "cld3", "tfevents", "otelsdk", "s2")

lib <- file.path(outdir, "lib")
dir.create(lib, showWarnings = FALSE)
.libPaths(c(lib, .libPaths()))

## binary dependencies, so only the targets compile against /opt/R
db <- available.packages(repos = repos)
deps <- tools::package_dependencies(targets, db = db, recursive = TRUE,
                                    which = c("Depends", "Imports",
                                              "LinkingTo"))
deps <- setdiff(unique(unlist(deps)),
                c(targets, rownames(installed.packages(priority = "base"))))
deps <- setdiff(deps, rownames(installed.packages()))
if (length(deps)) {
    install.packages(deps, lib = lib, repos = repos, type = "binary",
                     quiet = TRUE)
}

src <- download.packages(targets, destdir = outdir, repos = repos,
                         type = "source", quiet = TRUE)
rbin <- file.path(R.home("bin"), "R")
rscript <- file.path(R.home("bin"), "Rscript")

one <- function(pkg, tarball) {
    log <- file.path(outdir, paste0(pkg, "-install.log"))
    st <- system2(rbin, c("CMD", "INSTALL", "-l", shQuote(lib),
                          shQuote(tarball)),
                  stdout = log, stderr = log)
    loaded <- FALSE
    if (st == 0L) {
        expr <- sprintf(".libPaths(c('%s', .libPaths())); library(%s)",
                        lib, pkg)
        loaded <- system2(rscript, c("-e", shQuote(expr)),
                          stdout = FALSE, stderr = FALSE) == 0L
    }
    data.frame(package = pkg, version = db[pkg, "Version"],
               installed = st == 0L, loads = loaded)
}

res <- do.call(rbind, Map(one, src[, 1L], src[, 2L]))
rownames(res) <- NULL
print(res)
write.csv(res, file.path(outdir, "summary.csv"), row.names = FALSE)

for (pkg in res$package[!res$installed]) {
    cat("\n===== ", pkg, " (last 30 lines) =====\n", sep = "")
    log <- readLines(file.path(outdir, paste0(pkg, "-install.log")))
    cat(utils::tail(log, 30L), sep = "\n")
}
