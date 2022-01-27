#' COVID19AR
#'
#' A package to work with SQL datasources in a simple manner
#'
#' @docType package
#' @name COVID19AR
#' @import R6 lgr DBI
#' @importFrom utils str
#' @author Alejandro Baranek <abaranek@dc.uba.ar>

#' Executes code while loading the package.
#'
#' @param libname Library name
#' @param pkgname Package name
# execute onload
.onLoad <- function(libname, pkgname) {
 data.dir <- getEnv("data_dir", refresh.env = TRUE, fail.on.empty = FALSE)
  if (!is.null(data.dir)){
  }
  os <- getOS()
  if (os == "osx") {
  }
  if (os == "windows") {
    Sys.setlocale("LC_CTYPE", "Spanish_Argentina")
  }
}
