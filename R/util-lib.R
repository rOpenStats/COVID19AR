#' getEnv
#' @author ken4rab
#' @export
getEnv <- function(variable.name, package.prefix = getPackagePrefix(), fail.on.empty = TRUE,
                   env.file = "~/.Renviron", call.counter = 0, refresh.env = FALSE,
                   logger = lgr) {
  if (refresh.env) {
    readRenviron(env.file)
    # this does not work
    # dotenv::load_dot_env()
   if (file.exists(".env")) {
     readRenviron(".env")
   }
  }
  prefixed.variable.name <- paste(package.prefix, variable.name, sep = "")
  # First look for parameter without prefix, expected in .env
  ret <- Sys.getenv(variable.name)
  if (nchar(ret) == 0) {
    # If not found, then look for parameter with prefix, expected in .Renviron
    ret <- Sys.getenv(prefixed.variable.name)
  }
  if (nchar(ret) == 0) {
    if (call.counter == 0) {
      readRenviron(env.file)
      ret <- getEnv(
        variable.name = variable.name, package.prefix = package.prefix,
        fail.on.empty = fail.on.empty, env.file = env.file,
        call.counter = call.counter + 1
      )
    } else {
      message <- paste(
        "Must configure variable",
        prefixed.variable.name,
        "in", env.file,
        "or", variable.name,
        "in", ".env"
      )
      if (fail.on.empty) {
        stop(message)
      } else {
        logger$warn(message)
        ret <- NULL
      }
    }
  }
  ret
}


#' getLogsDir
#' @export
getLogsDir <- function() {
  logs.dir <- file.path(getEnv("data_dir"), "logs")
  dir.create(logs.dir, showWarnings = FALSE, recursive = TRUE)
  logs.dir
}

#' retrieveURL
#' @import lgr
#' @author kenarab
#' @export
retrieveURL <- function(data.url, col.types,
                        dest.filename,
                        dest.dir = getEnv("data_dir"),
                        force.download = FALSE,
                        download.new.data = TRUE,
                        daily.update.time = "20:00:00",
                        download.timeout = 480,
                        logger = lgr) {
  current.date <- Sys.Date()
  dest.path <- file.path(dest.dir, dest.filename)
  ret <- FALSE
  exists.dest.path <- file.exists(dest.path)
  logger$info("Exists dest path?", dest.path = dest.path, exists.dest.path = exists.dest.path)
  download.flag <- dir.exists(dest.dir)
  if (download.flag) {
    download.flag <- !file.exists(dest.path)
    if (!download.flag & file.exists(dest.path)) {
      if (download.new.data) {
        dest.path.check <- dest.path
        dest.path.check <- fixEncoding(dest.path.check)
        data.check <- read_csv(dest.path.check, col_types = col.types)
        max.date <- getMaxDate(data.check, report.date = current.date)
        current.datetime <- Sys.time()
        current.date <- as.Date(current.datetime, tz = Sys.timezone())
        current.time <- format(current.datetime, format = "%H:%M:%S")
        if (max.date < current.date - 1 | (max.date < current.date & current.time >= daily.update.time)) {
          download.flag <- TRUE
        }
        logger$info("Checking required downloaded ",
          downloaded.max.date = max.date,
          daily.update.time = daily.update.time,
          current.datetime = current.datetime,
          download.flag = download.flag
        )
      }
    }
  } else {
    stop(paste("Dest dir does not exists", dest.dir = dest.dir))
  }
  if (download.flag | force.download) {
    logger$info("Retrieving", url = data.url, dest.path = dest.path)
    options(timeout = download.timeout)
    download.file(url = data.url, destfile = dest.path)
    ret <- TRUE
  }
  dest.path
}

#' getPackagePrefix
#' @author kenarab
#' @export
getPackagePrefix <- function() {
  "COVID19AR_"
}

#' getAPIKey
#' @author kenarab
#' @export
getAPIKey <- function() {
  ret <- list()
  ret[["client_id"]] <- getEnv("client_id")
  ret[["client_secret"]] <- getEnv("client_secret")
  ret
}


#' apiCall
#' @import httr
#' @import jsonlite
#' @author kenarab
#' @export
apiCall <- function(url) {
  tmp <- getAPIKey()
  request <-
    GET(
      url = url,
      query = list(
        client_id = tmp[["client_id"]],
        client_secret = tmp[["client_secret"]]
      )
    )
  ret <- NULL
  if (request$status_code == 200) {
    response <- content(request, as = "text", encoding = "UTF-8")
    ret <- fromJSON(response) %>% data.frame()
  } else {
    message(paste("Request returned code =", request$status_code, ".Cannot parse content"))
    ret <- request
  }
  ret
}

#' genDateSubdir
#' @author kenarab
#' @export
genDateSubdir <- function(home.dir, create.dir = TRUE) {
  current.date <- Sys.Date()
  ret <- file.path(home.dir, as.character(current.date, "%Y"), as.character(current.date, "%m"), as.character(current.date, "%d"))
  dir.create(ret, recursive = TRUE, showWarnings = FALSE)

  ret
}


#' zipFile
#' @import utils
#' @author kenarab
#' @export
zipFile <- function(home.dir, current.file, rm.original = TRUE, overwrite = FALSE, minimum.size.accepted = 2000,
                    logger = lgr) {
  # Do not zip zip files
  if (!grepl("zip$", current.file)) {
    current.filepath <- file.path(home.dir, current.file)
    if (file.exists(current.filepath)) {
      file.info.current.filepath <- file.info(current.filepath)

      # TODO change 10000 with a statistics based threshold
      if (file.info.current.filepath$size < minimum.size.accepted) {
        message <- paste("Cannot process file", current.filepath, " with less than", minimum.size.accepted, "size. And was", file.info.current.filepath$size)
        logger$error(message)
        stop(message)
      }
      # Original file has to exists
      current.file.zipped <- paste(current.filepath, "zip", sep = ".")

      current.filepath <- gsub("\\/\\/", "/", current.filepath)
      current.file.zipped <- gsub("\\/\\/", "/", current.file.zipped)

      if (!file.exists(current.file.zipped) | overwrite) {
        # Expand paths
        current.filepath <- path.expand(current.filepath)
        current.file.zipped <- path.expand(current.file.zipped)
        # current.filepath <- normalizePath(current.filepath)
        logger$info(paste("Zipping", current.filepath))
        ret <- utils::zip(current.file.zipped, files = current.filepath)
        if (rm.original) {
          unlink(current.filepath)
        }
      }
    }
  }
}


#' removeAccents
#' @author kenarab
#' @export
removeAccents <- function(text) {
  ret <- iconv(text, to = "ASCII//TRANSLIT")
  ret <- gsub("'|\\~", "", ret)
  ret
}

#' normalizeString
#' @author kenarab
#' @export
normalizeString <- function(text, lowercase = TRUE) {
  text <- removeAccents(trimws(text))
  if (lowercase) {
    text <- tolower(text)
  } else {
    text <- toupper(text)
  }
}

#' genLogger
#' @export
genLogger <- function(r6.object) {
  lgr::get_logger(class(r6.object)[[1]])
}

#' getLogger
#' @author kenarab
#' @export
getLogger <- function(r6.object) {
  ret <- r6.object$logger
  if (is.null(ret)) {
    class <- class(r6.object)[[1]]
    stop(paste("Class", class, "don't seems to have a configured logger"))
  } else {
    ret.class <- class(ret)[[1]]
    if (ret.class == "logical") {
      stop(paste("Class", ret.class, "needs to initialize logger: self$logger <- genLogger(self)"))
    }
  }
  ret
}

#' loggerSetupFile
#' @param log.file
#' @import lgr
#' @export
loggerSetupFile <- function(log.file, default.threshold) {
  lgr::basic_config()
  lgr::get_logger("root")$add_appender(AppenderFile$new(log.file,
    layout = LayoutFormat$new(
      fmt = "%L [%t] %m %j",
      timestamp_fmt = "%Y-%m-%d %H:%M:%OS3",
      colors = NULL,
      pad_levels = "right"
    )
  ))
  lgr::threshold(default.threshold, lgr::get_logger("root"))
  lgr
}


#' loggerSetupFile
#' @param log.file
#' @import lgr
#' @export
loggerSetupFile <- function(log.file) {
  lgr::basic_config()
  lgr::get_logger("root")$add_appender(AppenderFile$new(log.file,
    layout = LayoutFormat$new(
      fmt = "%L [%t] %m %j",
      timestamp_fmt = "%Y-%m-%d %H:%M:%OS3",
      colors = NULL,
      pad_levels = "right"
    )
  ))
}


#' pad
#' @export
pad <- function(number, decimals) {
  sprintf(
    fmt = paste("%0", decimals, "d", sep = ""),
    number
  )
}

#' fixEncoding return filepath with encoding in UTF8
#' @import readr
#' @author kenarab
#' @export
fixEncoding <- function(file.path) {
  filename <- strsplit(file.path, split = "/")[[1]]
  filename <- filename[length(filename)]
  encoding.matches <- guess_encoding(file.path)
  encoding.utf16 <- FALSE
  best.match <- encoding.matches[1, ]
  if (grepl("(utf16|utf-16)", tolower(best.match[, "encoding"])) & best.match[, "confidence"] > 0.8) {
    encoding.utf16 <- TRUE
  }
  if (encoding.utf16) {
    # Has utf16 encoding
    file.path.original <- file.path
    file.path <- gsub("\\.csv$", ".utf8.csv", file.path)
    current.os <- getOS()
    if (current.os == "macos") {
      utf16.encoding <- "utf-16"
      utf8.encoding <- "utf-8"
    }
    if (current.os == "linux") {
      utf16.encoding <- "UTF16"
      utf8.encoding <- "UTF8"
    }
    if (current.os == "windows") {
      stop("File encoding conversion from UTF16 to UTF8 not implemented yet in Windows OS")
    }

    # iconv -f UTF16 -t UTF8 Covid19Casos.csv > Covid19Casos.utf8.csv
    iconv.command <- paste("iconv -f", utf16.encoding, "-t", utf8.encoding, file.path.original, ">", file.path)
    command.result <- system(iconv.command, intern = TRUE)
  }
  file.path
}

#' mutate_cond
#' @export
mutate_cond <- function(.data, condition, ..., envir = parent.frame()) {
  condition <- eval(substitute(condition), .data, envir)
  .data[condition, ] <- .data[condition, ] %>% mutate(...)
  .data
}

#' getOS returns linux, windows or macos
#' @author kenarab
#' @export
getOS <- function() {
  sysinf <- Sys.info()
  if (!is.null(sysinf)) {
    os <- sysinf["sysname"]
    if (os == "Darwin") {
      os <- "MacOS"
    }
  } else { ## mystery machine
    os <- .Platform$OS.type
    if (grepl("^darwin", R.version$os)) {
      os <- "MacOS"
    }
    if (grepl("linux-gnu", R.version$os)) {
      os <- "linux"
    }
  }
  tolower(os)
}

#' getMaxDate
#' @author kenarab
#' @export
getMaxDate <- function(covid19ar.data, report.date) {
  logger <- lgr
  data.fields <- names(covid19ar.data)
  date.fields <- data.fields[grep("fecha\\_", data.fields)]
  covid19ar.data$max.date <- apply(covid19ar.data[, date.fields], MARGIN = 1, FUN = function(x) {
    max(x, na.rm = TRUE)
  })
  future.rows <- covid19ar.data %>% filter(max.date > report.date)
  future.rows.agg <- future.rows %>%
    group_by(max.date) %>%
    summarise(n = n())
  for (i in seq_len(nrow(future.rows.agg))) {
    future.row <- future.rows.agg[i, ]
    logger$info("Future rows", date = future.row$max.date, n = future.row$n)
  }
  covid19ar.data <- covid19ar.data %>% filter(max.date <= report.date)
  max.dates <- apply(covid19ar.data[, date.fields], MARGIN = 2, FUN = function(x) {
    max(x, na.rm = TRUE)
  })
  max.date <- max(max.dates)
  max.date
}

#' removeQuotes
#' @export
removeQuotes <- function(df, fields, quotes.regexp = "\"", quotes.replacement = "") {
  for (field in fields) {
    df[, field] <- gsub(quotes.regexp, quotes.replacement, df[, field])
  }
  df
}


#' getSizeFormatted
#' @export
getSizeFormatted <- function(size, unit = NULL, digits = 2) {
  log.10.size <- log(size, base = 10)
  if (!is.null(unit)) {
    if (unit == "B") {
      log.10.size <- 1
    }
    if (unit == "KB") {
      log.10.size <- 3
    }
    if (unit == "MB") {
      log.10.size <- 6
    }
    if (unit == "GB") {
      log.10.size <- 9
    }
    if (unit == "TB") {
      log.10.size <- 12
    }
  }
  if (log.10.size >= 3) {
    if (log.10.size >= 6) {
      if (log.10.size >= 9) {
        if (log.10.size >= 12) {
          digit.threshold <- 12
          unit <- "TB"
        } else {
          digit.threshold <- 9
          unit <- "GB"
        }
      } else {
        digit.threshold <- 6
        unit <- "MB"
      }
    } else {
      digit.threshold <- 3
      unit <- "KB"
    }
    size.ret <- round(size / 10^digit.threshold, digits)
  } else {
    unit <- "B"
    size.ret <- size
  }
  ret <- c(size.ret, unit)
  ret
}

#' checkFileDownload
#' @export
checkFileDownload <- function(filepath, update.ts = Sys.time(), min.ts.diff = 12 * 60 * 60,
                              min.size = Inf) {
  ret <- TRUE
  if (file.exists(filepath)) {
    current.file.info <- file.info(filepath)
    observed.ts.diff <- as.numeric(difftime(update.ts, current.file.info$mtime, unit = "secs"))
    ret <- observed.ts.diff >= min.ts.diff | current.file.info$size < min.size
  }
  ret
}

checkUpdatedUrl <- function(download.url, filepath, filepath.prev,
                            logger = lgr){
 # curl_options()[grep("header", names(curl_options()))]
 # h <- new_handle()
 # handle_setopt(h, copypostfields = "moo=moomooo");
 # handle_setopt(h, )
 # handle_setheaders(h,
 #                   "Content-Type" = "text/moo",
 #                   "Cache-Control" = "no-cache",
 #                   "User-Agent" = "A cow"
 # )
 #curl::curl_fetch_memory(url = download.url, )

 fetch.header.command <- paste("curl -sI", download.url, "| grep -i Content-Length")
 content.length <- system(fetch.header.command, intern = TRUE)
 content.length <- gsub("Content-Length: ", "", content.length)
 content.length <- as.numeric(content.length)
 prev.size <- as.numeric(NA)
 if (file.exists(filepath.prev)){
  prev.size <- file.info(filepath.prev)$size
 }
 current.size <- as.numeric(NA)
 if (file.exists(filepath)){
  current.size <- file.info(filepath)$size
 }
 if (!is.na(current.size) | !is.na(prev.size)){
   max.size <- max(current.size, prev.size, na.rm = TRUE)
   download <- content.length > max(max.size)
 }
 else{
  download <- TRUE
 }
 if (is.na(current.size) & !is.na(prev.size)){
  download <- content.length != prev.size
  logger$info("Downloading because different file remote - prev",
              content.length = content.length,
              prev.size = prev.size)

 }
 max.current.file <- max(content.length, current.size, na.rm = TRUE)
 if (!is.na(max.current.file) & !is.na(prev.size)){
  if (max.current.file < prev.size){
   logger$warn("Prev file is greater than current", prev.size = prev.size,
                  current.size = current.size)
  }
 }
 if (!is.na(content.length) & !is.na(current.size)){
  if (content.length > current.size){
   logger$warn("Remote file is greater than current",
                  content.length = content.length,
                  current.size = current.size)
   download <- TRUE
  }
 }
 if (!download){
   logger$debug("Not downloading. Remote file is equal", url.size = content.length,
                prev.size = prev.size,
                current.size = current.size)
 }
 else{
  logger$info("Downloading", url.size = content.length,
               prev.size = prev.size,
               current.size = current.size)
 }
 download
}

#' binaryDownload
#' @import RCurl
#' @export
binaryDownload <- function(url, file, logger = lgr) {
  # enum {
  #  CURL_SSLVERSION_DEFAULT, // 0
  #  CURL_SSLVERSION_TLSv1, /* TLS 1.x */ // 1
  #  CURL_SSLVERSION_SSLv2, // 2
  #  CURL_SSLVERSION_SSLv3, // 3
  #  CURL_SSLVERSION_TLSv1_0, // 4
  #  CURL_SSLVERSION_TLSv1_1, // 5
  #  CURL_SSLVERSION_TLSv1_2, // 6
  #  CURL_SSLVERSION_TLSv1_3, // 7
  #
  #  CURL_SSLVERSION_LAST /* never use, keep last */ // 8
  # };
  #

  # opts <- RCurl::curlOptions()
  # if (ssl.version == "1.1") {
  #
  #   # TLS 1.1
  #   CURL_SSLVERSION_TLSv1_1 <- 5L
  #   opts <- RCurl::curlOptions( # verbose = TRUE,
  #     sslversion = CURL_SSLVERSION_TLSv1_1
  #   )
  # }
  # if (ssl.version == "1.2") {
  #   CURL_SSLVERSION_TLSv1_2 <- 6L
  #   # TLS 1.2
  #   opts <- RCurl::curlOptions( # verbose = TRUE,
  #     sslversion = CURL_SSLVERSION_TLSv1_2
  #   )
  # }
  #  f <- CFILE(file, mode="wb")
  #  a <- curlPerform(url = url, writedata = f@ref, noprogress=FALSE, .opts = opts)
  # close(f)
  #  a
  logger$info("curl_downlad", url = url, destfile = file)
  curl::curl_download(url = url, destfile = file, mode = "wb", quiet = FALSE)
}

#' unzipSystem
#' @export
unzipSystem <- function(zip.path, args = "-oj", exdir , logger = lgr) {
  ret <- NULL
  current.dir <- getwd()
  tryCatch(
    {
      setwd(exdir)
      decompress.command <- "unzip"
      args <- c(
        "-o", # include override flag
        zip.path
      )
      logger$debug("Executing",
        command = decompress.command,
        args = paste(args, collapse = " "),
        exdir = exdir
      )
      ret <-
        system2(decompress.command,
          args = args,
          stdout = TRUE
        )
    },
    error = function(e) {
      logger$error("Found error where uncompressing", e = e)
      setwd(current.dir)
    }
  )
  setwd(current.dir)
  ret
}




#' unJarSystem
#' @export
unJarSystem <- function(zip.path, exdir , logger = lgr) {
  ret <- NULL
  current.dir <- getwd()
  tryCatch(
    {
      setwd(exdir)
      decompress.command <- "jar"
      args <- paste("xfv", zip.path)
      logger$debug("Executing",
        command = decompress.command,
        args = paste(args, collapse = " "),
        exdir = exdir
      )
      ret <-
        system2(decompress.command,
          args = args,
          stdout = TRUE
        )
    },
    error = function(e) {
      logger$error("Found error where uncompressing", e = e)
      setwd(current.dir)
    }
  )
  setwd(current.dir)
  ret
}

