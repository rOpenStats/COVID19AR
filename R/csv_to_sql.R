#' Save a delimited text table into a single table sqlite database
#'
#' The table can be a comma separated (csv) or a tab separated (tsv) or any
#' other delimited text file. The file is read in chunks. Each chunk is copied
#' in the same sqlite table database before the next chunk is loaded into
#' memory. See the INBO tutorial \href{https://github.com/inbo/tutorials/blob/master/source/data-handling/large-files-R.Rmd}{Handling large files in R}
#' to learn more about.
#'
#' Version modified by @kenarab
#'
#' @section Remark:
#' The \code{callback} argument in the \code{read_delim_chunked} function call
#' refers to the custom written callback function `append_to_sql` applied
#' to each chunk.
#'
#' @param csv_file Name of the text file to convert.
#' @param sqlite_file Name of the newly created sqlite file.
#' @param table_name Name of the table to store the data table in the sqlite
#'   database.
#' @param delim Text file delimiter (default ",").
#' @param pre_process_size Number of lines to check the data types of the
#'   individual columns (default 1000).
#' @param chunk_size Number of lines to read for each chunk (default 50000).
#' @param show_progress_bar Show progress bar (default TRUE).
#' @param con = dbConnect(SQLite(), dbname = sqlite_file),
#' @param cols.spec = NULL Columns specification for read_delim
#' @param disconnect.after = TRUE
#' @param n_max = Inf
#' @param logger = lgr
#' @param ... Further arguments to be passed to \code{read_delim}.
#'
#' @return a SQLite database
#' @family Data_handling_utilities
#' @examples
#' \dontrun{
#' library(R.utils)
#' library(dplyr)
#' csv.name <- "2016-04-20-processed-logs-big-file-example.csv"
#' db.name <- "2016-04-20-processed-logs-big-file-example.db"
#' # download the CSV file example
#' csv.url <- paste("https://s3-eu-west-1.amazonaws.com/lw-birdtracking-data/",
#'   csv.name, ".gz",
#'   sep = ""
#' )
#' download.file(csv.url, destfile = paste0(csv.name, ".gz"))
#' gunzip(paste0(csv.name, ".gz"))
#' # Make a SQLite database
#' sqlite_file <- "example2.sqlite"
#' sqlite_conn <- dbConnect(SQLite(), dbname = sqlite_file)
#' table_name <- "birdtracks"
#' csv_to_sql(
#'   csv_file = csv.name,
#'   con = sqlite_conn,
#'   table_name = table_name
#' )
#' # Get access to SQLite database
#' my_db <- src_sqlite(sqlite_file, create = FALSE)
#' bird_tracking <- tbl(my_db, "birdtracks")
#' # Example query via dplyr
#' results <- bird_tracking %>%
#'   filter(device_info_serial == 860) %>%
#'   select(date_time, latitude, longitude, altitude) %>%
#'   filter(date_time < "2014-07-01") %>%
#'   filter(date_time > "2014-03-01") %>%
#'   as_tibble()
#' head(results)
#' }
#' @export
#' @importFrom DBI dbConnect dbDisconnect
#' @importFrom RSQLite SQLite dbWriteTable
#' @importFrom readr read_delim read_delim_chunked
#' @importFrom dplyr %>% select_if mutate_at
#' @importFrom lubridate is.Date is.POSIXt
csv_to_sql <- function(csv_file, table_name,
                       delim = ",",
                       quote = "\"",
                       pre_process_size = 1000, chunk_size = 50000,
                       show_progress_bar = TRUE,
                       con = dbConnect(SQLite(), dbname = sqlite_file),
                       cols.spec = NULL,
                       disconnect.after = TRUE,
                       n_max = Inf,
                       logger = lgr,
                       ...) {
  # con <- dbConnect(SQLite(), dbname = sqlite_file)

  # read a first chunk of data to extract the colnames and types
  # to figure out the date and the datetime columns

  can_process <- FALSE
  convert_dates2text <- FALSE
  con_class <- class(con)[[1]]
  if (con_class == "SQLiteConnection") {
    convert_dates2text <- TRUE
    can_process <- TRUE
  }
  if (con_class == "PostgreSQLConnection") {
    convert_dates2text <- FALSE
    can_process <- TRUE
  }
  if (!can_process) {
    stop(paste("Connection class", con_class, "not yet implemented"))
  }

  stopifnot(file.exists(csv_file))
  df <- read_delim(csv_file,
    delim = delim, n_max = min(pre_process_size, n_max),
    quote = quote,
    col_types = cols.spec, ...
  )
  date_cols <- df %>%
    select_if(is.Date) %>%
    colnames()
  datetime_cols <- df %>%
    select_if(is.POSIXt) %>%
    colnames()

  if (convert_dates2text) {
    # write the first batch of lines to SQLITE table, converting dates to string
    # representation
    x %<>%
      mutate_at(.vars = date_cols, .funs = as.character.Date) %>%
      mutate_at(.vars = datetime_cols, .funs = as.character.POSIXt)
  }
  dbWriteTable(con, table_name, df, overwrite = TRUE)
  csv_info <- file.info(csv_file)
  # readr chunk functionality
  logger$info("Uploading data",
    table_name = table_name,
    csv_file = csv_file,
    file_size = getSizeFormatted(csv_info$size)
  )
  append.function <- append_to_sql(
    con = con, table_name = table_name,
    date_cols = date_cols,
    datetime_cols = datetime_cols,
    convert_dates2text = convert_dates2text
  )
  if (n_max < Inf) {
    logger$warn("read_delim instead of read_delim_chunked called as ", n_max = n_max)
    data <- read_delim(
      csv_file,
      delim = delim,
      quote = quote,
      skip = pre_process_size + 1,
      progress = show_progress_bar,
      col_names = names(attr(df, "spec")$cols),
      col_types = cols.spec,
      n_max = n_max - pre_process_size,
      ...
    )
    append.function(data)
  } else {
    read_delim_chunked(
      csv_file,
      callback = append.function,
      delim = delim,
      quote = quote,
      skip = pre_process_size + 1,
      chunk_size = chunk_size,
      progress = show_progress_bar,
      col_names = names(attr(df, "spec")$cols),
      col_types = cols.spec,
      ...
    )
  }
  logger$info("Data uploaded")
  if (disconnect.after) {
    dbDisconnect(con)
  }
}

#' Callback function that appends new sections to the SQLite table.
#' @param con A valid connection to SQLite database.
#' @param table_name Name of the table to store the data table in the sqlite
#'   database.
#' @param date_cols Name of columns containing Date objects
#' @param datetime_cols Name of columns containint POSIXt objects.
#' @param convert_dates2text = TRUE Converting date fields to text (For sqlite db)
#' @import magrittr
#' @import dplyr
#' @keywords internal
append_to_sql <- function(con, table_name,
                          date_cols, datetime_cols,
                          convert_dates2text = TRUE) {
  #' @param x Data.frame we are reading from.
  function(x, pos) {
    x <- as.data.frame(x)
    if (convert_dates2text) {
      x %<>%
        mutate_at(.vars = date_cols, .funs = as.character.Date) %>%
        mutate_at(.vars = datetime_cols, .funs = as.character.POSIXt)
    }
    # append data frame to table
    dbWriteTable(con, table_name, x,
      append = TRUE,
      field.types =
      )
  }
}
