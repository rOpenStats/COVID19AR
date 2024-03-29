

#' retrieveArgentinasDeathsStatistics
#' @author kenarab
#' @export
retrieveArgentinasDeathsStatistics <- function(download.new.data = FALSE) {
  # http://www.deis.msal.gov.ar/index.php/base-de-datos/
  # TODO link scrape automation with rvest
  # deaths.stats.html <- read_html("http://www.deis.msal.gov.ar/index.php/base-de-datos/")
  # deaths.stats.html %>% html_nodes("") %>% html_attr('href')
  # deaths.stats.html %>% html_attr('href')
  deaths.path <- file.path(getEnv("data_dir"), "deaths")
  dir.create(deaths.path, recursive = TRUE, showWarnings = FALSE)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/descdef1.xlsx",
              dest.filename = "DescDef1.xlsx",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb19.csv",
              dest.filename = "DefWeb19.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb18.csv",
              dest.filename = "DefWeb18.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb17.csv",
              dest.filename = "DefWeb17.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb16.csv",
              dest.filename = "DefWeb16.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb15.csv",
              dest.filename = "DefWeb15.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb14.csv",
              dest.filename = "DefWeb14.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb13.csv",
              dest.filename = "DefWeb13.csv",
              download.new.data = download.new.data)
  retrieveURL(data.url = "https://www.argentina.gob.ar/sites/default/files/2021/03/defweb12.csv",
              dest.filename = "DefWeb12.csv",
              download.new.data = download.new.data)
  for (cy in 11:05){
    expected.filename <- paste("defweb", pad(cy, 2), ".csv", sep = "")
    retrieveURL(data.url = paste("https://www.argentina.gob.ar/sites/default/files/2021/03/", expected.filename, sep = ""),
               dest.filename = expected.filename,
               download.new.data = download.new.data)
  }
  # There is data up to 15

}


#' readMetadata
#' @author kenarab
#' @import readxl
#' @export
readMetadata <- function(file.path) {
  metadata <- read_xlsx(file.path, sheet = "DISEÑO", skip = 4)
  names(metadata)
  ret <- list()
  ret[["metadata"]] <- metadata
  for (i in seq_len(nrow(metadata))) {
    current.field.row <- metadata[i, ]
    field.name <- as.character(current.field.row[, 1])
    if (!is.na(current.field.row[, 3]) & current.field.row[, 3] == "Ver códigos") {
      if (field.name == "CAUSA") {
        sheet.name <- "CODMUER"
      } else {
        sheet.name <- field.name
      }
      current.field.sheet <- read_xlsx(file.path, sheet = sheet.name)
      ret[[field.name]] <- current.field.sheet
    }
  }
  ret
}


#' codeEdad
#' @author kenarab
#' @export
codeEdad <- function(data.deaths) {
  logger <- lgr
  all.codes <- sort(unique(data.deaths$GRUPEDAD))
  edad.regexp <- "([0-9]{2})\\_([0-9]{2}) a ([0-9]{2})"
  data.deaths$EDAD_CODE <- NA
  data.deaths$EDAD_MIN <- NA
  data.deaths$EDAD_MAX <- NA
  rows2correct <- grep(edad.regexp, data.deaths$GRUPEDAD)
  data2correct <- pull(data.deaths[rows2correct, ], "GRUPEDAD")
  codes2correct <- all.codes[grep(edad.regexp, all.codes)]
  data.deaths[rows2correct, "EDAD_CODE"] <- as.numeric(gsub(edad.regexp, "\\1", data2correct))
  data.deaths[rows2correct, "EDAD_MIN"] <- as.numeric(gsub(edad.regexp, "\\2", data2correct))
  data.deaths[rows2correct, "EDAD_MAX"] <- as.numeric(gsub(edad.regexp, "\\3", data2correct))
  codes.not.recognized <- sort(setdiff(all.codes, codes2correct))
  codes.na <- sort(unique(data.deaths[is.na(data.deaths$EDAD_CODE), ]$GRUPEDAD))
  stopifnot(identical(codes.not.recognized, codes.na))
  for (manual.code in codes.not.recognized) {
    rows2correct <- which(data.deaths$GRUPEDAD == manual.code)
    if (grepl("01_Menor de 1", manual.code)) {
      EDAD_CODE <- 1
      EDAD_MIN <- 0
      EDAD_MAX <- 1
    }
    if (manual.code == "02_1 a 9") {
      EDAD_CODE <- 2
      EDAD_MIN <- 1
      EDAD_MAX <- 9
    }
    if (grepl("17_80 y m", manual.code)) {
      EDAD_CODE <- 17
      EDAD_MIN <- 80
      EDAD_MAX <- 120
    }
    if (manual.code == "99_Sin especificar") {
      EDAD_CODE <- 99
      EDAD_MIN <- NA
      EDAD_MAX <- NA
    }
    # debug
    print(manual.code)

    data.deaths[rows2correct, ]$EDAD_CODE <- EDAD_CODE
    data.deaths[rows2correct, ]$EDAD_MIN <- EDAD_MIN
    data.deaths[rows2correct, ]$EDAD_MAX <- EDAD_MAX
    logger$info(paste("Correct", length(rows2correct), "for manual.code", manual.code), EDAD_MIN = EDAD_MIN, EDAD_MAX = EDAD_MAX)
  }
  data.deaths[, "EDAD_MEDIA"] <- (data.deaths[, "EDAD_MIN"] + data.deaths[, "EDAD_MAX"] - 1) / 2
  data.deaths
}


#' ConsolidatedDeathsData.class
#' @importFrom R6 R6Class
#' @import dplyr
#' @export
ConsolidatedDeathsData.class <- R6Class("ConsolidatedDeathsData",
  public = list(
    data.dir = NA,
    metadata = NA,
    data = NA,
    warnings = NULL,
    initialize = function(data.dir = getEnv("data_dir")) {
      self$data.dir <- data.dir
      self
    },
    consolidate = function() {
      self$metadata <- readMetadata(file.path = file.path(self$data.dir, "DescDef1.xlsx"))
      data.files <- dir(self$data.dir)
      data.files <- data.files[grep("csv", data.files)]
      ret <- NULL
      for (data.file in data.files) {
        lgr$info("Reading", file = data.file)
        data.deaths.current <- self$readDeathsStats(data.file)
        ret <- rbind(ret, data.deaths.current)
      }
      self$data <- ret
      self$postProcess()
      self$data
    },
    postProcess = function() {
      self$data$codigo.causas <- apply(self$data[, c("CAUSA", "CAUSA_description")],
        MARGIN = 1,
        FUN = function(x) {
          paste(x, collapse = "|")
        }
      )
      self$data
    },
    readDeathsStats = function(filename, fail.on.error = FALSE) {
      data.deaths <- read_csv(file.path(self$data.dir, filename),
        col_types =
          cols(
            PROVRES = col_character(),
            SEXO = col_character(),
            CAUSA = col_character(),
            MAT = col_character(),
            GRUPEDAD = col_character(),
            CUENTA = col_double()
          )
      )
      data.deaths <- as.data.frame(data.deaths)
      nrow(data.deaths)
      filename.regexp <- "DefWeb([0-9]{2}).csv"
      year.extracted <- gsub(filename.regexp, "\\1", filename)
      year <- as.numeric(paste("20", year.extracted, sep = ""))
      data.deaths$year <- year

      # FIX CAUSA in lowercase
      data.deaths$CAUSA <- toupper(data.deaths$CAUSA)

      # GRUPEDAD
      # data.deaths$GRUPEDAD <- normalizeString(data.deaths$GRUPEDAD)
      data.deaths <- codeEdad(data.deaths)
      # Generate factors for fields descriptions
      for (field in names(data.deaths)) {
        if (field %in% names(self$metadata)) {
          field.description <- paste(field, "description", sep = "_")
          metadata.df <- as.data.frame(self$metadata[[field]], stringsAsFactors = TRUE)
          names(metadata.df) <- c(field, field.description)
          metadata.df[, field.description] <- as.factor(metadata.df[, field.description])
          data.deaths <- data.deaths %>% left_join(metadata.df, by = field)
          # check missing values
          rows.not.coded <- which(is.na(data.deaths[, field.description]))
          codes.not.metadata <- unique(data.deaths[rows.not.coded, field])
          cases <- sum(data.deaths[rows.not.coded, ]$CUENTA)
          if (length(codes.not.metadata) > 0) {
            if (length(codes.not.metadata) == 1 & is.na(codes.not.metadata)) {
              lgr$info("NA values", count = length(rows.not.coded), field = field)
            } else {
              missed.codes <- paste(codes.not.metadata, collapse = ",")
              message <- paste("Codes", missed.codes, "in field", field, "not coded")
              if (fail.on.error) {
                stop(message)
              } else {
                new.warning <- data.frame(
                  year = year, field = field, message = message,
                  missed.codes = missed.codes, cases = cases
                )
                lgr$warn(new.warning)
                self$warnings <- rbind(self$warnings, new.warning)
              }
            }
          }
        }
      }
      nrow(data.deaths)
      data.deaths
    }
  )
)
