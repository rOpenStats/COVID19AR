#' COVID19ARdownloader
#' @author kenarab
#' @importFrom R6 R6Class
#' @import sqldf
#' @import dplyr
#' @import zoo
#' @import reshape2
#' @export
COVID19ARdownloader <- R6Class("COVID19ARdownloader",
  public = list(
   data.dir     = NA,
   working.dir  = NA,
   # Consolidated
   date = Sys.Date(),
   covid19ar.agg = NA,
   json.output = NULL,
   logger = NA,
   initialize = function(data.dir = getEnv("data_dir", refresh.env = TRUE)){
    self$data.dir <- data.dir
    self$working.dir <- file.path(tempdir(), "COVID19AR")
    dir.create(self$working.dir, showWarnings = FALSE, recursive = TRUE)
    self$logger <- genLogger(self)
    self
   },
   preprocess = function(){
    stop("Abstract class")
   },
   aggregate = function(){
    stop("Abstract class")
   },
   generateJson = function(){
    stop("Abstract class")
   },
   saveJson = function(dest.path = file.path(self$data.dir, paste("covid19ar_", as.character(self$date, format = "%Y%m%d"), ".json", sep = ""))){
    write_file(jsonlite::write_json(self$json.output, dest.path))
   }))


#' COVID19ARdownloaderCovidStats
#' @author kenarab
#' @importFrom R6 R6Class
#' @import sqldf
#' @import dplyr
#' @import zoo
#' @import reshape2
#' @export
COVID19ARdownloaderCovidStats <- R6Class("COVID19ARdownloaderCovidStats",
inherit = COVID19ARdownloader,
public = list(
# Consolidated2
covidstats.df = NULL,
#
initialize = function(data.dir = getEnv("data_dir"))
 {
 super$initialize(data.dir = data.dir)
 self$data.dir <- data.dir
 self$working.dir <- file.path(tempdir(), "COVID19AR")
 dir.create(self$working.dir, showWarnings = FALSE, recursive = TRUE)
 self
},
preprocess = function(force.download = FALSE){
 logger <- getLogger(self)
 # TODO put in .env
 self$data.dir
 dir(self$data.dir)
 vacunas.fields <- c("dosis1", "dosis2", "refuerzo", "adicional", "esquemacompleto")
 dir.create(self$data.dir, showWarnings = FALSE, recursive = FALSE)
 covidstats.summary.filepath <- file.path(self$data.dir, "covidstats-casos-vacunaciones.json")
 download <- checkFileDownload(covidstats.summary.filepath, min.ts.diff = 19*60*60)
 if (download | force.download){
  #download.file("https://covidstats.com.ar/ws/mapa?porprovincia=false&pordepartamento=true", destfile = covidstats.summary.filepath)
  download.file("https://covidstats.com.ar/ws/mapa?porprovincia=true&pordepartamento=false&datosadicionales=true", destfile = covidstats.summary.filepath)
 }
 if (file.exists(covidstats.summary.filepath)){
  covidstats.json <- fromJSON(read_file(covidstats.summary.filepath))
  logger$info("Building provincias Catalog", nrow = length(covidstats.json))
  ut.catalog.df <- data.frame(idx = numeric(), codigo = character(),
                                         provincia = character(),  poblacion = numeric())
  self$covidstats.df <- NULL
  for (i in seq_len(length(covidstats.json))){
   covidstats.unidad.territorial <- covidstats.json[[i]]
   covidstats.unidad.territorial <<- covidstats.unidad.territorial
   provincia <- unique(covidstats.unidad.territorial$denominacion)
   current.codigo <- unique(covidstats.unidad.territorial$codigo)
   # Casos and vacunation can not be updated up to same day
   length.data <- lapply(covidstats.unidad.territorial, FUN = length)
   data.length <- unlist(unique(length.data))
   data.length <- data.length[data.length > 1]
   data.length <- min(data.length)
   covidstats.unidad.territorial.clean <- lapply(covidstats.unidad.territorial, FUN = function(x)x[seq_len(data.length)])
   length.data <- lapply(covidstats.unidad.territorial.clean, FUN = length)
   covidstats.ut.df <- as.data.frame(covidstats.unidad.territorial.clean)
   covidstats.ut.df$day <- seq_len(nrow(covidstats.ut.df))
   covidstats.ut.df %<>% mutate(date = as.Date("2020-03-01") + day - 1)
   tail(covidstats.ut.df)
   stopifnot(length(current.codigo) == 1)
   covidstats.ut.df %<>% mutate(denominacion = zoo::na.locf(denominacion, na.rm = FALSE ))
   covidstats.ut.df %<>% mutate(codigo = zoo::na.locf(codigo, na.rm = FALSE ))
   covidstats.ut.df %<>% mutate(poblacion = zoo::na.locf(poblacion, na.rm = FALSE ))
   current.ut <- data.frame(idx = i, codigo = current.codigo, provincia = unique(covidstats.ut.df$denominacion))
   current.ut %<>% mutate(poblacion = unique(covidstats.ut.df$poblacion))
   ut.catalog.df <- rbind(ut.catalog.df, current.ut)
   self$covidstats.df <- rbind(self$covidstats.df, covidstats.ut.df)
  }
 }
 logger$info("Retrieved covidstats", nrow = nrow(self$covidstats.df))
},
aggregate = function(){
 self$covid19ar.agg <- self$covidstats.df %>%
  group_by(date) %>% summarize(casos_fa = sum(casos_fa),
                               casos_dx = sum(casos_dx),
                               fallecidos = sum(fallecidos),
                               diagnosticos = sum(diagnosticos),
                               dosis1 = sum(dosis1),
                               dosis2 = sum(dosis2),
                               adicional = sum(adicional),
                               refuerzo = sum(refuerzo),
                               poblacion = sum(poblacion)
  )
 counting.fields <- names(self$covid19ar.agg)
 counting.fields <- counting.fields[-grep("date", counting.fields)]
 covid19ar.agg.diff <- self$covid19ar.agg
 self$covid19ar.agg$type <- "accum"
 covid19ar.agg.diff$type <- "diff"
 for( field in counting.fields){
  first <- covid19ar.agg.diff[1, field]
  covid19ar.agg.diff[, field] <- covid19ar.agg.diff[, field] - lag(covid19ar.agg.diff[, field])
  covid19ar.agg.diff[1, field] <- first
 }
 self$covid19ar.agg %<>% bind_rows(covid19ar.agg.diff) %>% arrange(date, type)
 self$covid19ar.agg %<>% select(one_of(c("date", "type", counting.fields)))
 self$covid19ar.agg
},
generateOutputJson = function(source = "covidstats"){
  tail(self$covid19ar.agg, n = 15)
  names(self$covidstats.df)
  self$json.output <- list()
}
))




#' COVID19ARdownloaderMinsal
#' @author kenarab
#' @importFrom R6 R6Class
#' @import sqldf
#' @import dplyr
#' @import zoo
#' @import reshape2
#' @export
COVID19ARdownloaderMinsal <- R6Class("COVID19ARdownloaderMinsal",
  inherit = COVID19ARdownloader,
  public = list(
    data.dir     = NA,
    working.dir  = NA,
    cases.filename = NA,
    vaccines.filename = NA,
    cases.csv.filepath = NA,
    vaccines.csv.filepath = NA,
    hour.change.day = 21,
    current.date = NA,
    decompress = NA,
    # db
    #cases.db.file = NA,
    #vacciones.db.file = NA,
    # Consolidated
    cases.agg.df = NA,
    vaccines.agg.df = NA,
    processing.log = NULL,
    initialize = function(data.dir = getEnv("data_dir"),
                          cases.filename = "Covid19Casos.csv",
                          vaccines.filename = "datos_nomivac_covid19.csv",
                          decompress = TRUE
    ){
     super$initialize(data.dir)
     self$cases.filename <- cases.filename
     self$vaccines.filename <- vaccines.filename
     self$decompress        <- decompress
     self$processing.log <- data.frame(key = character(), begin.end = character(), ts = character())
     self
    },
    preprocessCasesMinsal = function(force.preprocess = FALSE){
     logger <- getLogger(self)
     self$setCurrentDate()
     cases.preprocessed.filepath <- file.path(self$data.dir, "cases_agg.csv")
     #cases.zip.path <- file.path(self$data.dir, gsub("\\.csv", ".zip", self$cases.filename))
     current.date.text <- as.character(self$current.date, format = "%y%m%d")
     previous.date.text <- as.character(self$current.date - 1, format = "%y%m%d")
     cases.zip.path <- file.path(self$data.dir, gsub("\\.csv",
                                 paste("_", current.date.text,".zip", sep = ""), self$cases.filename))
     cases.prev.zip.path <- gsub(current.date.text, previous.date.text, cases.zip.path)
     #download <- checkFileDownload(cases.zip.path, min.ts.diff = self$download.min.ts.diff, min.size = 340000000)
     download.url <- "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.zip"
     download <- checkUpdatedUrl(download.url, cases.zip.path, cases.prev.zip.path, logger = logger)
     #if (cases.info$mtime < Sys.time() - 60*60*19 | cases.info$size < 300000000){
     if (download){
       #https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.zip
       # download.file("https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.zip", destfile = cases.zip.path,
       #               method = "wget",
       #               mode = "wb")
       binaryDownload(url, cases.zip.path, logger = logger)
     }
     dest.file <- file.path(self$working.dir, self$cases.filename)
     logger$debug("Checking", cases.zip.path = cases.zip.path)
     if (self$decompress){
       if(file.exists(cases.zip.path) & !file.exists(dest.file) ){
        logger$info("Decompressing", zip.filepath = cases.zip.path)
        #unzip(normalizePath(cases.zip.path), junkpaths = TRUE, exdir = normalizePath(self$working.dir))
        # unzipSystem(normalizePath(cases.zip.path), args = "-oj", exdir = self$working.dir,
        #             logger = logger)
        self$unzip(cases.zip.path)

       }
       else{
        if (!file.exists(cases.zip.path)){
         stop(paste("File not found:", cases.zip.path))
        }
       }
      dir(self$working.dir)
      cases.filepath <- file.path(self$working.dir, self$cases.filename)
      stopifnot(file.exists(cases.filepath))
      logger$debug("File available",
                   cases.df = cases.filepath)
      self$cases.csv.filepath <- cases.filepath

     }
     #self$cases.db.file <- tempfile()
     # cases.sql.text <- paste("select replace(fecha_apertura, '\"', '') fecha_apertura,
     #                                 sum(CASE WHEN clasificacion_resumen == '\"Confirmado\"' THEN 1 ELSE 0) positivos,
     #                                 count(*) diagnosticos,
     #                                 sum(CASE WHEN cuidado_intensivo == '\"SI\"' THEN 1 ELSE 0) cuidados_intensivos,
     #                                 sum(CASE WHEN fecha_internacion == '\"\"' THEN 1 ELSE 0) internados,
     #                                 sum(CASE WHEN fallecido == '\"SI\"' THEN 1 ELSE 0) fallecidos
     #                           from file")
     # cases.file.info <- file.info(cases.filepath)
     # logger$debug("Generating sqldf db and cases.agg.df",
     #              cases.filepath = cases.filepath,
     #              size = paste(getSizeFormatted(cases.file.info$size), collapse = ""))
     # self$cases.agg.df <- read.csv.sql(self$cases.csv.filepath,
     #               sql = cases.sql.text,
     #               dbname = self$cases.db.file,
     #               #dbname = NULL,
     #               header = TRUE, sep = ",")
     # logger$debug("Running query repeated cases",
     #              cases.filepath = cases.filepath,
     #              size = paste(getSizeFormatted(cases.file.info$size), collapse = ""))
     self
    },
    preprocessVaccinesMinsal = function(force.preprocess = FALSE){
     logger <- getLogger(self)
     self$setCurrentDate()
     #self$data.dir <- "~/../Downloads/"
     current.date.text <- as.character(self$current.date, format = "%y%m%d")
     previous.date.text <- as.character(self$current.date - 1, format = "%y%m%d")
     vaccines.zip.path <- file.path(self$data.dir, gsub("\\.csv",
                                                        paste("_", as.character(self$current.date, format = "%y%m%d"),".zip", sep = ""), self$vaccines.filename))
     vaccines.prev.zip.path <- gsub(current.date.text, previous.date.text, vaccines.zip.path)
     #download <- checkFileDownload(vaccines.zip.path, min.ts.diff = self$download.min.ts.diff, min.size = 340000000)
     download.url <- "https://sisa.msal.gov.ar/datos/descargas/covid-19/files/datos_nomivac_covid19.zip"
     download <- checkUpdatedUrl(download.url, vaccines.zip.path, vaccines.prev.zip.path, logger = logger)

     download
     if (download){
      #https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.zip
      binaryDownload(download.url, vaccines.zip.path, logger = logger)
     }


     dest.file <- file.path(self$working.dir, self$vaccines.filename)
     if (self$decompress){
      if(file.exists(vaccines.zip.path) & !file.exists(dest.file) ){
       logger$debug("Decompressing", zip.filepath = vaccines.zip.path)
       #unzip(vaccines.zip.path, junkpaths = TRUE, exdir = self$working.dir)
       #unzipSystem(vaccines.zip.path, args = "-oj", exdir = self$working.dir, logger = logger)
       self$unzip(vaccines.zip.path)
      }
      vaccines.filepath <- file.path(self$working.dir, self$vaccines.filename)
      self$vaccines.csv.filepath <- vaccines.filepath
      stopifnot(file.exists(vaccines.filepath))
     }
    },
    setCurrentDate = function(){
     day.hour <- as.numeric(as.character(Sys.time(), format = "%H"))
     self$current.date <- Sys.Date() -1
     if (day.hour >= self$hour.change.day){
      self$current.date <- self$current.date + 1
     }

    },
    unzip = function(filepath){
     logger <- getLogger(self)
     os <- getOS()
     if (os %in% c("osx", "macos", "linux")) {
      unJarSystem(filepath, exdir = self$working.dir, logger = logger)
     }
     if (os == "windows") {
      unzip(normalizePath(filepath), junkpaths = TRUE, exdir = normalizePath(self$working.dir))
      #
      #unzipSystem(filepath, exdir = self$working.dir, logger = logger)
     }
    },
    preprocess = function(force.download = FALSE){
     logger <- getLogger(self)
     stopifnot(dir.exists(self$data.dir))
     #self$current.date <- as.character(self$current.date, format = "%y%m%d")
     self$preprocessCasesMinsal(force.preprocess = force.download)
     self$preprocessVaccinesMinsal(force.preprocess = force.download)
    }
   ))


generateAggregatedVaccinesWithSqldf <- function(vaccines.filepath, sepi.fis.df, total.chunks = 2, logger = lgr){
 chunk.begin <- 1
 chunk.size <- ceiling(nrow(sepi.fis.df) / total.chunks)
 chunk.begin + chunk.size
 counter <- 1
 logger$info("starting processing", total.chunks = total.chunks,
             chunk.size = chunk.size)
 vaccines.agg.chunks <- list()
 while(chunk.begin < nrow(sepi.fis.df)){
  gc()
  chunk.end <- min(chunk.begin + chunk.size, nrow(sepi.fis.df))
  current.chunk <- sepi.fis.df[chunk.begin:chunk.end,]
  chunk.min.fis <- paste(sepi.fis.df[chunk.begin, c("year_fis", "sepi_fis")], collapse = "-")
  chunk.max.fis <- paste(sepi.fis.df[chunk.end, c("year_fis", "sepi_fis")], collapse = "-")
  sql.text <- paste("select strftime('%Y', replace(fecha_aplicacion, '\"', '')) year_fis,
                          strftime('%W', replace(fecha_aplicacion, '\"', '')) week_fis,
                          jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario,
                          count(*) as applications
                   from file
                   where strftime('%Y-%W', replace(fecha_aplicacion, '\"', '')) >= '", chunk.min.fis,"'
                     and strftime('%Y-%W', replace(fecha_aplicacion, '\"', '')) <= '", chunk.max.fis,"'
                    group by fecha_aplicacion,
                            strftime('%Y', replace(fecha_aplicacion, '\"', '')),
                            strftime('%W', replace(fecha_aplicacion, '\"', '')),
                            jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario", sep = "")
  vaccines.agg.chunk.df <-
   read.csv.sql(vaccines.filepath,
                sql = sql.text, header = TRUE, sep = ",")
  base::closeAllConnections()
  head(vaccines.agg.chunk.df)
  chunk.sepi.fis.df <-
   vaccines.agg.chunk.df %>%
   group_by(year_fis, week_fis) %>%
   summarize(n = n())
  min.fs <- paste(chunk.sepi.fis.df[1, c("year_fis", "week_fis")], collapse = "-")
  max.fs <- paste(chunk.sepi.fis.df[nrow(chunk.sepi.fis.df), c("year_fis", "week_fis")], collapse = "-")
  vaccines.agg.chunk.df %<>% mutate(applications = as.numeric(applications))
  logger$debug("Processed chunk", counter = counter,
               nrows = nrow(vaccines.agg.chunk.df),
               min.fs = min.fs,
               max.fs = max.fs,
               applications = sum(vaccines.agg.chunk.df$applications))
  nrow(vaccines.agg.chunk.df)
  tail(vaccines.agg.chunk.df)
  vaccines.agg.chunk.df <- removeQuotes(vaccines.agg.chunk.df,
                                        fields = c("jurisdiccion_residencia", "depto_residencia", "grupo_etario", "applications"))
  chunk.begin <- chunk.end + 1
  counter <- counter + 1
  #stopifnot(counter <= 1)
  vaccines.agg.chunks[[counter]] <- vaccines.agg.chunk.df
 }
 length(vaccines.agg.chunks)
 #lapply(vaccines.agg.chunks, FUN = nrow)
 #stop("Under construction")
 vaccines.agg.df <- NULL
 for (vaccines.chunk.df in vaccines.agg.chunks){
  vaccines.agg.df <- rbind(vaccines.agg.df, vaccines.chunk.df)
 }
 logger$info("Generated vacciones.agg.df using read.csv.sql",
             nrows = nrow(vaccines.agg.df))
 names(vaccines.agg.df)
 vaccines.agg.df
}




