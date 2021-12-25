
#' EcologicalInferenceGenerator
#' @author kenarab
#' @importFrom R6 R6Class
#' @import sqldf
#' @import dplyr
#' @reshape2
#' @export
EcologicalInferenceGenerator <- R6Class("EcologicalInferenceGenerator",
  public = list(
   use.sqlfd    = TRUE,
   data.dir     = NA,
   working.dir  = NA,
   cases.filename = NA,
   vaccines.filename = NA,
   edad.coder   = NA,
   population.df = NA,
   # Consolidated
   provincias.departamentos.edad.df = NA,
   cases.agg.df = NA,
   cases.agg.identified = NA,
   vaccines.agg.df = NA,
   #
   merged.df    = NA,
   edad.coded   = NA,
   logger = NA,
   initialize = function(data.dir = "~/Downloads/",
                         cases.filename = "Covid19Casos.csv",
                         vaccines.filename = "datos_nomivac_covid19.csv"
   ){
    self$data.dir <- data.dir
    self$cases.filename <- cases.filename
    self$vaccines.filename <- vaccines.filename
    self$working.dir <- file.path(tempdir(), "COVID19AR")
    dir.create(self$working.dir, showWarnings = FALSE, recursive = TRUE)
    self$logger <- genLogger(self)
    self
   },
   preprocessCases = function(n.max = 3000000, force.preprocess = FALSE){
    logger <- getLogger(self)
    cases.preprocessed.filepath <- file.path(self$data.dir, "cases_agg.csv")
    if (!file.exists(cases.preprocessed.filepath) | force.preprocess){
     cases.zip.path <- file.path(self$data.dir, gsub("\\.csv", ".zip", self$cases.filename))
     dest.file <- file.path(self$working.dir, self$cases.filename)
     if(file.exists(cases.zip.path) & !file.exists(dest.file)){
      logger$debug("Decompressing", zip.filepath = cases.zip.path)
      unzip(cases.zip.path, junkpaths = TRUE, exdir = self$working.dir)
     }
     dir(self$working.dir)
     cases.file.path <- file.path(self$working.dir, self$cases.filename)
     logger$debug("Loading",
                  cases.df = cases.file.path,
                  n.max = n.max)
     cases.df <- readr::read_csv(cases.file.path, n_max = n.max,
                                      col_types = cols(
                                       id_evento_caso = col_double(),
                                       sexo = col_character(),
                                       edad = col_double(),
                                       edad_años_meses = col_character(),
                                       residencia_pais_nombre = col_character(),
                                       residencia_provincia_nombre = col_character(),
                                       residencia_departamento_nombre = col_character(),
                                       carga_provincia_nombre = col_character(),
                                       fecha_inicio_sintomas = col_date(format = ""),
                                       fecha_apertura = col_date(format = ""),
                                       sepi_apertura = col_double(),
                                       fecha_internacion = col_date(format = ""),
                                       cuidado_intensivo = col_character(),
                                       fecha_cui_intensivo = col_date(format = ""),
                                       fallecido = col_character(),
                                       fecha_fallecimiento = col_date(format = ""),
                                       asistencia_respiratoria_mecanica = col_character(),
                                       carga_provincia_id = col_character(),
                                       origen_financiamiento = col_character(),
                                       clasificacion = col_character(),
                                       clasificacion_resumen = col_character(),
                                       residencia_provincia_id = col_character(),
                                       fecha_diagnostico = col_date(format = ""),
                                       residencia_departamento_id = col_character(),
                                       ultima_actualizacion = col_date(format = "")
                                      ))
     names(cases.df)[4] <- "edad_anios_meses"
     grupos_etarios <- "30-39"

     age.breaks <- c(0, 12, 18, 30, 40, 50, 60, 70, 80, 90, 100, 13)
     age.labels <- c("<12", "12-17", "18-29", "30-39",
                     "40-49", "50-59", "60-69", "70-79", "80-89", "90-99",
                     ">=100")
     self$edad.coder <- EdadCoder$new()
     self$edad.coder$setupCoder(age.breaks = age.breaks,
                                age.labels = age.labels)

     #cases.df %>% select(edad_aC1os_meses)
     cases.df %<>% mutate_cond(edad_anios_meses == "Meses", edad = 0)
     cases.df %<>% mutate(grupo_etario= self$edad.coder$codeEdad(edad))
     #cases.df <- data.table::

     cases.df %<>% mutate_cond(is.na(fecha_inicio_sintomas), fecha_inicio_sintomas = fecha_apertura)
     cases.df %<>% mutate(sepi_fis = as.numeric(as.character(fecha_inicio_sintomas, format = "%W")))
     cases.df %<>% mutate(year_fis = as.numeric(as.character(fecha_inicio_sintomas, format = "%Y")))
     head(cases.df %>% select(year_fis, sepi_fis, fecha_inicio_sintomas, fallecido, cuidado_intensivo, clasificacion))

     cases.df %>% group_by(clasificacion_resumen) %>% summarize(n = n())
     nrow(cases.df)
     cases.df %<>% filter(clasificacion_resumen %in% c("Confirmado", "Descartado"))
     nrow(cases.df)

     self$cases.agg.df <-
      cases.df %>%
      #filter(grupo_etario %in% grupos_etarios) %>%
      group_by(year_fis, sepi_fis, residencia_provincia_nombre,
               residencia_departamento_nombre, grupo_etario) %>%
      summarize(fallecidos = sum(ifelse(fallecido == "SI" & clasificacion_resumen == "Confirmado", 1, 0)),
                cuidados_intensivos = sum(ifelse(cuidado_intensivo == "SI" & clasificacion_resumen == "Confirmado", 1, 0)),
                sospechosos = n(),
                positivos = sum(ifelse(clasificacion_resumen == "Confirmado", 1, 0)),
                descartados = sum(ifelse(clasificacion_resumen == "Descartado", 1, 0)))
     self$cases.agg.df %<>% mutate(residencia_identificada = ifelse(residencia_departamento_nombre == "SIN ESPECIFICAR", 0, 1))
     self$cases.agg.identified <- self$cases.agg.df %>%
      group_by(residencia_provincia_nombre, residencia_identificada) %>%
      summarize(fallecidos = sum(fallecidos),
                cuidados_intensivos = sum(cuidados_intensivos),
                positivos  = sum(positivos),
                sospechosos = sum(sospechosos))
     self$cases.agg.identified
     reshape2::dcast(self$cases.agg.identified, residencia_provincia_nombre ~ residencia_identificada,
                     value.var  = "positivos", fill = 0)

     as.character(Sys.Date(), format = "%W")
     self$cases.agg.df %>% filter(year_fis == 2021 & sepi_fis == 50)
     tail(self$cases.agg.df)

     #self$cases.agg.df %>% filter(residencia_departamento_nombre == "SIN ESPECIFICAR")
     logger$info("Saving cases.agg.df", cases.agg.filepath = cases.preprocessed.filepath)
     write_delim(self$cases.agg.df, file = cases.preprocessed.filepath, delim = ";",
                 )
    }
    else{
     self$cases.agg.df  <- read_delim(file = cases.preprocessed.filepath, delim = ";",
                                      col_types = cols(
                                       year_fis = col_number(),
                                       sepi_fis = col_number(),
                                       residencia_provincia_nombre = col_character(),
                                       residencia_departamento_nombre = col_character(),
                                       grupo_etario = col_character(),
                                       fallecidos = col_number(),
                                       cuidados_intensivos = col_number(),
                                       sospechosos = col_number(),
                                       positivos = col_number(),
                                       descartados = col_number(),
                                       residencia_identificada = col_number()
                                      ))
    }
    self$cases.agg.df
   },
   preprocessVaccines = function(n.max = 3000000, force.preprocess = FALSE){
    logger <- getLogger(self)
    self$data.dir <- "~/../Downloads/"
    vaccines.zip.path <- file.path(self$data.dir, gsub("\\.csv", ".zip", self$vaccines.filename))
    dest.file <- file.path(self$working.dir, self$vaccines.filename)
    vaccines.preprocessed.filepath <- file.path(self$data.dir, "vaccines_agg.csv")
    #dir(self$data.dir)[grep("vaccin", dir(self$data.dir), ignore.case = TRUE)]
    if (!file.exists(vaccines.preprocessed.filepath) | force.preprocess ){
     self$LoadDataFromCovidStats()
     if(file.exists(vaccines.zip.path) & !file.exists(dest.file)){
      logger$debug("Decompressing", zip.filepath = vaccines.zip.path)
      unzip(vaccines.zip.path, junkpaths = TRUE, exdir = self$working.dir)
     }
     #vaccines.file.path <- file.path(self$working.dir, self$vaccines.filename)
     vaccines.file.path <- file.path(self$working.dir, gsub("\\.csv", "_head.csv", self$vaccines.filename))
     logger$debug("Loading",
                  vaccines.file.path = vaccines.file.path,
                  n.max = n.max)
     if (self$use.sqlfd){
      sql.text <- "select strftime('%Y', replace(fecha_aplicacion, '\"', '')) year_fis,
                          strftime('%W', replace(fecha_aplicacion, '\"', '')) week_fis,
                          jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario,
                          count(*) as applications
                   from file
                   group by fecha_aplicacion,
                            strftime('%Y', replace(fecha_aplicacion, '\"', '')),
                            strftime('%W', replace(fecha_aplicacion, '\"', '')),
                            jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario"
      self$vaccines.agg.df <-
           read.csv.sql(vaccines.file.path,
                        #vaccines.zip.path,
                        sql = sql.text, header = TRUE, sep = ",",
                        nrows = n.max)
      names(self$vaccines.agg.df)
      self$vaccines.agg.df <- removeQuotes(self$vaccines.agg.df, fields = c("jurisdiccion_residencia", "depto_residencia", "grupo_etario", "applications"))
      nrow(self$vaccines.agg.df)
      self$vaccines.agg.df[900:910,]
      unique(self$vaccines.agg.df$jurisdiccion_residencia)
      head(self$vaccines.agg.df %>% filter(jurisdiccion_residencia == "CABA" & grupo_etario == "18-29"))
     }

     else{
      vaccines.df <- readr::read_csv(vaccines.file.path,
                                     n_max = n.max,
                                     col_types = cols(
                                      sexo = col_character(),
                                      grupo_etario = col_character(),
                                      jurisdiccion_residencia = col_character(),
                                      jurisdiccion_residencia_id = col_character(),
                                      depto_residencia = col_character(),
                                      depto_residencia_id = col_character(),
                                      jurisdiccion_aplicacion = col_character(),
                                      jurisdiccion_aplicacion_id = col_character(),
                                      depto_aplicacion = col_character(),
                                      depto_aplicacion_id = col_character(),
                                      fecha_aplicacion = col_date(format = ""),
                                      vacuna = col_character(),
                                      cod_dosis_generica = col_double(),
                                      nombre_dosis_generica = col_character(),
                                      condicion_aplicacion = col_character(),
                                      orden_dosis = col_double(),
                                      lote_vacuna = col_character()
                                     ))
      #names(vaccines.df)
      #unique(vaccines.df$grupo_etario)
      vaccines.df %<>% mutate(fecha_inmunidad = fecha_aplicacion)
      vaccines.df %<>% mutate(sepi_fis = as.numeric(as.character(fecha_inmunidad, format = "%W")))
      vaccines.df %<>% mutate(year_fis = as.numeric(as.character(fecha_inmunidad, format = "%Y")))

      self$vaccines.agg.df <- vaccines.df %>%
       #filter(grupo_etario %in% grupos_etarios) %>%
       group_by(year_fis, sepi_fis, jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario) %>%
       summarize(applications = n())
      #Cumsum applications
      self$vaccines.agg.df %<>%
       ungroup(year_fis, sepi_fis) %>%
       mutate(cum.applications = cumsum(applications))

     }

     # self$provincias.departamentos.edad.df %>%
     #  filter(departamento == "Almirante Brown" & grupo == "30-39")
     self$vaccines.agg.df %<>%
      left_join(self$provincias.departamentos.edad.df %>%
                 select(provincia, departamento, grupo, personas),
                by = c(jurisdiccion_residencia = "provincia",
                       depto_residencia = "departamento", grupo_etario = "grupo"))
     self$vaccines.agg.df %<>% mutate(applications.perc = round(cum.applications/personas, 4))

     self$vaccines.agg.df %>% filter(year_fis == 2021 & sepi_fis == 49)
     tail(self$vaccines.agg.df %>%
           filter(year_fis == 2021 & depto_residencia == "Almirante Brown" & orden_dosis == 2) %>%
           select(year_fis, sepi_fis, cum.applications, applications.perc, personas))
     tail(self$vaccines.agg.df)
     #self$cases.agg.df %>% filter(residencia_departamento_nombre == "SIN ESPECIFICAR")
     logger$info("Saving vaccines.agg.df", vaccines.agg.filepath = vaccines.preprocessed.filepath)
     write_delim(self$vaccines.agg.df, file = vaccines.preprocessed.filepath, delim = ";")
    }
    else{
     self$vaccines.agg.df  <- read_delim(file = vaccines.preprocessed.filepath, delim = ";")
    }

   },
   preprocessMinSal = function(n.max = 3000000, force.preprocess = FALSE){
    logger <- getLogger(self)
    #self$loadFiles(n.max = n.max)

    self$preprocessCases(n.max, force.preprocess = force.preprocess)
    self$preprocessVaccines(n.max, force.preprocess = force.preprocess)


   },
   LoadDataFromCovidStats = function(force.download = FALSE){
    logger <- getLogger(self)
    # TODO put in .env
    data.dir <- "~/.R/COVIDAR"
    vacunas.fields <- c("dosis1", "dosis2", "refuerzo", "adicional", "esquemacompleto")
    dir.create(data.dir, showWarnings = FALSE, recursive = FALSE)
    vacunaciones.filepath <- file.path(data.dir, "covidstats-vacunaciones.json")
    if (!file.exists(vacunaciones.filepath) | force.download){
     #download.file("https://covidstats.com.ar/ws/mapa?porprovincia=false&pordepartamento=true", destfile = vacunaciones.filepath)
     download.file("https://covidstats.com.ar/ws/mapa?porprovincia=false&pordepartamento=true&datosadicionales=true", destfile = vacunaciones.filepath)
    }
    if (file.exists(vacunaciones.filepath)){
     vacunaciones.json <- fromJSON(read_file(vacunaciones.filepath))
     logger$info("Building provincias Catalog", nrow = length(vacunaciones.json))
     departamentos.catalog.df <- data.frame(idx = numeric(), codigo = character(),
                                   provincia = character(), idprovincia = numeric(),
                                   iddepartamento = numeric(), departamento = character(), poblacion = numeric())
     for (i in seq_len(length(vacunaciones.json))){
      vacunaciones.departamento.json <- vacunaciones.json[[i]]
      departamento <- unique(vacunaciones.departamento.json$denominacion)
      current.codigo <- unique(vacunaciones.departamento.json$codigo)
      if (is.null(vacunaciones.departamento.json$poblacion)){
       vacunaciones.departamento.json$poblacion <- NA
       logger$warn("No population data for", i = i,
                   current.codigo = current.codigo,
                   departamento = departamento)
      }
      #debug
      vacunaciones.departamento.json <<- vacunaciones.departamento.json
      vacunaciones.departamento.df <- as.data.frame(vacunaciones.departamento.json)
      stopifnot(length(current.codigo) == 1)
      current.provincia.departamento <- data.frame(idx = i, codigo = current.codigo, departamento = unique(vacunaciones.departamento.df$denominacion))
      parsed.codigo <- strsplit(current.codigo, split = "-")[[1]]
      current.provincia.departamento %<>% mutate(idprovincia = parsed.codigo[1])
      current.provincia.departamento %<>% mutate(iddepartamento = parsed.codigo[2])
      current.provincia.departamento %<>% mutate(provincia = "")
      current.provincia.departamento %<>% mutate(poblacion = unique(vacunaciones.departamento.df$poblacion))
      departamentos.catalog.df <- rbind(departamentos.catalog.df, current.provincia.departamento)
     }
    }
    vacunaciones.edad.filepath <- file.path(data.dir, "covidstats-vacunaciones-edad.json")
    if (!file.exists(vacunaciones.edad.filepath) | force.download){
     download.file("https://covidstats.com.ar/ws/vacunadosedades?bot=1", destfile = vacunaciones.edad.filepath)
    }
    if (file.exists(vacunaciones.edad.filepath)){
     vacunaciones.edad.json <- fromJSON(read_file(vacunaciones.edad.filepath))
     vacunaciones.edad.df <- as.data.frame(vacunaciones.edad.json)
    }
    head(vacunaciones.edad.df)
    tail(as.data.frame(vacunaciones.edad.json))

    provincias.df <- vacunaciones.edad.df %>%
                    group_by(idprovincia, provincia) %>%
                    summarize(personas = sum(personas))
    self$provincias.departamentos.edad.df <- NULL
    logger$info("Calculating populations for departamentos")
    for (i in seq_len(nrow(provincias.df))){
     provincia.df <- provincias.df[i,]
     vacunaciones.edad.provincia.df <- vacunaciones.edad.df %>% filter(provincia == provincia.df$provincia)
     logger$debug("Calculating populations for provincia", provincia = provincia.df$provincia, personas = provincia.df$personas)
     departamentos.catalog.df %<>% mutate_cond(idprovincia %in% provincia.df$idprovincia, provincia = provincia.df$provincia)
     vacunaciones.edad.provincia.df %<>% mutate(personas.share = round(personas/sum(vacunaciones.edad.provincia.df$personas), 5))
     provincia.departamentos.df <- departamentos.catalog.df %>% filter(idprovincia == provincia.df$idprovincia)
     provincia.departamentos.edad.df <- NULL
     for (j in seq_len(nrow(provincia.departamentos.df))){
      provincia.departamento.df <- provincia.departamentos.df[j,]
      provincia.departamento.edad.df <- vacunaciones.edad.provincia.df
      provincia.departamento.edad.df %<>% mutate(departamento = provincia.departamento.df$departamento,
                                                 codigo = provincia.departamento.df$codigo)
      provincia.departamento.edad.df %<>% mutate(personas = round(personas.share * provincia.departamento.df$poblacion, 1))
      provincia.departamento.edad.df <- provincia.departamento.edad.df[, - which(names(provincia.departamento.edad.df) %in% vacunas.fields)]
      provincia.departamentos.edad.df <- rbind(provincia.departamentos.edad.df, provincia.departamento.edad.df)
     }
     self$provincias.departamentos.edad.df <- rbind(self$provincias.departamentos.edad.df, provincia.departamentos.edad.df)
    }
    # logger$info("Calculating vaccunations for week")
    # for (i in seq_len(length(vacunaciones.json))){
    #  vacunaciones.departamento.json <- vacunaciones.json[[i]]
    #  departamento <- unique(vacunaciones.departamento.json$denominacion)
    #  current.codigo <- unique(vacunaciones.departamento.json$codigo)
    #  if (is.null(vacunaciones.departamento.json$poblacion)){
    #   vacunaciones.departamento.json$poblacion <- NA
    #  }
    #  vacunaciones.departamento.df <- as.data.frame(vacunaciones.departamento.json)
    #  vacunaciones.departamento.df$date <- as.Date("2020-03-01") + seq_len(nrow(vacunaciones.departamento.df))
    #  nrow(vacunaciones.departamento.df)
    #
    #  tail(vacunaciones.departamento.df)
    #  #debug
    # }
    # for (field in vacunas.fields){
    #  provincia.departamento.edad.df[, field] <- round(provincia.departamento.edad.df[, field]/provincia.departamento.edad.df$personas, 5)
    # }
    #self$pr
   },
   makeEcologicalInference = function(prediction.field = "cuidados_intensivos"){
     year.week.df <- self$vaccines.agg.df %>%
       group_by(year_fis, sepi_fis) %>%
       summarize(applications = sum(applications)) %>%
       arrange(year_fis, sepi_fis)
     ecoinference.df <- NULL
     for (i in seq_len(nrow(year.week.df))){
       current.week.df <- year.week.df[i,]
       names(vaccines.week.df)
       vaccines.week.df <- self$vaccines.agg.df %>%
         filter(year_fis == current.week.df$year_fis, sepi_fis == current.week.df$sepi_fis)
       vaccines.week.df %<>% mutate(provincia_departamento = paste(jurisdiccion_residencia, depto_residencia, sep = "-"))
       vaccines.week.df.not.vaccunated <- vaccines.week.df %>% group_by(provincia_departamento, grupo_etario, personas) %>% summarize(applications.perc = 1- sum(applications.perc))
       vaccines.week.df.not.vaccunated %<>% mutate(orden_dosis = 0)
       vaccines.week.df <- vaccines.week.df %>% bind_rows(vaccines.week.df.not.vaccunated)
       vaccines.week.df %>% select(provincia_departamento)
       vaccines.week.df.tab <- reshape2::dcast(vaccines.week.df, provincia_departamento + grupo_etario ~ orden_dosis, value.var = "applications.perc", fill = 0)
       vaccines.week.df.tab
       tail(vaccines.week.df.tab)
       names(cases.week.df)
       cases.week.df <- self$cases.agg.df %>%
         filter(year_fis == current.week.df$year_fis, sepi_fis == current.week.df$sepi_fis)
       cases.week.df %<>% mutate(provincia_departamento = paste(residencia_provincia_nombre, residencia_departamento_nombre, sep = "-"))
       cases.week.df %<>% ungroup() %>% select(any_of(c("provincia_departamento", "grupo_etario", prediction.field)))
       cases.week.df
       ecological.inference.df <- vaccines.week.df.tab %>% inner_join(cases.week.df,
                                        by = c("provincia_departamento", "grupo_etario"))
       ecological.inference.df
       stop("Under construction")
     }
   }))
