
#' EcologicalInferenceGenerator
#' @author kenarab
#' @importFrom R6 R6Class
#' @import dplyr
#' @export
EcologicalInferenceGenerator <- R6Class("EcologicalInferenceGenerator",
  public = list(
   data.dir     = NA,
   working.dir  = NA,
   cases.filename = NA,
   vaccines.filename = NA,
   edad.coder   = NA,
   cases.df     = NA,
   vaccines.df  = NA,
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
   loadFilesFromMinSal = function(n.max = Inf){
    logger <- getLogger(self)
    cases.zip.path <- file.path(self$data.dir, gsub("\\.csv", ".zip", self$cases.filename))
    dest.file <- file.path(self$working.dir, self$cases.filename)
    if(file.exists(cases.zip.path) & !file.exists(dest.file)){
     logger$debug("Decompressing", zip.filepath = cases.zip.path)
     unzip(cases.zip.path, junkpaths = TRUE, exdir = self$working.dir)
    }

    vaccines.zip.path <- file.path(self$data.dir,gsub("\\.csv", ".zip", self$vaccines.filename))
    dest.file <- file.path(self$working.dir, self$vaccines.filename)
    if(file.exists(vaccines.zip.path) & !file.exists(dest.file)){
     logger$debug("Decompressing", zip.filepath = vaccines.zip.path)
     unzip(vaccines.zip.path, junkpaths = TRUE, exdir = self$working.dir)
    }

    dir(self$working.dir)
    logger$debug("Loading",
                 cases.df = file.path(self$working.dir, cases.filename),
                 n.max = n.max)
    self$cases.df <- readr::read_csv(file.path(self$working.dir, cases.filename), n_max = n.max,
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
    logger$debug("Loading",
                 cases.df = file.path(self$working.dir, vaccines.filename),
                 n.max = n.max)
    self$vaccines.df <- readr::read_csv(file.path(self$working.dir, vaccines.filename), n_max = n.max,
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
    self$preprocessMinSal
   },
   preprocessMinSal = function(n.max = 3000000){
    logger <- getLogger(self)
    #self$loadFiles(n.max = n.max)

    grupos_etarios <- "30-39"

    age.breaks <- c(0, 12, 18, 30, 40, 50, 60, 70, 80, 90, 100, 13)
    age.labels <- c("<12", "12-17", "18-29", "30-39",
                    "40-49", "50-59", "60-69", "70-79", "80-89", "90-99",
                    ">=100")
    self$edad.coder <- EdadCoder$new()
    self$edad.coder$setupCoder(age.breaks = age.breaks,
                          age.labels = age.labels)

    #cases.df %>% select(edad_años_meses)
    self$cases.df %<>% mutate_cond(edad_años_meses == "Meses", edad = 0)
    self$cases.df %<>% mutate(grupo_etario= self$edad.coder$codeEdad(edad))
    #self$cases.df <- data.table::

    self$cases.df %<>% mutate_cond(is.na(fecha_inicio_sintomas), fecha_inicio_sintomas = fecha_apertura)
    self$cases.df %<>% mutate(sepi_fis = as.numeric(as.character(fecha_inicio_sintomas, format = "%W")))
    self$cases.df %<>% mutate(year_fis = as.numeric(as.character(fecha_inicio_sintomas, format = "%Y")))
    head(self$cases.df %>% select(year_fis, sepi_fis, fecha_inicio_sintomas, fallecido, cuidado_intensivo, clasificacion))

    self$cases.df %>% group_by(clasificacion_resumen) %>% summarize(n = n())
    nrow(self$cases.df)
    self$cases.df %<>% filter(clasificacion_resumen %in% c("Confirmado", "Descartado"))
    nrow(self$cases.df)

    self$cases.agg.df <-
     self$cases.df %>%
     filter(grupo_etario %in% grupos_etarios) %>%
     group_by(year_fis, sepi_fis, residencia_provincia_nombre,
              residencia_departamento_nombre, grupo_etario) %>%
     summarize(fallecidos = sum(ifelse(fallecido == "SI" & clasificacion_resumen == "Confirmado", 1, 0)),
               cuidados_intensivos = sum(ifelse(cuidado_intensivo == "SI" & clasificacion_resumen == "Confirmado", 1, 0)),
               sospechosos = n(),
               positivos = sum(ifelse(clasificacion_resumen == "Confirmado", 1, 0)),
               descartados = sum(ifelse(clasificacion_resumen == "Descartado", 1, 0)))
    self$cases.agg.df %<>% mutate(residencia_identificada = ifelse(residencia_departamento_nombre == "SIN ESPECIFICAR", 0, 1))
    self$cases.agg.identified <- cases.agg.df %>%
     group_by(residencia_provincia_nombre, residencia_identificada) %>%
     summarize(fallecidos = sum(fallecidos),
               cuidados_intensivos = sum(cuidados_intensivos),
               positivos  = sum(positivos),
               sospechosos = sum(sospechosos))
    self$cases.agg.identified
    dcast(self$cases.agg.identified, residencia_provincia_nombre ~ residencia_identificada,
          value.var  = "positivos", fill = 0)

    as.character(Sys.Date(), format = "%W")
    cases.agg.df %>% filter(year_fis == 2021 & sepi_fis == 50)
    tail(cases.agg.df)

    cases.agg.df %>% filter(residencia_departamento_nombre == "SIN ESPECIFICAR")

    cases.agg.df %>% filter()
    #https://covidstats.com.ar/ws/mapa?porprovincia=false&pordepartamento=true

    names(vaccines.df)
    unique(vaccines.df$grupo_etario)
    self$vaccines.df %<>% mutate(fecha_inmunidad = fecha_aplicacion + 15)
    self$vaccines.df %<>% mutate(sepi_fis = as.numeric(as.character(fecha_inmunidad, format = "%W")))
    self$vaccines.df %<>% mutate(year_fis = as.numeric(as.character(fecha_inmunidad, format = "%Y")))

    self$vaccines.agg.df <- self$vaccines.df %>%
     filter(grupo_etario %in% grupos_etarios) %>%
     group_by(year_fis, sepi_fis, jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario) %>% summarize(n = n())
    self$vaccines.agg.df %>% filter(year_fis == 2021 & sepi_fis == 49)
    tail(vaccines.agg.df)
    stop("Not yet completed. Data and consolidation is missing")
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
    names(population.json)
    tail(as.data.frame(vacunaciones.edad.json))
    names(as.data.frame(population.json[[1]]))

    provincias.df <- vacunaciones.edad.df %>%
                    group_by(idprovincia, provincia) %>%
                    summarize(personas = sum(personas))
    self$provincias.departamentos.edad.df <- NULL
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
    stop("Under construction")
    for (field in vacunas.fields){
     provincia.departamento.edad.df[, field] <- round(provincia.departamento.edad.df[, field]/provincia.departamento.edad.df$personas, 5)
    }

    self$merged.df


   },
   merge = function(){

   }))
