
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
   vaccines.dist.dep.last.update.df = NA,
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
   preprocessCases = function(force.preprocess = FALSE){
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
     cases.filepath <- file.path(self$working.dir, self$cases.filename)
     stopifnot(file.exists(cases.filepath))
     logger$debug("Loading",
                  cases.df = cases.filepath)
     cases.df <- readr::read_csv(cases.filepath,
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
   preprocessVaccines = function(force.preprocess = FALSE){
    logger <- getLogger(self)
    #self$data.dir <- "~/../Downloads/"
    vaccines.zip.path <- file.path(self$data.dir, gsub("\\.csv", ".zip", self$vaccines.filename))
    dest.file <- file.path(self$working.dir, self$vaccines.filename)
    vaccines.preprocessed.filepath <- file.path(self$data.dir, "vaccines_agg.csv")
    #dir(self$data.dir)[grep("vaccin", dir(self$data.dir), ignore.case = TRUE)]
    self$LoadDataFromCovidStats()
    if (!file.exists(vaccines.preprocessed.filepath) | force.preprocess ){
     if(file.exists(vaccines.zip.path) & !file.exists(dest.file)){
      logger$debug("Decompressing", zip.filepath = vaccines.zip.path)
      unzip(vaccines.zip.path, junkpaths = TRUE, exdir = self$working.dir)
     }

     vaccines.filepath <- file.path(self$working.dir, self$vaccines.filename)
     stopifnot(file.exists(vaccines.filepath))
     #vaccines.filepath <- file.path(self$working.dir, gsub("\\.csv", "_head.csv", self$vaccines.filename))
     logger$info("Loading",
                  vaccines.filepath = vaccines.filepath)
     if (self$use.sqlfd){
      sepi.fis.df <- self$cases.agg.df %>%
                       filter(year_fis >= 2021 | sepi_fis > 48) %>%
                       group_by(year_fis, sepi_fis) %>% summarize(n = n())
      self$vaccines.agg.df <- generateAggregatedVaccinesWithSqldf(vaccines.filepath, sepi.fis.df, total.chunks = 2,
                                                                  logger = logger)
      #nrow(self$vaccines.agg.df)
      #self$vaccines.agg.df[900:910,]
      #unique(self$vaccines.agg.df$jurisdiccion_residencia)
      self$vaccines.agg.df %<>%
       group_by(jurisdiccion_residencia, depto_residencia, orden_dosis, grupo_etario) %>%
       mutate(cum.applications = cumsum(applications))
      tail(self$vaccines.agg.df %>% filter(jurisdiccion_residencia == "CABA" & grupo_etario == "18-29" & depto_residencia == "COMUNA 1"))
     }
     else{
      # Eliminate quotes
      #read.csv.sql("myfile.csv", filter = 'tr.exe -d \'^"\' ' )
      vaccines.df <- readr::read_csv(vaccines.filepath,
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
     write_delim(self$vaccines.agg.df, file = vaccines.preprocessed.filepath, delim = ";")
    }
    else{
     self$vaccines.agg.df  <- read_delim(file = vaccines.preprocessed.filepath, delim = ";",
                                         col_types = cols(
                                          year_fis = col_number(),
                                          week_fis = col_number(),
                                          jurisdiccion_residencia = col_character(),
                                          depto_residencia = col_character(),
                                          orden_dosis = col_number(),
                                          grupo_etario = col_character(),
                                          applications = col_double(),
                                          cum.applications = col_double()
                                         ))
    }
    vaccines.agg.covidstats.df <- self$fixVaccinesAgg()

    #vaccines.agg.covidstats.df <<- vaccines.agg.covidstats.df
    #stop("under construction")
    vaccines.tab.df <- self$makeVaccinesTab(vaccines.agg.covidstats.df)
    vaccines.agg.covidstats.df <- vaccines.agg.covidstats.df %>%
     left_join(vaccines.people.df,
               by = c("jurisdiccion_checked",
                      "departamento_checked", "grupo_etario"))


    self$provincias.departamentos.edad.df %>% filter(departamento == "COMUNA 1")
    self$vaccines.agg.df %<>% mutate(applications.perc = round(cum.applications/personas, 4))

    self$vaccines.agg.df %>% filter(year_fis == 2021 & sepi_fis == 49)
    tail(self$vaccines.agg.df %>%
          filter(year_fis == 2021 & depto_residencia == "Almirante Brown" & orden_dosis == 2) %>%
          select(year_fis, sepi_fis, cum.applications, applications.perc, personas))
    tail(self$vaccines.agg.df)
    #self$cases.agg.df %>% filter(residencia_departamento_nombre == "SIN ESPECIFICAR")
    logger$info("Saving vaccines.agg.df", vaccines.agg.filepath = vaccines.preprocessed.filepath)

   },
   fixVaccinesAgg = function(){

    # self$provincias.departamentos.edad.df %>%
    #  filter(departamento == "Almirante Brown" & grupo == "30-39")
    # Match departamentos
    vaccines.departamentos <- self$vaccines.agg.df %>% group_by(jurisdiccion_residencia, depto_residencia) %>% summarize(n = n())
    covidstats.departamentos <- self$provincias.departamentos.edad.df %>% group_by(provincia, departamento) %>% summarize(n = n())
    logger$debug("Matching departamentos",
                 vaccines.departamentos = nrow(vaccines.departamentos),
                 covidstats.departamentos = nrow(covidstats.departamentos))
    vaccines.departamentos %<>% mutate(jurisdiccion_checked = jurisdiccion_residencia, departamento_checked = depto_residencia)
    covidstats.departamentos %<>% mutate(jurisdiccion_checked = provincia, departamento_checked = departamento)

    names(covidstats.departamentos)
    anti.join <- vaccines.departamentos %>% anti_join(covidstats.departamentos, by = c("jurisdiccion_checked",
                                                                                       "departamento_checked"))
    anti.join %<>% arrange(-n)

    anti.join %>% group_by(jurisdiccion_checked) %>% summarize(n = n())
    anti.join %>% filter(n > 500)
    #26 rows
    hist(anti.join$n)
    anti.join %>% filter(n > 1000)
    # A tibble: 15 x 5
    # Groups:   jurisdiccion_residencia [10]
    # jurisdiccion_residencia depto_residencia     n jurisdiccion_checked departamento_checked
    # <chr>                   <chr>            <int> <chr>                <chr>
    #  1 S.I.                    S.I.              3489 S.I.                 S.I.
    # 2 CABA                    S.I.              3088 CABA                 S.I.
    # 3 Buenos Aires            Zárate            2916 Buenos Aires         Zárate
    # 4 Buenos Aires            S.I.              2895 Buenos Aires         S.I.
    # 5 Salta                   Cerrillos         2812 Salta                Cerrillos
    # 6 Jujuy                   Palpalá           2602 Jujuy                Palpalá
    # 7 La Rioja                Chilecito         2417 La Rioja             Chilecito
    # 8 Buenos Aires            Pinamar           2316 Buenos Aires         Pinamar
    # 9 Neuquén                 Zapala            2215 Neuquén              Zapala
    # 10 Córdoba                 S.I.              1720 Córdoba              S.I.
    # 11 La Rioja                Chamical          1708 La Rioja             Chamical
    # 12 Mendoza                 S.I.              1537 Mendoza              S.I.
    # 13 Chubut                  Sarmiento         1458 Chubut               Sarmiento
    # 14 Salta                   S.I.              1175 Salta                S.I.
    # 15 La Rioja                Famatina          1060 La Rioja             Famatina

    covidstats.departamentos %>% filter(departamento == "Pinamar")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 1", departamento_checked = "COMUNA 01", jurisdiccion_checked = "CABA")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 12", jurisdiccion_checked = "CABA")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 13", jurisdiccion_checked = "CABA")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 3", departamento_checked = "COMUNA 03", jurisdiccion_checked = "CABA")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 5", departamento_checked = "COMUNA 05", jurisdiccion_checked = "CABA")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 2", departamento_checked = "COMUNA 02")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 4", departamento_checked = "COMUNA 04")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 6", departamento_checked = "COMUNA 06")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 7", departamento_checked = "COMUNA 07")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 8", departamento_checked = "COMUNA 08")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "COMUNA 9", departamento_checked = "COMUNA 09")
    vaccines.departamentos %<>% mutate_cond(depto_residencia == "S.I.", departamento_checked = NA)

    covidstats.departamentos %>% filter(jurisdiccion_checked == "CABA")

    anti.join <- vaccines.departamentos %>% anti_join(covidstats.departamentos, by = c("jurisdiccion_checked",
                                                                                       "departamento_checked"))
    anti.join %>% arrange(-n)
    names(self$vaccines.agg.df)

    vaccines.agg.covidstats.df <- self$vaccines.agg.df %>%
                                   inner_join(vaccines.departamentos, by = c("jurisdiccion_residencia", "depto_residencia"))
    vaccines.agg.covidstats.df %<>% mutate(year_week_fis = paste(year_fis, pad(week_fis, 2), sep = "-"))

    #(vaccines.agg.covidstats.df %>% select(year_week_fis))[10000:10010,]

    provincias.departamentos.edad.df <-
     self$provincias.departamentos.edad.df %>%
     inner_join(covidstats.departamentos, by = c("provincia", "departamento"))

    vaccines.agg.covidstats.df <- vaccines.agg.covidstats.df %>%
     left_join(provincias.departamentos.edad.df %>%
                select(jurisdiccion_checked, departamento_checked, grupo, personas.covidstats = personas),
               by = c("jurisdiccion_checked",
                      "departamento_checked", grupo_etario = "grupo"))
    vaccines.agg.covidstats.df %<>% group_by(jurisdiccion_checked, departamento_checked, orden_dosis, grupo_etario, year_week_fis)



    names(vaccines.agg.covidstats.df)

    vaccines.people.df <- vaccines.agg.covidstats.df %>%
     filter(orden_dosis == 1) %>%
     group_by(jurisdiccion_checked, departamento_checked, grupo_etario) %>%
     summarize(personas.1.dosis = max(cum.applications))

    vaccines.agg.covidstats.df %>% filter(jurisdiccion_checked == "CABA" & grupo_etario == "70-79") %>% arrange(desc(cum.applications))
    vaccines.people.df %>% filter(jurisdiccion_checked == "CABA" & grupo_etario == "70-79")

    vaccines.agg.covidstats.df %<>%
     inner_join(vaccines.people.df,
                by = c("jurisdiccion_checked", "departamento_checked", "grupo_etario"))

    vaccines.dist.dep.last.update.df <- vaccines.agg.covidstats.df %>%
     group_by(jurisdiccion_checked, departamento_checked, orden_dosis, grupo_etario) %>%
     summarize(max_year_week_fis = max(year_week_fis))


    vaccines.agg.region.df <- vaccines.agg.covidstats.df %>%
     filter(!is.na(personas.covidstats) & orden_dosis %in% 1) %>%
     inner_join(vaccines.dist.dep.last.update.df,
                by =c("jurisdiccion_checked", "departamento_checked", "grupo_etario", "orden_dosis", year_week_fis = "max_year_week_fis")) %>%
     group_by(jurisdiccion_checked, departamento_checked, grupo_etario, orden_dosis) %>%
     summarize(cum.applications = max(cum.applications),
               personas.covidstats = max(personas.covidstats),
               cobertura.1.dosis = cum.applications/personas.covidstats)

    vaccines.agg.region.df %<>% mutate(region = "")
    for (jurisdiccion in sort(unique(vaccines.agg.region.df$jurisdiccion_checked))){
     vaccines.agg.region.df %<>% mutate_cond(jurisdiccion_checked == jurisdiccion, region = getRegion(jurisdiccion, ""))
    }
    vaccines.agg.region.df
    region.trim <- 0.3
    vaccines.agg.region.agg.df <- vaccines.agg.region.df %>% group_by(region, grupo_etario) %>%
      summarize(cobertura.1.dosis.trimed = mean(cobertura.1.dosis, trim = region.trim))
    vaccines.agg.region.agg.df %>% filter(region == "PBA")
    vaccines.agg.region.df



    vaccines.agg.provincia.df <- vaccines.agg.covidstats.df %>%
     filter(!is.na(personas.covidstats) & orden_dosis %in% 1:2) %>%
     inner_join(vaccines.dist.dep.last.update.df,
                by =c("jurisdiccion_checked", "departamento_checked", "grupo_etario", "orden_dosis", year_week_fis = "max_year_week_fis")) %>%
     group_by(jurisdiccion_checked, departamento_checked, grupo_etario, orden_dosis) %>%
     summarize(cum.applications = max(cum.applications),
               personas.covidstats = max(personas.covidstats),
               cobertura.1.dosis = cum.applications/personas.covidstats) %>%
     group_by(jurisdiccion_checked, grupo_etario, orden_dosis) %>%
     summarize(,
               n = n())



    #vaccines.dist.dep.last.update.df %>% filter(jurisdiccion_checked == "CABA" & grupo_etario == "70-79")
    #vaccines.dist.dep.last.update.df %>% arrange(max_year_week_fis)

    #vaccines.dist.dep.last.update.df %<>% inner_join(vaccines.agg.covidstats.df %>% select(cum.applications, personas.covidstats),
    #                                                 by = c("jurisdiccion_checked", "departamento_checked", "grupo_etario", "orden_dosis", max_year_week_fis = "year_week_fis"))
    vaccines.agg.provincia.df <- vaccines.agg.covidstats.df %>%
                                  filter(!is.na(personas.covidstats) & orden_dosis %in% 1:2) %>%
                                  inner_join(vaccines.dist.dep.last.update.df,
                                             by =c("jurisdiccion_checked", "departamento_checked", "grupo_etario", "orden_dosis", year_week_fis = "max_year_week_fis")) %>%
                                  group_by(jurisdiccion_checked, departamento_checked, grupo_etario, orden_dosis) %>%
                                    summarize(cum.applications = max(cum.applications),
                                              personas.covidstats = max(personas.covidstats)) %>%
                                  group_by(jurisdiccion_checked, grupo_etario, orden_dosis) %>%
                                  summarize(cobertura.1.dosis = sum(cum.applications)/sum(personas.covidstats),
                                            n = n())
    #vaccines.agg.provincia.df %>% filter(jurisdiccion_checked == "CABA") %>% arrange(desc(cobertura.1.dosis))
    vaccines.agg.provincia.df %>% arrange(cobertura.1.dosis)
    (vaccines.agg.provincia.df %>% arrange(desc(cobertura.1.dosis)))[35+0:10,]
    hist(as.data.frame(vaccines.agg.provincia.df)[,"cobertura.1.dosis"], breaks = 100)


    vaccines.agg.covidstats.df %>% filter(jurisdiccion_checked == "CABA" & grupo_etario == "70-79") %>%
     inner_join(vaccines.dist.dep.last.update.df,
                by =c("jurisdiccion_checked", "departamento_checked", "grupo_etario", "orden_dosis", year_week_fis = "max_year_week_fis")) %>%
     select(covidstats = personas.covidstats, dosis_1 = personas.1.dosis, cum.applications)


    vaccines.agg.covidstats.df %<>% mutate(personas.estimated = max(personas.covidstats, personas.1.dosis, na.rm = TRUE))
    #tail(vaccines.agg.covidstats.df %>% filter(is.na(personas.estimated)) %>% select("personas.covidstats", "personas.1.dosis", "personas.estimated"))
    vaccines.agg.covidstats.df %>% filter(personas.covidstats < personas.1.dosis) %>%
     select(prov = jurisdiccion_checked, dep = departamento_checked, ge = grupo_etario,
            personas.covidstats, personas.1.dosis, personas.estimated)

    vaccines.agg.covidstats.df
   },
   makeVaccinesTab = function(vaccines.agg.covidstats.df, dosis.prefix = "orden_dosis"){
    # vaccines.df <- self$vaccines.agg.df %>%
    #  group_by(year_fis, week_fis, jurisdiccion_checked, departamento_checked, grupo_etario, orden_dosis) %>%
    #  summarize(applications = max(cum.applications))
    names(self$vaccines.agg.df)
    vaccines.tab.df <- dcast(vaccines.agg.covidstats.df,
                             formula = year_fis + week_fis + jurisdiccion_checked +  departamento_checked+  grupo_etario + personas.estimated ~ orden_dosis ,
                             value.var = "cum.applications", fill = 0, fun.aggregate = sum)
    dosis <- 1:(ncol(vaccines.tab.df)-6)
    vaccines.tab.df[, paste(dosis.prefix, 0, sep = "_")] <- vaccines.tab.df[, "personas.estimated"]
    names(vaccines.tab.df)[6+dosis] <- paste(dosis.prefix, dosis, sep = "_")
    vaccines.perc.tab.df <- vaccines.tab.df
    dosis.vacunated <- rep(0, nrow(vaccines.tab.df))
    for (orden.dosis in rev(c(0, dosis))){
     dosis.field <- paste(dosis.prefix, orden.dosis, sep = "_")
     logger$debug("Calculating", field = dosis.field)
     if (orden.dosis <=2){
      vaccines.tab.df[, dosis.field] <- vaccines.tab.df[, dosis.field] - dosis.vacunated
     }
     dosis.errors <- which(vaccines.tab.df[, dosis.field] < -0)
     vaccines.error <- vaccines.tab.df[dosis.errors,] %>%
      group_by(across(c("jurisdiccion_checked", "departamento_checked", "grupo_etario"))) %>%
      summarize(dosis.error = max(across(c(dosis.field))))
     #vaccines.error %<> mutate(orden_dosis = orden.dosis)
     vaccines.tab.df[dosis.errors, dosis.field] <- 0
     vaccines.perc.tab.df[, dosis.field] <- round(vaccines.tab.df[, dosis.field] / vaccines.perc.tab.df[,"personas.estimated"], 5)
     dosis.vacunated <- dosis.vacunated + vaccines.tab.df[, dosis.field]
    }
    vaccines.tab.df[, dosis.field]
    tail(vaccines.tab.df)
    kable(tail(vaccines.perc.tab.df, n = 1))
    vaccines.tab.df
   },
   preprocessMinSal = function(force.preprocess = FALSE){
    logger <- getLogger(self)
    self$preprocessCases(force.preprocess = force.preprocess)
    self$preprocessVaccines(force.preprocess = force.preprocess)
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
      length.data <- lapply(vacunaciones.departamento.json, FUN = length)
      data.length <- unlist(unique(length.data))
      data.length <- data.length[data.length > 1]
      data.length <- min(data.length)
      vacunaciones.departamento.cleaned.json <- lapply(vacunaciones.departamento.json, FUN = function(x)x[seq_len(data.length)])
      length.data <- lapply(vacunaciones.departamento.cleaned.json, FUN = length)
      vacunaciones.departamento.df <- as.data.frame(vacunaciones.departamento.cleaned.json)
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


#' EcologicalInferenceCalvo
#' @import foreign
#' @import boot
#' @import networkD3
#' @import webshot
#' @export
EcologicalInferenceCalvo <- R6Class("EcologicalInferenceCalvo",
  public = list(
   data.folder = NA,
   input.data = NA,
   output.data = NA,
   location.field = NA,
   votes.field = NA,
   seed = NA,
   # internal
   estsPG = NA,
   fracsPG = NA,
   # output
   output.table = NA,
   output = NA,
   sankey.network = NA,
   logger = NA,
   initialize = function(input.data,
                         output.data,
                         location.field = "COD_ZONA",
                         seed = 143324) {
    self$data.folder    <- data.folder
    self$input.filename <- input.filename
    self$scenario       <- scenario
    self$location.field <- location.field
    self$seed           <- seed
    self$data.folder.s3 <- file.path(data.folder, "s3")
    dir.create(self$data.folder.s3, showWarnings = FALSE, recursive = TRUE)
    self$data.folder.d3 <- file.path(data.folder, "d3")
    dir.create(self$data.folder.d3, showWarnings = FALSE, recursive = TRUE)
    self$go.s3.bucket.postprocessor <- Elecciones2021PostprocessorS3BucketSync$new(
     local.path = self$data.folder.s3,
     remote.url = "s3://interactivo.pagina12.com.ar/andy/ein-scenarios"
    )
    self$logger <- genLogger(self)
    self
   },
   wallTime = function(expr) {
    system.time(expr)[3]
   },
   # CALL.DIFP
   # Calculates penalty for given parameters
   # p     - parameter vector R x (C-1)
   # mx    - Column marginals
   # my    - row marginals
   # nR    - number of rows
   # nC    - number of columns
   # nP    - number of precincts
   # const - weight for penalty
   callDifp = function(p, mx, my, covar, nR, nC, nP, const) {
    # debug1
    # p <<- p
    # mx <<- mx
    # my.debug <<- my
    # covar <<- covar
    # nR <<- nR
    # nC <<- nC
    # nP <<- nP
    # const <<- const
    # mx[which(is.na(mx[,1])),]

    pen <- 0
    d <- seq(from = 0, to = 0, length = nR * (nC - 1))
    g <- p[1:(nR * (nC - 1))]
    if (is.numeric(covar)) {
     d <- p[(nR * (nC - 1) + 1):(2 * nR * (nC - 1))]
     gamma <- array(0, c(nR, nC - 1, nP))
     diff <- 0
     for (i in 1:nP) {
      temp <- 0
      if (is.numeric(covar)) temp <- covar[i]
      gamma[, , i] <- matrix(data = g + temp * d, nrow = nR, ncol = nC - 1, byrow = T)
      expo <- exp(gamma[, , i])
      if (nC != 2) {
       ebeta <- exp(gamma[, , i]) / (1 + apply(exp(gamma[, , i]), 1, sum))
      } else {
       ebeta <- exp(gamma[, , i]) / (1 + exp(gamma[, , i]))
      }
      yhat <- mx[i, ] %*% ebeta
      diff <- diff + sum((yhat - my[i, -C])^2)
      # diff <- diff + sum((yhat-my[i,-nC])^2) + (const*sum(gamma[,,i]^2));
      # diff <- diff + sum((yhat-my[i,-C])^2) + (10000*sum(gamma[,,i]^2));
     }
    } else {
     # debug
     # print(paste(nR, nC))
     gamma <- matrix(data = g, nrow = nR, ncol = nC - 1, byrow = T)
     expo <- exp(gamma)
     ebeta <- exp(gamma) / (1 + apply(exp(gamma), 1, sum))
     yhat <- mx %*% ebeta
     diff <- sum((yhat - my[, -nC])^2) + (const * sum(gamma^2))
     # is.numeric(my[483,])
    }
    return(diff)
   },
   # Ecological Inference in the RxC case
   # Penalized Least Square Minimizer
   # PARAMS.ESTIM
   # Estimates parameters minimizing the penalized least squares criterion
   # x       - index (optional, for bootstrapping)
   # data    - marginals (optionally with covariates)
   # nR      - number of rows
   # nC      - number of columns
   # const   - weight for penalty
   # parSeed - Seed for parameters (optional)
   paramsEstim = function(data, x = -1, nR, nC, const = 0.001, parSeed = -1) {
    if (x[1] == -1) x <- 1:nrow(data)

    mx <- data[x, 1:nR]
    my <- data[x, (nR + 1):(nR + nC)]
    nP <- nrow(data)
    covar <- F
    if (ncol(data) > nR + nC) {
     covar <- data[x, nR + nC + 1]
     if (parSeed[1] == -1) {
      parSeed <- rnorm(2 * nR * (nC - 1))
     }
    } else {
     if (parSeed[1] == -1) {
      parSeed <- rnorm(nR * (nC - 1))
     }
    }
    fit <- optim(parSeed, fn = self$callDifp, method = "L-BFGS-B", covar = covar, nR = nR, nC = nC, nP = nP, mx = mx, my = my, const = const)
    # , method="L-BFGS-B", method="SANN"
    return(fit$par)
   },
   # Calculate Fractions
   # CALC.FRACTIONS
   # Calculate fractions from the parameters
   # p     - parameters
   # nR    - number of rows
   # nC    - number of columns
   # covar - (Optional) Vector of covariates
   calcFractions = function(p, nR, nC, covar = F) {
    d <- seq(from = 0, to = 0, length = nR * (nC - 1))
    g <- p[1:(nR * (nC - 1))]
    if (is.numeric(covar)) {
     nP <- length(covar)
     ests <- array(0, c(nR, nC, nP))
     d <- p[(nR * (nC - 1) + 1):(2 * nR * (nC - 1))]
     for (i in 1:nP) {
      p.exp <- exp(g + d * covar[i])
      p.matrix <- matrix(p.exp, nrow = nR, byrow = T)
      p.sums <- apply(p.matrix, 1, sum)
      p.sums <- p.sums + 1
      p.less <- p.matrix / p.sums
      ests[, , i] <- cbind(p.less, 1 - apply(p.less, 1, sum))
     }
    } else {
     p.exp <- exp(g)
     p.matrix <- matrix(p.exp, nrow = nR, byrow = T)
     p.sums <- apply(p.matrix, 1, sum)
     p.sums <- p.sums + 1
     p.less <- p.matrix / p.sums
     ests <- cbind(p.less, 1 - apply(p.less, 1, sum))
    }
    return(ests)
   },
   # Bootstrapping
   # PARAMS.BOOT
   # data        - marginals (optionally, with covariates)
   # nR          - number of rows
   # nC          - number of columns
   # bootSamples - number of bootstrap samples
   paramsBoot = function(data, nR, nC, bootSamples) {
    output <- boot(data = data, statistic = self$paramsEstim, R = bootSamples, nR = nR, nC = nC)
    return(output)
   },
   generateNormalizedOutput = function( indicator = "perc"){
    allowed.indicators <- c("perc", "count")
    stopifnot(indicator %in% allowed.indicators)
    ret <- data.frame(source = character(), target = character(), value = numeric())
    input.names <- rownames(self$output.table)
    input.names <- input.names[seq_len(length(input.names) - 1)]
    output.names <- colnames(self$output.table)
    output.names <- output.names[seq_len(length(output.names) - 1)]
    rows <- seq_len(length(input.names))
    cols <- seq_len(length(output.names))
    total.votes <- sum(self$output.table[rows, cols])
    c <- 1
    for (i in rows){
     for (j in cols){
      value <- self$output.table[i, j]
      if (indicator == "perc"){
       value <- round(value / total.votes * 100, 2)
      }
      ret[c,] <- c(input.names[i], output.names[j], value)
      c <- c + 1
     }
    }
    ret
   },
   makeSankeyDiagram = function(save.output = TRUE){
    logger <- getLogger(self)
    # A connection data frame is a list of flows with intensity for each flow
    links <- self$generateNormalizedOutput(indicator = "perc")
    nodes <-  data.frame(
     name=c(as.character(links$source),
            as.character(links$target)) %>% unique())

    # From these flows we need to create a node data frame: it lists every entities involved in the flow
    nodes <- data.frame(cat = "s", name = unique(links$source))
    nodes <- rbind(nodes, data.frame(cat = "t", name = unique(links$target)))
    nodes %<>% mutate(id = paste(cat, name, sep = "_"))

    # With networkD3, connection must be provided using id, not using real name like in the links dataframe.. So we need to reformat it.
    links$IDsource <- match(paste("s", links$source, sep = "_"), nodes$id)-1
    links$IDtarget <- match(paste("t", links$target, sep = "_"), nodes$id)-1
    logger$info("Generating sankeyNetwork", nodes = nrow(nodes), links = nrow(links))
    p <- sankeyNetwork(Links = links, Nodes = nodes,
                       Source = "IDsource", Target = "IDtarget",
                       Value = "value", NodeID = "name",
                       iterations = 3,
                       sinksRight = TRUE,
                       fontSize = 18)
    self$sankey.network <- p
    if (save.output){
     tmp.path <- file.path(tempdir(), "d3")
     dir.create(tmp.path, showWarnings = FALSE, recursive = TRUE)
     tmp.html.filepath <- file.path(tmp.path, "sn.html")
     saveNetwork(p, tmp.html.filepath)

     sankey.d3.png.filepath <- file.path(
      self$data.folder.d3,
      self$getFilename("chile-eir-sankey", extension = "png")
     )
     sankey.d3.png.filepath <<- sankey.d3.png.filepath
     write_delim(links, file = gsub("\\.png", ".csv", sankey.d3.png.filepath))
     # you convert it as png
     file.exists(tmp.html.filepath)
     webshot::webshot(tmp.html.filepath, sankey.d3.png.filepath, vwidth = 1000, vheight = 900)
    }
    p
   },
   getFilename = function(file.prefix, extension = "csv") {
    paste(file.prefix, "-scen-", self$scenario, "-s-", self$seed, ".", extension, sep = "")
   },
   generateOutputJSON = function(save.output = TRUE, upload.s3 = FALSE) {
    self$generateOutput()
    eir.json <- jsonlite::toJSON(self$output, pretty = TRUE)
    eir.json <- gsub("\\[|\\]", "", eir.json)
    eir.json <- gsub("\\\"|\\\"", "", eir.json)
    eir.json <- paste("var poll = ",
                      eir.json,
                      ";\n",
                      sep = ""
    )
    json.filepath <- file.path(
     self$data.folder.s3,
     self$getFilename("chile-eir", extension = "js")
    )
    write_file(x = eir.json, file = json.filepath, append = FALSE)
    if (upload.s3) {
     self$sync.result <- self$syncS3()
    }
    eir.json
   },
   syncS3 = function() {
    self$go.s3.bucket.postprocessor$postprocess()
   },
   getBetab = function() {
    betab <- self$output.table[
     seq_len(nrow(self$output.table) - 1),
     seq_len(ncol(self$output.table) - 1)
    ]
    betab <- as.data.frame(betab)
    total.cols <- ncol(betab)
    cols <- seq_len(total.cols)
    for (j in cols) {
     betab[, total.cols + j] <- 0
     names(betab)[total.cols + j] <- paste(names(betab)[j], "perc", sep = "_")
    }
    betab
    for (i in seq_len(nrow(betab))) {
     betab[i, total.cols + cols] <- round(betab[i, cols] / sum(betab[i, cols]), 3)
    }
    betab$source <- rownames(betab)
    betab$scenario <- self$scenario
    betab <- betab[, c("scenario", "source", names(betab)[seq_len(2 * total.cols)])]
    betab
   },
   exportBetab = function(output.folder = self$data.folder.s3, overwrite = FALSE) {
    betab <- self$getBetab()
    dir.create(output.folder, showWarnings = FALSE, recursive = TRUE)
    betab.filepath <- file.path(
     output.folder,
     self$getFilename("eir-betab-esc", extension = "csv")
    )
    # betab.filepath <<- betab.filepath
    if (!file.exists(betab.filepath) | overwrite) {
     write_delim(betab, file = betab.filepath, delim = ";")
    }
   },
   runChile2021WithProcessor = function(servel.processor,
                                        download.last.snapshot = TRUE,
                                        include.blancos = TRUE, include.ausentes = TRUE) {
    logger <- getLogger(self)
    if (download.last.snapshot) {
     servel.processor$processVotacionesLastSnapshot()
    }
    servel.processor$parseRetrieved()
    # Ballotage data
    self$output.data <- servel.processor$getResultsPivoted("4", output.format = "perc-total")
    self$loadInputChile2021PivotCandidatos()
    self$runChile(include.blancos = include.blancos, include.ausentes = include.ausentes)
   },
   convertShares2Votes = function(election.df){
    share.fields <- self$getSharesFields(names(election.df))
    for (share.field in share.fields){
     election.df[, share.field] <- election.df[, share.field] * election.df[, self$votes.field]
    }
    election.df
   },
   convertVotes2Shares = function(election.df){
    share.fields <- self$getSharesFields(names(election.df))
    total.votes <- rowSums(election.df[, share.fields])
    votes.rows <- which(total.votes > 0)
    for (share.field in share.fields){
     election.df[votes.rows, share.field] <- election.df[votes.rows, share.field] / total.votes[votes.rows]
     if (length(votes.rows) < nrow(election.df)){
      #Empty votes
      # Do nothing for empty votes
     }
    }
    election.df
   },
   getSharesFields = function(election.fields){
    share.fields <- election.fields
    share.fields <- setdiff(share.fields, c(self$location.field, self$votes.field))
    not.candidate.fields <- c("blanco_y_nulo", "ausente", "ausente-1", "ausente-2")
    candidate.fields <- setdiff(share.fields, not.candidate.fields)
    not.candidate.fields <- sort(intersect(not.candidate.fields, share.fields), decreasing = TRUE)
    ret <- c(sort(candidate.fields), not.candidate.fields)
    ret

   },
   runChile = function(include.blancos = TRUE, include.ausentes = TRUE) {
    logger <- getLogger(self)
    logger$info("Setting seed", seed = self$seed)
    set.seed(self$seed)
    input.votes.col <- ncol(self$input.data)
    output.votes.col <- ncol(self$output.data)
    self$input.data <- self$convertShares2Votes(self$input.data)
    self$output.data <- self$convertShares2Votes(self$output.data)
    if (include.ausentes){
     input.ausente.col <- input.votes.col
     input.votes.col   <- input.votes.col + 1
     self$input.data[, input.votes.col] <- self$input.data[, input.ausente.col]
     self$input.data[, input.ausente.col] <- 0
     names(self$input.data)[input.ausente.col:input.votes.col] <- c("ausente", "votos")
     output.ausente.col <- output.votes.col
     output.votes.col   <- output.votes.col + 1
     self$output.data[, output.votes.col] <- self$output.data[, output.ausente.col]
     self$output.data[, output.ausente.col] <- 0
     names(self$output.data)[output.ausente.col:output.votes.col] <- c("ausente", "votos")
     total.votes.df <- cbind(self$input.data[, input.votes.col], self$output.data[, output.votes.col])
     names(total.votes.df) <- c("input.votes", "output.votes")
     total.votes.df[, "min.votes"] <- apply(total.votes.df, MARGIN = 1, FUN = min)
     total.votes.df <- as.data.frame(total.votes.df %>% select(min.votes))
     # Missing in input is calculated from output
     self$input.data[, input.ausente.col] <- self$output.data[, output.votes.col] -  total.votes.df[,1]
     # Missing in output is calculated from input
     self$output.data[, output.ausente.col] <- self$input.data[, input.votes.col] -  total.votes.df[,1]
    }
    #input.shares.fields <- names(self$input.data)[2:input.ausente.col]
    #output.shares.fields <- names(self$output.data)[2:output.ausente.col]
    input.shares.fields <- self$getSharesFields(election.fields = names(self$input.data))
    output.shares.fields <- self$getSharesFields(election.fields = names(self$output.data))
    if (!include.blancos){
     input.shares.fields <- input.shares.fields[input.shares.fields != "blanco_y_nulo" ]
     output.shares.fields <- output.shares.fields[output.shares.fields != "blanco_y_nulo" ]
    }
    # input.data <- self$input.data
    # dsPpre.empty.rows <- apply(input.data[, 2:8], MARGIN = 1, FUN = function(x) max(is.na(x)))
    # input.data[which(dsPpre.empty.rows != 0), ]
    # if (length(dsPpre.empty.rows) > 0) {
    #   logger$warn("COD_ZONA in input sums 0 votes!",
    #     zonas = paste(input.data[which(dsPpre.empty.rows != 0), ]$COD_ZONA, collapse = ", ")
    #   )
    #   input.data <- input.data[which(dsPpre.empty.rows == 0), ]
    # }
    # dsPpre.zones <- input.data$COD_ZONA
    # dsPpre <- input.data[, 2:8]

    dsPpre.zones <- self$input.data[, self$location.field]
    # input
    dsPpre <- self$input.data[, input.shares.fields]
    dsPpre <- self$convertVotes2Shares(dsPpre)
    dsPpre <- cbind(dsPpre, 1 - rowSums(dsPpre))
    colnames(dsPpre)
    dsPpre <- as.matrix(dsPpre)
    dsPpre <<- dsPpre
    # Emtpy rows
    input.check.col <- ncol(dsPpre)
    which(dsPpre[,input.check.col] == 1)
    dsPpre[which(self$input.data[, self$votes.field] == 0),]

    # output
    dsGpre <- self$output.data[, output.shares.fields]
    dsGpre <- self$convertVotes2Shares(dsGpre)
    dsGpre <- cbind(dsGpre, 1 - rowSums(dsGpre))
    #dsGpre %<>% filter(COD_ZONA %in% input.data$COD_ZONA)
    colnames(dsGpre)
    dsGpre <- as.matrix(dsGpre)
    output.check.col <- ncol(dsGpre)
    which(dsGpre[,output.check.col] == 1)

    nR <- ncol(dsPpre)
    nC <- ncol(dsGpre)


    # dsPpre.empty.rows <- apply(dsPpre, MARGIN = 1, FUN = function(x) max(is.na(x)))
    # stopifnot(length(dsPpre.empty.rows[dsPpre.empty.rows != 0]) == 0)
    #
    # dsGpre.empty.rows <- apply(dsGpre, MARGIN = 1, FUN = function(x) max(is.na(x)))
    # stopifnot(length(which(dsGpre.empty.rows != 0)) == 0)
    # dsGpre[which(dsGpre.empty.rows != 0), ]

    # General a Ballotage
    logger$debug("newdata = dsPpre+dsGpre",
                 nrow.dsPpre = nrow(dsPpre),
                 nrow.dsGpre = nrow(dsGpre)
    )
    newdata <- as.matrix(cbind(dsPpre, dsGpre))
    nd.input.check.col <- input.check.col
    nd.output.check.col<- nR + output.check.col
    ncol(newdata)
    # Empty rows
    empty.rows <- which(newdata[,nd.input.check.col] == 1 | newdata[,nd.output.check.col] == 1 )
    if (length(empty.rows) > 0){
     newdata <- newdata[-empty.rows,]
    }
    logger$info("ParamsEstim", nR = nR, nC = nC)

    self$estsPG <- self$paramsEstim(newdata, nR = nR, nC = nC)
    logger$info("calcFractions")
    self$fracsPG <- self$calcFractions(self$estsPG, nR = nR, nC = nC)
    colnames(self$fracsPG) <- colnames(dsGpre)
    rownames(self$fracsPG) <- colnames(dsPpre)

    ## Datos Summary
    # VotosPaso <- data1[,7:16]
    #cols.general <- ncol(input.data)
    totals.general <- self$input.data[, self$votes.field]
    #VotosGeneral <- self$input.data[, c(2:(cols.general - 1))]
    VotosGeneral <- self$input.data[, input.shares.fields]
    # for (c in 1:(ncol(VotosGeneral))) {
    #   VotosGeneral[, c] <- VotosGeneral[, c] * totals.general
    # }
    VotosGeneral <- cbind(VotosGeneral, round(totals.general - rowSums(VotosGeneral), 3))

    #cols.ballotage <- ncol(self$output.data)

    #VotosBallotage <- self$output.data[, c(2:3, cols.ballotage)]
    VotosBallotage <- self$output.data[, output.shares.fields]
    #totals.ballotage <- apply(VotosBallotage, MARGIN = 1, FUN = sum)
    totals.ballotage <- rowSums(VotosBallotage)
    VotosBallotage <- cbind(VotosBallotage, totals.ballotage - rowSums(VotosBallotage))
    colnames(VotosGeneral) <- colnames(dsPpre)
    colnames(VotosBallotage) <- colnames(dsGpre)
    self$output.table <- round(self$fracsPG * colSums(VotosGeneral), 0)
    self$output.table
   }
  )
)

#' getRegion
#' @export
getRegion <- function(provincia, departamento){
 ret <- NULL
 if (provincia %in% c("Buenos Aires", "CABA")){
   ret <- "PBA"
 }
 if (provincia %in% c("Chaco", "Formosa", "Corrientes", "Misiones")){
  ret <- "NEA"
 }
 if (provincia %in% c("Catamarca", "Salta", "Santiago del Estero", "Jujuy", "Tucumán")){
  ret <- "NOA"
 }
 if (provincia %in% c("La Pampa", "Chubut", "Río Negro", "Tierra del Fuego", "Santa Cruz", "Neuquén")){
  ret <- "PAT"
 }
 if (provincia %in% c("Córdoba", "Santa Fe", "Entre Ríos")){
  ret <- "CEN"
 }
 if (provincia %in% c("La Rioja", "Mendoza", "San Juan", "San Luis")){
  ret <- "CUY"
 }
 stopifnot(!is.null(ret))
 ret
}



