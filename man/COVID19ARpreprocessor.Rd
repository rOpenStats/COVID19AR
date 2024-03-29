% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/eir-lib.R
\name{COVID19ARpreprocessor}
\alias{COVID19ARpreprocessor}
\title{COVID19ARpreprocessor}
\description{
COVID19ARpreprocessor

COVID19ARpreprocessor
}
\author{
kenarab
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-new}{\code{COVID19ARpreprocessor$new()}}
\item \href{#method-preprocessCases}{\code{COVID19ARpreprocessor$preprocessCases()}}
\item \href{#method-preprocessVaccines}{\code{COVID19ARpreprocessor$preprocessVaccines()}}
\item \href{#method-fixVaccinesAgg}{\code{COVID19ARpreprocessor$fixVaccinesAgg()}}
\item \href{#method-makeVaccinesTab}{\code{COVID19ARpreprocessor$makeVaccinesTab()}}
\item \href{#method-preprocessMinSal}{\code{COVID19ARpreprocessor$preprocessMinSal()}}
\item \href{#method-LoadDataFromCovidStats}{\code{COVID19ARpreprocessor$LoadDataFromCovidStats()}}
\item \href{#method-makeEcologicalInference}{\code{COVID19ARpreprocessor$makeEcologicalInference()}}
\item \href{#method-clone}{\code{COVID19ARpreprocessor$clone()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-new"></a>}}
\if{latex}{\out{\hypertarget{method-new}{}}}
\subsection{Method \code{new()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$new(
  data.dir = "~/Downloads/",
  cases.filename = "Covid19Casos.csv",
  vaccines.filename = "datos_nomivac_covid19.csv"
)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-preprocessCases"></a>}}
\if{latex}{\out{\hypertarget{method-preprocessCases}{}}}
\subsection{Method \code{preprocessCases()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$preprocessCases(force.preprocess = FALSE)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-preprocessVaccines"></a>}}
\if{latex}{\out{\hypertarget{method-preprocessVaccines}{}}}
\subsection{Method \code{preprocessVaccines()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$preprocessVaccines(force.preprocess = FALSE)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-fixVaccinesAgg"></a>}}
\if{latex}{\out{\hypertarget{method-fixVaccinesAgg}{}}}
\subsection{Method \code{fixVaccinesAgg()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$fixVaccinesAgg()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-makeVaccinesTab"></a>}}
\if{latex}{\out{\hypertarget{method-makeVaccinesTab}{}}}
\subsection{Method \code{makeVaccinesTab()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$makeVaccinesTab(
  vaccines.agg.covidstats.df,
  dosis.prefix = "orden_dosis"
)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-preprocessMinSal"></a>}}
\if{latex}{\out{\hypertarget{method-preprocessMinSal}{}}}
\subsection{Method \code{preprocessMinSal()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$preprocessMinSal(force.preprocess = FALSE)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-LoadDataFromCovidStats"></a>}}
\if{latex}{\out{\hypertarget{method-LoadDataFromCovidStats}{}}}
\subsection{Method \code{LoadDataFromCovidStats()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$LoadDataFromCovidStats(force.download = FALSE)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-makeEcologicalInference"></a>}}
\if{latex}{\out{\hypertarget{method-makeEcologicalInference}{}}}
\subsection{Method \code{makeEcologicalInference()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$makeEcologicalInference(
  prediction.field = "cuidados_intensivos"
)}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-clone"></a>}}
\if{latex}{\out{\hypertarget{method-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{COVID19ARpreprocessor$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
