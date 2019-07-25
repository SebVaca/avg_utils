# print(Sys.setenv(RWINLIB_LOCAL= "C:/arrow/arrow-0.14.0.zip"))
# 
# Sys.getenv("RWINLIB_LOCAL")
# 
# remotes::install_github("apache/arrow/r")
# 
# library(devtools)
# devtools::install_github("apache/arrow", subdir = "r", ref = "apache-arrow-0.12.0")



library(reticulate)
library(dplyr)
use_python(python = "C:/Users/svaca/AppData/Local/Programs/Python/Python37/pythonw.exe")
use_virtualenv("I:/.virtualenvs/2019sp-final-project-SebVaca-bzDB9anI/")
pandas <- import("pandas")
#use_virtualenv(venv, required = TRUE)

read_parquet_r<- function(path, columns = NULL) {
  
  path <- path.expand(path)
  path <- normalizePath(path)
  
  if (!is.null(columns)) columns = as.list(columns)
  
  xdf <- pandas$read_parquet(path, columns = columns)
  
  xdf <- as.data.frame(xdf, stringsAsFactors = FALSE)
  
  dplyr::tbl_df(xdf)
  
}

read_parquet_r("C:/Users/svaca/Documents/Code_saved/GeneticAlgorithm/Luigi_AvG_workflow/2019sp-final-project-SebVaca/data/AvantGardeDIA_Export_239784e5.parquet")

read_partitioned_parquet_r<- function(parquet_dataset_path, ID_Analyte){
  parquet_dataset_path <- path.expand(parquet_dataset_path)
  parquet_dataset_path <- file.path(parquet_dataset_path, paste0('ID_Analyte=',ID_Analyte))
  
  pq_file = list.files(path = parquet_dataset_path, pattern = ".parquet")
  A <- read_parquet_r(file.path(parquet_dataset_path,pq_file))
  
  return(A)
}

read_partitioned_parquet_r("C:/Users/svaca/Documents/Code_saved/GeneticAlgorithm/Luigi_AvG_workflow/2019sp-final-project-SebVaca/data/pq_ds/", ID_Analyte ="0b59f29e")

# Sys.setenv(RETICULATE_PYTHON = "C:/Users/svaca/.virtualenvs/2019sp-final-project-SebVaca-bzDB9anI/Scripts/python.exe")
# devtools::install_github("hrbrmstr/sparrow")
