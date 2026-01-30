init_gee <- function() {
  library(reticulate)
  library(yaml)
  library(here)
  
  local_config <- read_yaml(here("local/user_config.yaml"))
  use_python(local_config$python_path, required = TRUE)
  
  ee <- import("ee")
  ee$Initialize(project = local_config$gee_project)
  
  return(ee)
}