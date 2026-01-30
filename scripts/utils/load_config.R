# ==============================================================================
# load_config.R
# Load project config and user-specific local config
# ==============================================================================

load_config <- function() {
  library(yaml)
  library(here)
  
  # Load main config
  config <- read_yaml(here("config.yaml"))
  
  # Load local user config if exists
  local_config_path <- here("local/user_config.yaml")
  if (file.exists(local_config_path)) {
    config$local <- read_yaml(local_config_path)
  }
  
  return(config)
}