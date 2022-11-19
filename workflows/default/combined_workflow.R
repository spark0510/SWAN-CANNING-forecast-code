readRenviron("~/.Renviron") # MUST come first
library(tidyverse)
library(lubridate)
lake_directory <- here::here()
setwd(lake_directory)
forecast_site <<- "ALEX"
configure_run_file <<- "configure_run.yml"
update_run_config <<- TRUE
config_set_name <<- "default"

message("Checking for NOAA forecasts")
noaa_ready <- FLAREr::check_noaa_present_arrow(lake_directory,
                                               configure_run_file,
                                               config_set_name = config_set_name)

if(!noaa_ready){
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)
  lapsed_time <- as.numeric(as.duration(Sys.time() - lubridate::as_datetime(config$run_config$forecast_start_datetime)))/(60*60)
  if(lapsed_time > 24){
    FLAREr::update_run_config(config, lake_directory, configure_run_file, saved_file = NA, new_horizon = NA, day_advance = 1, new_start_datetime = FALSE)
  }
}

config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)


if(noaa_ready){

  message("Generating targets")
  source(file.path("workflows", config_set_name, "01_generate_targets.R"))
  
  setwd(lake_directory)
  
  message("Generating inflow forecast")
  #source(file.path("workflows", config_set_name, "02_run_inflow_forecast.R"))
  
  setwd(lake_directory)
  
  message("Generating forecast")
  source(file.path("workflows", config_set_name, "03_run_flarer_forecast.R"))
  
  setwd(lake_directory)
  
  RCurl::url.exists("https://hc-ping.com/31c3e142-8f8c-42ae-9edc-d277adb94b31", timeout = 5)
  
  
}
