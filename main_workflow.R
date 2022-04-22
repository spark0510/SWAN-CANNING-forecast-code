#!/usr/bin/env Rscript
library(tidyverse)
library(lubridate)

args <- commandArgs(trailingOnly=TRUE)
# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  config_set_name <- "default"
  workflow_scripts <- 0
  configure_run_file <- "configure_run.yml"
} else if(length(args)==1){
  config_set_name <- args[1]
  workflow_scripts <- 0
  configure_run_file <- "configure_run.yml"
}else if(length(args)==2){
  config_set_name <- args[1]
  workflow_scripts <- args[2]
  configure_run_file <- "configure_run.yml"
} else{
  config_set_name <- args[1]
  workflow_scripts <- args[2]
  configure_run_file <- args[3]
}

lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "fcre"
update_run_config <- TRUE

FLAREr::set_configuration(configure_run_file = configure_run_file,
                          lake_directory = lake_directory,
                          config_set_name = config_set_name)

message("Checking for NOAA forecasts")

noaa_ready <- FLAREr::check_noaa_present(lake_directory,
                                         configure_run_file,
                                         config_set_name)

if(noaa_ready){

  if(workflow_scripts == 0){

    message("Generating targets")
    source(file.path(lake_directory,"workflows", config_set_name, "01_generate_targets.R"))

    setwd(lake_directory)

    message("Generating inflow forecast")
    source(file.path(lake_directory,"workflows", config_set_name, "02_run_inflow_forecast.R"))

    setwd(lake_directory)

    message("Generating forecast")
    source(file.path(lake_directory,"workflows", config_set_name, "03_run_flarer_forecast.R"))

    setwd(lake_directory)

    message("Generating plots")
    source(file.path(lake_directory,"workflows", config_set_name, "04_visualize.R"))

  }else if(workflow_scripts == 1){
    message("Generating targets")
    source(file.path(lake_directory,"workflows", config_set_name, "01_generate_targets.R"))
  }else if(workflow_scripts == 2){
    message("Generating inflow forecast")
    source(file.path(lake_directory,"workflows", config_set_name, "02_run_inflow_forecast.R"))
  }else if(workflow_scripts == 3){
    message("Generating forecast")
    source(file.path(lake_directory,"workflows", config_set_name, "03_run_flarer_forecast.R"))
  }else if(workflow_scripts == 4){
    message("Generating plots")
    source(file.path(lake_directory,"workflows", config_set_name, "04_visualize.R"))
  }
}
