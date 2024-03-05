library(tidyverse)
library(lubridate)

source('R/fct_awss3Connect_sensorcode.R')
source('R/fct_awss3Connect.R')
source('R/collect_profile_targets.R')

lake_directory <- here::here()
config_set_name <- "default"
forecast_site <- c("CANN")
#configure_run_file <- paste0("configure_run_",forecast_site,".yml")
configure_run_file <- "configure_run.yml"
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

config_obs <- FLAREr::initialize_obs_processing(lake_directory, 
                                                observation_yml = "observation_processing.yml", 
                                                config_set_name = config_set_name)


## COLLECT SENSOR DATA FROM INFLOW
##### NO SENSOR CODES FROM UPSTREAM FOUND YET ###

## COLLECT PROFILE DATA FROM INFLOW
rawwiski <- awss3Connect(filename = 'arms/wiski.csv')

# test objects
#wiski_data <- rawwiski

## pull sites upstream of the Kent St weir
#sites <- c('KEN', 'BAC', 'KS7', 'NIC', 'ELL')
inflow_sites <- c('YULEB', 'STHNR', 'CANNR')

inflow_profile_data <- collect_profile_targets(sites = inflow_sites, 
                                          wiski_data = rawwiski)