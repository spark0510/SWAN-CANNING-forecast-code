library(tidyverse)
library(tidymodels)
library(xgboost)

source('R/fct_awss3Connect_sensorcode.R')
source('R/fct_awss3Connect.R')

source('R/run_inflow_model.R')
source('R/run_inflow_flow_model.R')
source('R/run_inflow_temperature_model.R')
source('R/run_inflow_salinity_model.R')

lake_directory <- here::here()
config_set_name <- "default"
forecast_site <- c("CANN")
configure_run_file <- "configure_run.yml"
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

FLAREr::get_targets(lake_directory, config)


message("Forecasting inflow and outflows")

sensorcode_df <- read_csv('configuration/default/sensorcode.csv')

## define define flow sensorcodes
print('Running Flow Inflow Forecast')
cann_flow_codes <- 'sensor_repository_00804'
south_flow_codes <- 'sensor_repository_00752'

## define temperature sensorcodes
temp_codes <- c("sensor_repository_81684", "sensor_repository_81698", "sensor_repository_81768", "sensor_repository_81782")

## define salt sensorcodes
salt_codes <- c('sensor_repository_81685', 'sensor_repository_81699', 'sensor_repository_81769', 'sensor_repository_81783')


reference_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime)
noaa_date <- reference_datetime - lubridate::days(1)
site_identifier <- 'CANN'
endpoint <- 'renc.osn.xsede.org'


## Run Forecast
inflow_forecast <- run_inflow_model(site_id = site_identifier, 
                                    forecast_start_datetime = reference_datetime, 
                                    use_s3_inflow = FALSE, 
                                    inflow_bucket = config$s3$inflow_drivers$bucket,
                                    inflow_endpoint = config$s3$inflow_drivers$endpoint,
                                    inflow_local_directory = file.path(lake_directory, "drivers/inflow"), 
                                    forecast_horizon = config$run_config$forecast_horizon)
