readRenviron("~/.Renviron") # MUST come first
library(tidyverse)
library(lubridate)
lake_directory <- here::here()
setwd(lake_directory)

source('march_wkshp/generate_met_files_arrow.R')
source('march_wkshp/run_flare.R')
source('march_wkshp/create_inflow_outflow_files_arrow.R')

forecast_site <- "CANN"
configure_run_file <- "configure_run.yml"
config_set_name <- "default"

fresh_run <- TRUE
# REMOVED FRESH RUN CODE 

Sys.setenv("AWS_DEFAULT_REGION" = "renc",
           "AWS_S3_ENDPOINT" = "osn.xsede.org",
           "USE_HTTPS" = TRUE)


message("Checking for NOAA forecasts")

config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)


## REPLACE WITH SC CODE FOR TARGETS

source('workflows/default/generate_targets.R')

#' Move targets to s3 bucket

message("Successfully generated targets")

FLAREr::put_targets(site_id =  config$location$site_id,
                    cleaned_insitu_file,
                    cleaned_met_file = NA,
                    cleaned_inflow_file = NA,
                    use_s3 = config$run_config$use_s3, # SET THIS TO FALSE
                    config = config)

if(config$run_config$use_s3){
  message("Successfully moved targets to s3 bucket")
}

## DELTE RESTART FILE 
## UPDATE CONFIG FILES - USE_OBS = TRUE, DATES MATCH, START DATE IS 30 DAYS PRIOR
## RUN CONFIG IS UPDATED BELOW

# a set of forecast dates to run
forecast_dates <- seq.Date(as_date('2023-09-01'), as_date('2023-11-01'), by = 'day')

forecast_dates <- as_date('2023-11-02')
#---------------------------------#
# set up first forecast run_config
forecast_start_datetime <- forecast_dates[1]
start_datetime <- forecast_start_datetime - lubridate::days(30) ## SET LONGER LOOK BACK FOR DATA FOR FIRST FORECAST
restart_file <- NA
end_datetime <- NA
forecast_horizon <- 34
sim_name <- config$run_config$sim_name # USE FREYAS
configure_flare <- config$run_config$configure_flare
configure_obs = config$run_config$configure_obs 
use_s3 = config$run_config$use_s3
bucket = config$s3$warm_start$bucket # CHECK SLACK NOTES FROM FREYA (VTBACKUP/DRIVERS/NOAA)
endpoint = config$s3$warm_start$endpoint # NEW ENDPOINT
use_https = TRUE


run_config <- NULL
run_config$restart_file <- restart_file
run_config$start_datetime <- as.character(lubridate::as_datetime(start_datetime))
if (lubridate::hour(run_config$start_datetime) == 0) {
  run_config$start_datetime <- paste(run_config$start_datetime, 
                                     "00:00:00")
}
run_config$forecast_start_datetime <- as.character(lubridate::as_datetime(forecast_start_datetime))
if (!is.na(run_config$forecast_start_datetime)) {
  if (lubridate::hour(run_config$forecast_start_datetime) == 
      0) {
    run_config$forecast_start_datetime <- paste(run_config$forecast_start_datetime, 
                                                "00:00:00")
  }
}

run_config$end_datetime <- as.character(lubridate::as_datetime(end_datetime))
if (!is.na(run_config$end_datetime)) {
  if (lubridate::hour(run_config$end_datetime) == 0) {
    run_config$end_datetime <- paste(run_config$end_datetime, 
                                     "00:00:00")
  }
}

run_config$forecast_horizon <- forecast_horizon
run_config$sim_name <- sim_name
run_config$configure_flare <- configure_flare
run_config$configure_obs <- configure_obs
run_config$use_s3 <- use_s3

file_name <- file.path(lake_directory, "restart", forecast_site, 
                       sim_name, configure_run_file)

yaml::write_yaml(run_config, file_name)
#---------------------------------------------#

# forecast_start_datetime <- config$run_config$forecast_start_datetime

while(as_date(forecast_start_datetime) %in% forecast_dates) {
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)
  
  source('R/run_inflow_forecast_full.R')
  
  # use the custom function for this example set of forecasts
  output <- run_flare(lake_directory = lake_directory,
                      configure_run_file = configure_run_file,
                      config_set_name = config_set_name)
  
  setwd(lake_directory)
  
  forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(1)
  start_datetime <- lubridate::as_datetime(forecast_start_datetime - lubridate::days(5)) ## SET LONGER LOOK BACK FOR DATA
  restart_file <- paste0(config$location$site_id,"-", (lubridate::as_date(forecast_start_datetime)- days(1)), "-",config$run_config$sim_name ,".nc")
  
  FLAREr::update_run_config2(lake_directory = lake_directory,
                             configure_run_file = configure_run_file, 
                             restart_file = basename(output$restart_file), 
                             start_datetime = lubridate::as_datetime(config$run_config$start_datetime) + lubridate::days(1), 
                             end_datetime = NA, 
                             forecast_start_datetime = lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(1),  
                             forecast_horizon = config$run_config$forecast_horizon,
                             sim_name = config$run_config$sim_name, 
                             site_id = config$location$site_id,
                             configure_flare = config$run_config$configure_flare, 
                             configure_obs = config$run_config$configure_obs, 
                             use_s3 = config$run_config$use_s3,
                             bucket = config$s3$warm_start$bucket,
                             endpoint = config$s3$warm_start$endpoint,
                             use_https = TRUE)
  
  # noaa_ready <- FLAREr::check_noaa_present_arrow(lake_directory,
  #                                                configure_run_file,
  #                                                config_set_name = config_set_name)
  
  
}


### save output to workshop bucket 
s3_save <- arrow::s3_bucket('bio230121-bucket01', endpoint_override = 'renc.osn.xsede.org')

# hindcast
arrow::open_dataset('forecasts/parquet/') |> 
  filter(model_id %in% c('glm_annual_run', 'glm_annual_run_noDA')) |> 
  arrow::write_dataset(s3_save$path('march24-workshop/hindcast/forecasts'), partitioning = 'site_id')

arrow::open_dataset('scores/parquet/') |> 
  filter(model_id %in% c('glm_annual_run', 'glm_annual_run_noDA')) %>% 
  arrow::write_dataset(s3_save$path('march24-workshop/hindcast/scores'), partitioning = 'site_id')

# reforecasts
arrow::open_dataset('forecasts/parquet/') |> 
  filter(model_id %in% c('glm_annual_run')) |> 
  arrow::write_dataset(s3_save$path('march24-workshop/reforecast/forecasts'), partitioning = 'site_id')

arrow::open_dataset('scores/parquet/') |> 
  filter(model_id %in% c('glm_annual_run')) |> 
  arrow::write_dataset(s3_save$path('march24-workshop/reforecast/scores'), partitioning = 'site_id')




## test plotting
score_df <- arrow::open_dataset('/home/rstudio/SWAN-CANNING-forecast-code/scores/parquet/site_id=CANN/model_id=glm_example_test/reference_date=2023-11-02/part-0.parquet') %>% 
  collect()

score_df |>
  filter(variable == 'temperature', 
         depth == 1.5) %>% 
  ggplot(aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5), alpha = 0.2) +
  #coord_cartesian(xlim = c(max(hindcast_scores$datetime) - days(120),
  #                        max(hindcast_scores$datetime))) +
  ylab('Temperature (degC)') +
  theme_bw() + 
  geom_vline(xintercept = as_datetime('2023-11-02'), linetype = 'dashed')
