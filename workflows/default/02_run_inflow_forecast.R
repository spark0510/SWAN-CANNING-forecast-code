#renv::restore()

library(tidyverse)
library(lubridate)

lake_directory <- here::here()

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

configure_run_file <- "configure_run.yml"
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

FLAREr::get_targets(lake_directory, config)

noaa_forecast_path <- FLAREr::get_driver_forecast_path(config,
                                             forecast_model = config$met$forecast_met_model)


if(!is.null(noaa_forecast_path)){

  FLAREr::get_driver_forecast(lake_directory, forecast_path = noaa_forecast_path)

  message("Forecasting inflow and outflows")
  # Forecast Inflows

  config$future_inflow_flow_coeff <- c(0.0010803, 0.9478724, 0.3478991)
  config$future_inflow_flow_error <- 0.00965
  config$future_inflow_temp_coeff <- c(0.20291, 0.94214, 0.04278)
  config$future_inflow_temp_error <- 0.943

  forecast_files <- list.files(file.path(lake_directory, "drivers", noaa_forecast_path), full.names = TRUE)
  if(length(forecast_files) == 0){
    stop(paste0("missing forecast files at: ", noaa_forecast_path))
  }
  temp_flow_forecast <- forecast_inflows_outflows(inflow_obs = file.path(config$file_path$qaqc_data_directory, "fcre-targets-inflow.csv"),
                                                  forecast_files = forecast_files,
                                                  obs_met_file = file.path(config$file_path$qaqc_data_directory,"observed-met_fcre.nc"),
                                                  output_dir = config$file_path$inflow_directory,
                                                  inflow_model = config$inflow$forecast_inflow_model,
                                                  inflow_process_uncertainty = FALSE,
                                                  forecast_location = config$file_path$forecast_output_directory,
                                                  config = config,
                                                  use_s3 = config$run_config$use_s3,
                                                  bucket = "drivers",
                                                  model_name = config$model_settings$model_name)


  message(paste0("successfully generated inflow forecats for: ", file.path(config$met$forecast_met_model,config$location$site_id,lubridate::as_date(config$run_config$forecast_start_datetime))))

}else{

  message("An inflow forecasts was not needed because the forecast horizon was 0 in run configuration file")

}
