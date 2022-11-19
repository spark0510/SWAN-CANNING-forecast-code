#renv::restore()

library(tidyverse)
library(lubridate)

if(file.exists("~/.aws")){
  warning(paste("Detected existing AWS credentials file in ~/.aws,",
                "Consider renaming these so that automated upload will work"))
}

lake_directory <- here::here()
update_run_config <- TRUE
files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

configure_run_file <- "configure_run.yml"

config_set_name <- "default"

config <- FLAREr::set_configuration(configure_run_file, lake_directory, config_set_name = config_set_name)

config <- FLAREr::get_restart_file(config, lake_directory)

message(paste0("     Running forecast that starts on: ", config$run_config$start_datetime))

#forecast_dir <- NULL

#Need to remove the 00 ensemble member because it only goes 16-days in the future
pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$par_config_file), col_types = readr::cols())
obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$obs_config_file), col_types = readr::cols())
states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$states_config_file), col_types = readr::cols())


#Download and process observations (already done)


met_out <- FLAREr::generate_met_files_arrow(obs_met_file = NULL,
                                            out_dir = config$file_path$execute_directory,
                                            start_datetime = config$run_config$start_datetime,
                                            end_datetime = config$run_config$end_datetime,
                                            forecast_start_datetime = config$run_config$forecast_start_datetime,
                                            forecast_horizon =  config$run_config$forecast_horizon,
                                            site_id = config$location$site_id,
                                            use_s3 = TRUE,
                                            bucket = config$s3$drivers$bucket,
                                            endpoint = config$s3$drivers$endpoint,
                                            local_directory = NULL,
                                            use_forecast = TRUE,
                                            use_ler_vars = FALSE)

met_out$filenames <- met_out$filenames[!stringr::str_detect(met_out$filenames, "31")]

management <- NULL

#Create observation matrix
obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                 obs_config = obs_config,
                                 config)

states_config <- FLAREr::generate_states_to_obs_mapping(states_config, obs_config)

model_sd <- FLAREr::initiate_model_error(config, states_config)

init <- FLAREr::generate_initial_conditions(states_config,
                                            obs_config,
                                            pars_config,
                                            obs,
                                            config,
                                            historical_met_error = met_out$historical_met_error)
#Run EnKF
da_forecast_output <- FLAREr::run_da_forecast(states_init = init$states,
                                              pars_init = init$pars,
                                              aux_states_init = init$aux_states_init,
                                              obs = obs,
                                              obs_sd = obs_config$obs_sd,
                                              model_sd = model_sd,
                                              working_directory = config$file_path$execute_directory,
                                              met_file_names = met_out$filenames,
                                              inflow_file_names = NULL, #inflow_outflow_files$inflow_file_name,
                                              outflow_file_names = NULL, #inflow_outflow_files$outflow_file_name,
                                              config = config,
                                              pars_config = pars_config,
                                              states_config = states_config,
                                              obs_config = obs_config,
                                              management = NULL,
                                              da_method = config$da_setup$da_method,
                                              par_fit_method = config$da_setup$par_fit_method,
                                              obs_secchi = NULL,
                                              obs_depth = NULL)

message("Writing netcdf")
saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                            forecast_output_directory = config$file_path$forecast_output_directory,
                                            use_short_filename = TRUE)

message("Writing arrow forecast")
forecast_df <- FLAREr::write_forecast_arrow(da_forecast_output = da_forecast_output,
                                            use_s3 = config$run_config$use_s3,
                                            bucket = config$s3$forecasts_parquet$bucket,
                                            endpoint = config$s3$forecasts_parquet$endpoint,
                                            local_directory = file.path(lake_directory, "forecasts/parquet"))

message("Grabbing last 16-days of forecasts")
evaluate_past <- FALSE
if(evaluate_past){
reference_datetime_format <- "%Y-%m-%d %H:%M:%S"
past_days <- strftime(lubridate::as_datetime(forecast_df$reference_datetime[1]) - lubridate::days(config$run_config$forecast_horizon), tz = "UTC")

vars <- FLAREr:::arrow_env_vars()
s3 <- arrow::s3_bucket(bucket = config$s3$forecasts_parquet$bucket, endpoint_override = config$s3$forecasts_parquet$endpoint)
past_forecasts <- arrow::open_dataset(s3) |>
  dplyr::filter(model_id == forecast_df$model_id[1],
                site_id == forecast_df$site_id[1],
                reference_datetime > past_days) |>
  dplyr::collect()
FLAREr:::unset_arrow_vars(vars)
}else{
  past_forecasts <- NULL
}

message("Combining forecasts")
combined_forecasts <- dplyr::bind_rows(forecast_df, past_forecasts)

message("Scoring forecasts")
FLAREr::generate_forecast_score_arrow(targets_file = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                      forecast_df = combined_forecasts,
                                      use_s3 = config$run_config$use_s3,
                                      bucket = config$s3$scores$bucket,
                                      endpoint = config$s3$scores$endpoint,
                                      local_directory = file.path(lake_directory, "scores/parquet"),
                                      variable_types = c("state","parameter"))

#rm(da_forecast_output)
#gc()
message("Generating plot")
FLAREr::plotting_general_2(file_name = saved_file,
                           target_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")),
                           ncore = 2,
                           obs_csv = FALSE)

message("Putting forecast")
FLAREr::put_forecast(saved_file, eml_file_name = NULL, config)

FLAREr::update_run_config(config, lake_directory, configure_run_file, saved_file, new_horizon = forecast_horizon, day_advance = days_between_forecasts)

FLAREr::update_run_config(config, lake_directory, configure_run_file, saved_file, new_horizon = 16, day_advance = 1)

message(paste0("successfully generated flare forecats for: ", basename(saved_file)))
