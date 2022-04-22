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

config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

config <- FLAREr::get_restart_file(config, lake_directory)

FLAREr::get_targets(lake_directory, config)

noaa_forecast_path <- FLAREr::get_driver_forecast_path(config,
                                                       forecast_model = config$met$forecast_met_model)

inflow_forecast_path <- FLAREr::get_driver_forecast_path(config,
                                                         forecast_model = config$inflow$forecast_inflow_model)

if(!is.null(noaa_forecast_path)){
  FLAREr::get_driver_forecast(lake_directory, forecast_path = noaa_forecast_path)
  forecast_dir <- file.path(config$file_path$noaa_directory, noaa_forecast_path)
}else{
  forecast_dir <- NULL
}

if(!is.null(inflow_forecast_path)){
  FLAREr::get_driver_forecast(lake_directory, forecast_path = inflow_forecast_path)
  inflow_file_dir <- file.path(config$file_path$noaa_directory,inflow_forecast_path)
}else{
  inflow_file_dir <- NULL
}


pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$par_config_file), col_types = readr::cols())
obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$obs_config_file), col_types = readr::cols())
states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$states_config_file), col_types = readr::cols())


#Download and process observations (already done)

met_out <- FLAREr::generate_glm_met_files(obs_met_file = file.path(config$file_path$qaqc_data_directory, paste0("observed-met_",config$location$site_id,".nc")),
                                          out_dir = config$file_path$execute_directory,
                                          forecast_dir = forecast_dir,
                                          config = config)

met_out$filenames <- met_out$filenames[!stringr::str_detect(met_out$filenames, "ens00")]


inflow_outflow_files <- FLAREr::create_glm_inflow_outflow_files(inflow_file_dir = inflow_file_dir,
                                                                inflow_obs = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-inflow.csv")),
                                                                working_directory = config$file_path$execute_directory,
                                                                config = config,
                                                                state_names = states_config$state_names)

management <- NULL

if(config$model_settings$model_name == "glm_aed"){

  https_file <- "https://raw.githubusercontent.com/cayelan/FCR-GLM-AED-Forecasting/master/FCR_2013_2019GLMHistoricalRun_GLMv3beta/inputs/FCR_SSS_inflow_2013_2021_20211102_allfractions_2DOCpools.csv"
  download.file(https_file,
                file.path(config$file_path$execute_directory, basename(https_file)))

  inflow_outflow_files$inflow_file_name <- cbind(inflow_outflow_files$inflow_file_name, rep(file.path(config$file_path$execute_directory,basename(https_file)), length(inflow_outflow_files$inflow_file_name)))
}

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
                                            restart_file = config$run_config$restart_file,
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
                                              inflow_file_names = inflow_outflow_files$inflow_file_name,
                                              outflow_file_names = inflow_outflow_files$outflow_file_name,
                                              config = config,
                                              pars_config = pars_config,
                                              states_config = states_config,
                                              obs_config = obs_config,
                                              management,
                                              da_method = config$da_setup$da_method,
                                              par_fit_method = config$da_setup$par_fit_method)

# Save forecast

saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                            forecast_output_directory = config$file_path$forecast_output_directory,
                                            use_short_filename = TRUE)

#Create EML Metadata
eml_file_name <- FLAREr::create_flare_metadata(file_name = saved_file,
                                               da_forecast_output = da_forecast_output)

#Clean up temp files and large objects in memory
#unlink(config$file_path$execute_directory, recursive = TRUE)

FLAREr::put_forecast(saved_file, eml_file_name, config)

rm(da_forecast_output)
gc()

FLAREr::update_run_config(config, lake_directory, configure_run_file, saved_file, new_horizon = 16, day_advance = 1)

message(paste0("successfully generated flare forecats for: ", basename(saved_file)))
