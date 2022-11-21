library(tidyverse)
library(lubridate)
set.seed(100)

Sys.setenv("AWS_DEFAULT_REGION" = "s3",
           "AWS_S3_ENDPOINT" = "flare-forecast.org")

lake_directory <- here::here()

starting_index <- 1

#files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
#sapply(files.sources, source)

sim_names <- "ALEX_test"
config_set_name <- "testing"
configure_run_file <- "configure_run.yml"


num_forecasts <- 10 #52 * 3 - 3
#num_forecasts <- 1#19 * 7 + 1
days_between_forecasts <- 1
forecast_horizon <- 16 #32
starting_date <- as_date("2022-11-10")
#second_date <- as_date("2020-12-01") - days(days_between_forecasts)
#starting_date <- as_date("2018-07-20")
#second_date <- as_date("2019-01-01") - days(days_between_forecasts)
#second_date <- as_date("2020-12-31") #- days(days_between_forecasts)
second_date <- as_date("2022-11-19") #- days(days_between_forecasts)



start_dates <- rep(NA, num_forecasts)
start_dates[1:2] <- c(starting_date, second_date)
for(i in 3:(3 + num_forecasts)){
  start_dates[i] <- as_date(start_dates[i-1]) + days(days_between_forecasts)
}

start_dates <- as_date(start_dates)
forecast_start_dates <- start_dates + days(days_between_forecasts)
forecast_start_dates <- forecast_start_dates[-1]

print(start_dates)
print(forecast_start_dates)
j = 1
sites <- "ALEX"

#function(i, sites, lake_directory, sim_names, config_files, )

message(paste0("Running site: ", sites[j]))

if(starting_index == 1){
  run_config <- yaml::read_yaml(file.path(lake_directory, "configuration", config_set_name, configure_run_file))
  run_config$configure_flare <- "configure_flare.yml"
  run_config$sim_name <- sim_names
  yaml::write_yaml(run_config, file = file.path(lake_directory, "configuration", config_set_name, configure_run_file))
  if(file.exists(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))){
    unlink(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))
  }
}

use_s3 <- FALSE

##'
# Set up configurations for the data processing
#config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name = config_set_name)
 
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

d <- readr::read_csv(file.path(lake_directory, "data_raw", "current_water_temp.csv"), skip = 1) |> 
  rename(time = `Timestamp (UTC+09:30)`,
         observed = `Value (°C)`) |> 
  select(time, observed) |> 
  mutate(time = lubridate::force_tz(time, tzone = "Etc/GMT+9"),
         time = time - lubridate::minutes(30),
         time = lubridate::with_tz(time, tzone = "UTC"),
         date = lubridate::as_date(time),
         hour = lubridate::hour(time)) |>
  group_by(date, hour) |> 
  summarize(observation = mean(observed, na.rm = TRUE), .groups = "drop") |> 
  mutate(variable = "temperature",
         depth = 0.5,
         site_id = "ALEX",
         datetime = lubridate::as_datetime(date) + lubridate::hours(hour)) |> 
  filter(hour == 0) |> 
  select(site_id, datetime, depth, variable, observation) |> 
  write_csv(file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")))

##` Download NOAA forecasts`

message("    Downloading NOAA data")

cycle <- "00"

if(starting_index == 1){
  config$run_config$start_datetime <- as.character(paste0(start_dates[1], " 00:00:00"))
  config$run_config$forecast_start_datetime <- as.character(paste0(start_dates[2], " 00:00:00"))
  config$run_config$forecast_horizon <- 0
  config$run_config$restart_file <- NA
  run_config <- config$run_config
  yaml::write_yaml(run_config, file = file.path(config$file_path$configuration_directory, configure_run_file))
}

#for(i in 1:1){

for(i in starting_index:length(forecast_start_dates)){
  #i <- 1
  
  config <- FLAREr::set_configuration(configure_run_file, lake_directory, config_set_name = config_set_name)
  
  config <- FLAREr::get_restart_file(config, lake_directory)
  
  message(paste0("     Running forecast that starts on: ", config$run_config$start_datetime))
  
  #forecast_dir <- NULL
  
  #Need to remove the 00 ensemble member because it only goes 16-days in the future
  #pars_config <- NULL
  #pars_config <- NULL #readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
  pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$par_config_file), col_types = readr::cols())
  obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$obs_config_file), col_types = readr::cols())
  states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$states_config_file), col_types = readr::cols())
  
  
  #Download and process observations (already done)
  
  #met_out <- FLAREr::generate_glm_met_files(obs_met_file = file.path(config$file_path$qaqc_data_directory, paste0("observed-met_",config$location$site_id,".nc")),
  #                                          out_dir = config$file_path$execute_directory,
  #                                          forecast_dir = forecast_dir,
  #                                          config = config)
  
  #inflow_outflow_files <- FLAREr::create_glm_inflow_outflow_files(inflow_file_dir = inflow_file_dir,
  #                                                                inflow_obs = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-inflow.csv")),
  #                                                                working_directory = config$file_path$execute_directory,
  #                                                                config = config,
  #                                                                state_names = states_config$state_names)
  
  #readr::read_csv(file.path(config$file_path$data_directory, "met_hourly_AlexSY.csv")) |> 
  #  mutate(time = lubridate::force_tz(time, tzone = "Etc/GMT+9"),
  #         time = time - lubridate::minutes(30),
  #         time = lubridate::with_tz(time, tzone = "UTC"),
  #         date = lubridate::as_date(time),
  #         hour = lubridate::hour(time),
  #         hour = ifelse(hour < 10, paste0("0",hour), hour),
  #         time = paste0(date," ", hour,":00")) |> 
  #  select(-c("date","hour")) |> 
  #  readr::write_csv(file.path(config$file_path$execute_directory, "met.csv"))

  #met_out <- NULL
  #met_out$filenames <- file.path(config$file_path$execute_directory, "met.csv")
  #met_out$historical_met_error <- FALSE
  
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
  
  
  file.copy(file.path(config$file_path$data_directory, "inflow_Well_WQ_DOcorr_v2.csv"),
           file.path(config$file_path$execute_directory, "inflow.csv"))
  
  file.copy(file.path(config$file_path$data_directory, "outflow_Well_v2.csv"),
            file.path(config$file_path$execute_directory, "outflow.csv"))
  
  inflow_outflow_files <- NULL
  inflow_outflow_files$inflow_file_names <- file.path(config$file_path$execute_directory, "inflow.csv")
  inflow_outflow_files$outflow_file_names <- file.path(config$file_path$execute_directory, "outflow.csv")
  
  #Create observation matrix
  obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                   obs_config = obs_config,
                                   config)
  
  start_datetime <- lubridate::as_datetime(config$run_config$start_datetime)
  if(is.na(config$run_config$forecast_start_datetime)){
    end_datetime <- lubridate::as_datetime(config$run_config$end_datetime)
    forecast_start_datetime <- end_datetime
  }else{
    forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime)
    end_datetime <- forecast_start_datetime + lubridate::days(config$run_config$forecast_horizon)
  }
  
  full_time <- seq(start_datetime, end_datetime, by = "1 day")
  
  #obs[ ,2:dim(obs)[2], ] <- NA
  
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
  
  message("Writing arrow score")
  
  message("Scoring forecasts")
  FLAREr::generate_forecast_score_arrow(targets_file = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                        forecast_df = forecast_df,
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
}
