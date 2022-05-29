#renv::restore()

library(tidyverse)
library(lubridate)

message("Beginning generate targets")
message(config_set_name)

#' Set the lake directory to the repository directory

lake_directory <- here::here()


#' Source the R files in the repository

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

#' Generate the `config_obs` object and create directories if necessary

config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name)
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

readr::read_csv(file.path(lake_directory, "data_raw", "DataSetExport-Water Temp.Best Available--Continuous@A4261133-20220518222148.csv"), skip = 1) |> 
  rename(time = `Timestamp (UTC+09:30)`,
         observed = `Value (°C)`) |> 
  select(time, observed) |> 
  mutate(time == lubridate::force_tz(time, tzone = "Australia/Adelaide"),
         time == lubridate::with_tz(time, tzone = "UTC"),
         date = lubridate::as_date(time),
         hour = lubridate::hour(time)) |>
  group_by(date, hour) |> 
  summarize(observed = mean(observed, na.rm = TRUE), .groups = "drop") |> 
  mutate(variable = "temperature",
         depth = 1,
         site_id = "ALEX",
         time = lubridate::as_datetime(date) + lubridate::hours(hour)) |> 
  select(site_id, time, depth, variable, observed) |> 
  write_csv(file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")))

#' Move targets to s3 bucket

message("Successfully generated targets")

FLAREr::put_targets(site_id = config_obs$site_id,
                    cleaned_insitu_file,
                    cleaned_met_file,
                    cleaned_inflow_file,
                    use_s3 = config$run_config$use_s3)

if(config$run_config$use_s3){
  message("Successfully moved targets to s3 bucket")
}

