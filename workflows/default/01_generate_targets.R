#renv::restore()

library(tidyverse)
library(lubridate)

message("Beginning generate targets")
message(config_set_name)

#' Set the lake directory to the repository directory

lake_directory <- here::here()


#' Source the R files in the repository

#files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
#sapply(files.sources, source)

#' Generate the `config_obs` object and create directories if necessary

#config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name)
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

download.file(url = "https://water.data.sa.gov.au/Export/DataSet?DataSet=Water%20Temp.Best%20Available--Continuous%40A4261133&DateRange=Days30&ExportFormat=csv&Compressed=false&RoundData=False&Unit=degC&Timezone=9.5&_=1668874574781",
              destfile = file.path(lake_directory, "data_raw", "current_water_temp.csv"))

cleaned_insitu_file <- file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv"))
readr::read_csv(file.path(lake_directory, "data_raw", "current_water_temp.csv"), skip = 1) |> 
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
  write_csv(cleaned_insitu_file)

#' Move targets to s3 bucket

message("Successfully generated targets")

FLAREr::put_targets(site_id =  config$location$site_id,
                    cleaned_insitu_file,
                    cleaned_met_file = NA,
                    cleaned_inflow_file = NA,
                    use_s3 = config$run_config$use_s3,
                    config = config)

if(config$run_config$use_s3){
  message("Successfully moved targets to s3 bucket")
}

