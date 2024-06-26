library(tidyverse)
library(lubridate)

source('R/fct_awss3Connect_sensorcode.R')

lake_directory <- here::here()
config_set_name <- "open_meteo"
forecast_site <- c("CANN")
#configure_run_file <- paste0("configure_run_",forecast_site,".yml")
configure_run_file <- "configure_run.yml"
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)

config_obs <- FLAREr::initialize_obs_processing(lake_directory, 
                                                observation_yml = "observation_processing.yml", 
                                                config_set_name = config_set_name)

dir.create(file.path(lake_directory, "targets", config$location$site_id), showWarnings = FALSE)

## RUN CODE TO GENERATE DATA
sensorcode_df <- read_csv('configuration/default/sensorcode.csv')

obs_download <- awss3Connect_sensorcode(sensorCodes = c('sensor_repository_81684','sensor_repository_81681', 'sensor_repository_81685'), code_df = sensorcode_df)

# remove duplicates 
obs_dedup <- obs_download |> distinct(datetime, Height, variable, .keep_all = TRUE)

obs_df_wide <- obs_dedup |> pivot_wider(names_from = variable, values_from = Data) |> rename(Salinity = `Salinity (ppt)`)

obs_df <- obs_df_wide |> pivot_longer(cols = c('Temperature','Salinity'),
                                      names_to = 'variable', 
                                      values_to = 'var_obs')

obs_df$Date <- as.Date(obs_df$datetime, tz = "Australia/Perth")


## assign columns
obs_df$site_id <- 'CANN'
#obs_df$depth <- 2 # over 500 different depth values all between 0 - 2.17m -- we can discuss how to deal with this later
#obs_df$observation <- obs_df$Data


cleaned_insitu_file <- obs_df |> 
  #filter(variable %in% c('Temperature', 'Salinity (ppt)')) |> 
  group_by(Date, variable) |> 
  #filter(variable %in% c('Temperature', 'Salinity (ppt)')) |> 
  mutate(observation = mean(var_obs, na.rm = TRUE)) |> 
  ungroup() |> 
  distinct(Date, variable, .keep_all = TRUE) |> 
  mutate(datetime = as.POSIXct(paste(Date, '00:00:00'), tz = "Australia/Perth")) |> 
  rename(depth = Depth) |> 
  select(datetime, site_id, depth, observation, variable)

write_csv(cleaned_insitu_file,file.path(lake_directory,"targets", 
                                        config$location$site_id,
                                        paste0(config$location$site_id,"-targets-insitu.csv")))
