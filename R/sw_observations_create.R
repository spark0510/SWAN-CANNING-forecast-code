library(tidyverse)

source('R/fct_awss3Connect_sensorcode.R')

sensorcode_df <- read_csv('configuration/default/sensorcode.csv')

obs_df <- awss3Connect_sensorcode(sensorCodes = c('sensor_repository_81684','sensor_repository_81681'), code_df = sensorcode_df)

obs_df$Date <- as.Date(obs_df$datetime, tz = "Australia/Perth")

## assign columns
obs_df$site_id <- 'swan_canning'
obs_df$depth <- 2 # over 500 different depth values all between 0 - 2.17m -- we can discuss how to deal with this later
obs_df$observation <- obs_df$Data


obs_aggregate_df <- obs_df |> 
  group_by(Date) |> 
  filter(variable == 'Temperature') |> 
  mutate(day_avg = mean(Data, na.rm = TRUE)) |> 
  ungroup() |> 
  distinct(Date, .keep_all = TRUE) |> 
  mutate(datetime = as.POSIXct(paste(Date, '00:00:00'), tz = "Australia/Perth")) |> 
  select(datetime, site_id, depth, observation, variable)

write.csv(obs_aggregate_df, 'targets/swan_canning_observations.csv')
