source('R/fct_awss3Connect_sensorcode.R')
source('R/collect_insitu_targets.R')
source('R/collect_profile_targets.R')


# COLLECT INSITU SENSOR DATA
sensorcode_df <- read_csv('configuration/default/sensorcode.csv')

all_site_codes <- c('sensor_repository_81684', 'sensor_repository_81685', # Bacon Down (temp, salt)
                    'sensor_repository_81698', 'sensor_repository_81699', # Bacon Up
                    'sensor_repository_81768', 'sensor_repository_81769', # Nicholson Down
                    'sensor_repository_81782', 'sensor_repository_81783') # Nicholson Up

insitu_obs_df <- awss3Connect_sensorcode(sensorCodes = obs_sensorcodes, code_df = sensorcode_df) |> 
  select(-QC, -Date)

lake_insitu_df <- collect_insitu_targets(obs_download = insitu_obs_df, 
                                        site_location = 'CANN', 
                                        assign_depth = 1.5)


# COLLECT PROFILE DATA 
profile_obs_df <- awss3Connect(filename = 'arms/wiski.csv')
lake_profile_sites <- c('BAC','BACD300','BACD500','BACU300','CASMID','ELL','GRE','KEN',
                        'KENU300','KS7','KS9','MACD50','MASD50','NIC','NIC-IN',
                        'NICD200','PAC','PO2','RIV','SAL','SCB2') 

lake_profile_df <- collect_profile_targets(profile_data_download = profile_obs_df, 
                                           sites = lake_profile_sites)





#############
library(tidyverse)
library(lubridate)

source('R/fct_awss3Connect_sensorcode.R')
source('R/fct_awss3Connect.R')
source('R/generate_profile_targets.R')

lake_directory <- here::here()
config_set_name <- "default"
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

##  GENERATE DATA FOR LAKE SITES (TAKEN FROM PROFILE DATA)
lake_site_data <- generate_in_lake_targets(sensor)

### GENERATE INSITU WQ TARGETS

## current data only grabs water temperature in-situ data from Bacon Downstream site
all_site_codes <- c('sensor_repository_81684', 'sensor_repository_81685', # Bacon Down (temp, salt)
                     'sensor_repository_81698', 'sensor_repository_81699', # Bacon Up
                     'sensor_repository_81768', 'sensor_repository_81769', # Nicholson Down
                     'sensor_repository_81782', 'sensor_repository_81783') # Nicholson Up

insitu_obs_df <- awss3Connect_sensorcode(sensorCodes = obs_sensorcodes, code_df = sensorcode_df) |> 
  select(-QC, -Date)

# depth_df <- obs_download |> 
#   filter(variable == 'Depth')


# lake_obs <- obs_download |> 
#   pivot_wider(names_from = variable, values_from = Data)

# remove duplicates 
obs_dedup <- lake_obs |> distinct(datetime, Height, variable, .keep_all = TRUE)

obs_df_wide <- obs_dedup |> pivot_wider(names_from = variable, values_from = Data) |> rename(salt = `Salinity (ppt)`, temperature = 'Temperature')

obs_df <- obs_df_wide |> pivot_longer(cols = c('temperature','salt'),
                                      names_to = 'variable', 
                                      values_to = 'observation')

## assign columns
obs_df$site_id <- 'CANN'

cleaned_insitu_file <- obs_df |> 
  mutate(Date = as.Date(datetime)) |> 
  filter(lubridate::hour(datetime) == 0) |> # only want midnight observations for the daily value
  group_by(Date, variable) |> 
  mutate(observation = mean(observation, na.rm = TRUE)) |> 
  ungroup() |> 
  distinct(Date, variable, .keep_all = TRUE) |> 
  mutate(datetime = as.POSIXct(paste(Date, '00:00:00'), tz = "Australia/Perth")) |> 
  mutate(depth = 1.5) |> # assign depth to match model config depths (median depth value is 1.6)
  select(datetime, site_id, depth, observation, variable)



## GENERATE PROFILE TARGETS (all sites included here (inflow and lake sites))

#read in profile data
rawwiski <- awss3Connect(filename = 'arms/wiski.csv')
rawwiski$`Collect Date` <- as.Date(rawwiski$`Collect Date`,format="%d/%m/%Y")

profile_sites <- c('BAC','BACD300','BACD500','BACU300','ELL','GRE','KS7','KS9','MACD50','MASD50','NIC','NIC-IN',
                   'NICD200','PAC','PO2','KEN','KENU300') #('KEN','KENU300')  #these are all the sites in Canning River, and 'KEN' is Kent st weir. Note 'Bacon Downstream'  is not included in this dataset as it belongs to a different program. 
lake_profile_data <- generate_profile_targets(sites = profile_sites, wiski_data = rawwiski)

# combine targets and remove duplicates
#combined_targets <- dplyr::bind_rows(cleaned_insitu_file, profile_data)

write_csv(cleaned_insitu_file,file.path(lake_directory,"targets", 
                                        config$location$site_id,
                                        paste0(config$location$site_id,"-targets-insitu.csv")))

## GENERATE MET TARGETS 

met_download <- awss3Connect_sensorcode(sensorCodes = c('sensor_repository_84745', 'sensor_repository_84749', 'sensor_repository_00954', 'sensor_repository_00962'), 
                                        code_df = sensorcode_df)

# remove duplicates 
met_dedup <- met_download |> distinct(datetime, variable, .keep_all = TRUE)

#obs_df_wide <- obs_dedup |> pivot_wider(names_from = variable, values_from = Data) |> rename(Salinity = `Salinity (ppt)`)

#obs_df <- obs_df_wide |> pivot_longer(cols = c('Temperature','Salinity'),
#                                      names_to = 'variable', 
#                                      values_to = 'var_obs')

#met_dedup$Date <- as.Date(met_dedup$datetime, tz = "Australia/Perth")

## assign columns
met_dedup$site_id <- 'CANN'
#obs_df$depth <- 2 # over 500 different depth values all between 0 - 2.17m -- we can discuss how to deal with this later
#obs_df$observation <- obs_df$Data


cleaned_met_file <- met_dedup |> 
  #filter(variable %in% c('Temperature', 'Salinity (ppt)')) |> 
  # group_by(Date, variable) |> 
  # #filter(variable %in% c('Temperature', 'Salinity (ppt)')) |> 
  # mutate(observation = mean(Data, na.rm = TRUE)) |> 
  # ungroup() |> 
  # distinct(Date, variable, .keep_all = TRUE) |> 
  # mutate(datetime = as.POSIXct(paste(Date, '00:00:00'), tz = "Australia/Perth")) |> 
  rename(observation = Data) |> 
  select(datetime, site_id, observation, variable)

write_csv(cleaned_met_file,file.path(lake_directory,"targets", 
                                     config$location$site_id,
                                     paste0(config$location$site_id,"-targets-met.csv")))
