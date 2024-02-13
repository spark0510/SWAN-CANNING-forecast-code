## code for creating inflow forecasts for swan-canning using xgboost model

library(tidyverse)
library(tidymodels)
library(xgboost)

source('R/fct_awss3Connect_sensorcode.R')


## get all necessary data
reference_datetime <- Sys.Date()  
noaa_date <- reference_datetime - lubridate::days(1)

## pull in past NOAA data
met_s3_future <- arrow::s3_bucket(paste0("drivers/noaa/gefs-v12-reprocess/stage2/parquet/0/",noaa_date,"/CANN"),
                                  endpoint_override = "s3.flare-forecast.org",
                                  anonymous = TRUE)

df_future <- arrow::open_dataset(met_s3_future) |> 
  select(datetime, parameter, variable, prediction) |> 
  filter(variable %in% c("precipitation_flux","air_temperature")) |> 
  collect() |> 
  rename(ensemble = parameter) |> 
  mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
         variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
         prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction))


min_datetime <- min(df_future$datetime)

met_s3_past <- arrow::s3_bucket(paste0("drivers/noaa/gefs-v12-reprocess/stage3/parquet/CANN"),
                                endpoint_override = "s3.flare-forecast.org",
                                anonymous = TRUE)

#week_prior <- reference_datetime - lubridate::days(7)
years_prior <- reference_datetime - lubridate::days(1825) # 5 years

df_past <- arrow::open_dataset(met_s3_past) |> 
  select(datetime, parameter, variable, prediction) |> 
  filter(variable %in% c("precipitation_flux","air_temperature"),
         ((datetime <= min_datetime  & variable == "precipitation_flux") | 
            datetime < min_datetime  & variable == "air_temperature"),
         datetime > years_prior) |> 
  collect() |> 
  rename(ensemble = parameter) |> 
  mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
         variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
         prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction))


df_combined <- bind_rows(df_future, df_past) |> 
  arrange(variable, datetime, ensemble)

forecast_precip <- df_combined |> 
  filter(variable == 'precipitation') |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(precip = sum(prediction, na.rm = TRUE), .by = c("date")) |> 
  mutate(sevenday_precip = RcppRoll::roll_sum(precip, n = 7, fill = NA,align = "right")) |> 
  mutate(doy = lubridate::yday(date))

forecast_temp <- df_combined |> 
  filter(variable == 'temperature_2m') |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(temperature = mean(prediction, na.rm = TRUE), .by = c("date"))

forecast_met <- forecast_precip |> 
  right_join(forecast_temp, by = c('date'))

## get inflow data
sensorcode_df <- read_csv('configuration/default/sensorcode.csv')

### GENERATE INSITU Inflow TARGETS
cann_inflow_download <- awss3Connect_sensorcode(sensorCodes = c('sensor_repository_00804'), code_df = sensorcode_df)
south_inflow_download <- awss3Connect_sensorcode(sensorCodes = c('sensor_repository_00752'), code_df = sensorcode_df)

cann_inflow_download$site_id <- 'cann_river'
south_inflow_download$site_id <- 'south_river'

inflow_combined <- dplyr::bind_rows(cann_inflow_download, south_inflow_download)

inflow_combined$Date <- as.Date(inflow_combined$datetime, tz = "Australia/Perth")

site_daily_inflow_rate_df <- inflow_combined |> 
  group_by(Date, site_id) |> 
  mutate(site_average_rate = mean(Data, na.rm = TRUE)) |> 
  ungroup() |> 
  distinct(Date, site_id, .keep_all = TRUE)

site_daily_inflow_total <- site_daily_inflow_rate_df |> 
  mutate(daily_total = site_average_rate*86400) # second rate (m3/s) to day rate (86400 seconds per day) 


daily_inflow_combined <- site_daily_inflow_total |> 
  group_by(Date) |> 
  mutate(combined_rate = ifelse((length(unique(site_id)) > 1), sum(daily_total), NA)) |> 
  ungroup() |> 
  drop_na(combined_rate)

site_inflow_wide <- daily_inflow_combined |> 
  pivot_wider(id_cols = c('Date'), names_from = 'site_id', values_from = 'daily_total') |> 
  group_by(Date) |> 
  mutate(total_flow = sum(cann_river + south_river)) |> 
  ungroup() |> 
  arrange(Date) |> 
  select(date = Date, total_flow)

forecast_drivers <- forecast_met |> 
  left_join(site_inflow_wide, by = c('date')) |> 
  drop_na(total_flow)

split <- initial_split(forecast_drivers, prop = 0.80, strata = NULL)

train_data <- training(split)
test_data <- testing(split)

#set the recipe
rec <- recipe(total_flow ~ precip + sevenday_precip + doy + temperature,
              data = train_data)

rec_preprocess <- rec |> 
  step_normalize(all_numeric_predictors())

xgboost_mod <- boost_tree() |> 
  set_mode("regression") |>  
  set_engine("xgboost")

xgboost_inflow_wkflow <- 
  workflow() %>% 
  add_model(xgboost_mod) %>% 
  add_recipe(rec_preprocess)

## fit the model
xgboost_inflow_fit <- fit(xgboost_inflow_wkflow, data = train_data)

## set up forecasted df for predictions
predicted_met <- forecast_met |> 
  filter(date >= Sys.Date())

# make future inflow predictions
inflow_predictions <- predict(xgboost_inflow_fit, new_data = predicted_met)

prediction_df <- cbind(predicted_met,inflow_predictions)
