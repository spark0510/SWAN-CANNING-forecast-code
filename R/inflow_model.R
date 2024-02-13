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
  summarise(precip_hourly = median(prediction, na.rm = TRUE), .by = c("datetime")) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(precip = sum(precip_hourly, na.rm = TRUE), .by = c("date")) |> 
  mutate(sevenday_precip = RcppRoll::roll_sum(precip, n = 7, fill = NA,align = "right")) |> 
  mutate(doy = lubridate::yday(date))

forecast_temp <- df_combined |> 
  filter(variable == 'temperature_2m') |> 
  summarise(temp_hourly = median(prediction, na.rm = TRUE), .by = c("datetime")) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(temperature = median(temp_hourly, na.rm = TRUE), .by = c("date"))

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

# split <- initial_split(forecast_drivers, prop = 0.80, strata = NULL)
# 
# train_data <- training(split)
# test_data <- testing(split)

train_data <- forecast_drivers |> 
  filter(date < Sys.Date())

## define folds in training data 
folds <- vfold_cv(train_data, v = 10)

#set the recipe
rec <- recipe(total_flow ~ precip + sevenday_precip + doy + temperature,
              data = train_data)

rec_preprocess <- rec |> 
  step_normalize(all_numeric_predictors()) #|> 
  #step_dummy(doy)

## define model and tunining parameters
xgboost_mod <- boost_tree(tree_depth = tune(), trees = tune(), learn_rate = tune()) |> 
  set_mode("regression") |>  
  set_engine("xgboost")

# define the model workflow
xgboost_inflow_wkflow <- 
  workflow() %>% 
  add_model(xgboost_mod) %>% 
  add_recipe(rec_preprocess)

# tune the hyper-parameters
inflow_resample_fit <- xgboost_inflow_wkflow |> 
  tune_grid(resamples = folds, 
            grid = 25, 
            control = control_grid(save_pred = TRUE),
            metrics = metric_set(rmse))

# show the results from tuning 
inflow_resample_fit %>% 
  collect_metrics() |> 
  arrange(mean)

best_hyperparameters <- inflow_resample_fit %>%
  select_best("rmse")

final_wrorkflow <- xgboost_inflow_wkflow |> 
  finalize_workflow(best_hyperparameters)

## fit the model
xgboost_inflow_fit <- fit(final_wrorkflow, data = train_data)

# make predictions for each enemble member 
forecast_precip_ens <- df_combined |> 
  filter(variable == 'precipitation') |> 
  #summarise(precip_hourly = sum(prediction, na.rm = TRUE), .by = c("datetime","ensemble")) |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(precip = sum(prediction, na.rm = TRUE), .by = c("date","ensemble")) |> 
  arrange(date, ensemble) |> 
  group_by(ensemble) |> 
  mutate(sevenday_precip = RcppRoll::roll_sum(precip, n = 7, fill = NA,align = "right")) |> 
  ungroup() |> 
  mutate(doy = lubridate::yday(date))

forecast_temp_ens <- df_combined |> 
  filter(variable == 'temperature_2m') |> 
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(temperature = median(prediction, na.rm = TRUE), .by = c("date","ensemble"))

forecast_met_ens <- forecast_precip_ens |> 
  right_join(forecast_temp_ens, by = c('date',"ensemble")) |> 
  arrange(date,ensemble)

#make empty dataframe to store predictions
data_build <- data.frame()

for (i in unique(forecast_met_ens$ensemble)){
  
  ens_df <- forecast_met_ens |> 
    filter(ensemble == i)
  
  ens_inflow <- predict(xgboost_inflow_fit, new_data = ens_df)
  
  ens_predictions <- cbind(ens_df,ens_inflow) |> 
    rename(prediction = .pred)
  
  
  data_build <- bind_rows(data_build,ens_predictions)
  
}

final_predictions <- data_build 

write.csv(final_predictions, 'inflow_predictions.csv', row.names = FALSE)

### forecasting for day instead of each ensemble member
# ## set up forecasted df for predictions
# predicted_met <- forecast_met |> 
#   filter(date >= Sys.Date())

# # make future inflow predictions
# inflow_predictions <- predict(xgboost_inflow_fit, new_data = predicted_met)
# 
# prediction_df <- cbind(predicted_met,inflow_predictions)

# # ## visual inspection using training data
# historical_drivers <- forecast_drivers |>
#   filter(date < Sys.Date())
# 
# trained_predictions <- predict(xgboost_inflow_fit, new_data = historical_drivers)
# 
# historical_predictions <- cbind(historical_drivers, trained_predictions) |>
#   rename(predictions = .pred)
# 
# inflow_plot <- ggplot(historical_predictions, aes(x=date)) +
#   geom_line(aes(y = total_flow), color = "darkred") +
#   geom_line(aes(y = predictions), color="steelblue") +
#   xlim(c(as.Date('2021-01-01'), as.Date('2022-01-01')))
# 
# inflow_plot
# 
# ## testing set
# test_predictions <- predict(xgboost_inflow_fit, new_data = test_data)
# 
# test_predictions <- cbind(test_data, test_predictions) |>
#   rename(predictions = .pred)
# 
# inflow_plot <- ggplot(test_predictions, aes(x=date)) +
#   geom_line(aes(y = total_flow), color = "darkred") +
#   geom_line(aes(y = predictions), color="steelblue") #+
#   #xlim(c(as.Date('2021-01-01'), as.Date('2022-01-01')))
# 
# inflow_plot
