## function for creating inflow forecasts using xgboost model

create_ml_inflows <- function(forecast_start_date, site_identifier, endpoint, s3_save_path){

## pull in past NOAA data
reference_datetime <- lubridate::as_datetime(forecast_start_date)
noaa_date <- reference_datetime - lubridate::days(1)
  
met_s3_future <- arrow::s3_bucket(paste0("drivers/noaa/gefs-v12-reprocess/stage2/parquet/0/",noaa_date,"/",site_identifier),
                                  endpoint_override = endpoint,
                                  anonymous = TRUE)

df_future <- arrow::open_dataset(met_s3_future) |> 
  #select(datetime, parameter, variable, prediction) |> 
  filter(variable %in% c("precipitation_flux","air_temperature")) |> 
  collect() |> 
  rename(ensemble = parameter) |> 
  mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
         variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
         prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction))


min_datetime <- min(df_future$datetime)

met_s3_past <- arrow::s3_bucket(paste0("drivers/noaa/gefs-v12-reprocess/stage3/parquet/",site_identifier),
                                endpoint_override = endpoint,
                                anonymous = TRUE)

years_prior <- reference_datetime - lubridate::days(1825) # 5 years

df_past <- arrow::open_dataset(met_s3_past) |> 
  #select(datetime, parameter, variable, prediction) |> 
  filter(variable %in% c("precipitation_flux","air_temperature"),
         ((datetime <= min_datetime  & variable == "precipitation_flux") | 
            datetime < min_datetime  & variable == "air_temperature"),
         datetime > years_prior) |> 
  collect() |> 
  rename(ensemble = parameter) |> 
  mutate(variable = ifelse(variable == "precipitation_flux", "precipitation", variable),
         variable = ifelse(variable == "air_temperature", "temperature_2m", variable),
         prediction = ifelse(variable == "temperature_2m", prediction - 273.15, prediction))

# combine past and future noaa data
df_combined <- bind_rows(df_future, df_past) |> 
  arrange(variable, datetime, ensemble)

forecast_precip <- df_combined |> 
  filter(variable == 'precipitation') |> 
  summarise(precip_hourly = median(prediction, na.rm = TRUE), .by = c("datetime")) |> # get the median hourly precip across all EMs
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(precip = sum(precip_hourly, na.rm = TRUE), .by = c("date")) |> # get the total precip for each day
  mutate(sevenday_precip = RcppRoll::roll_sum(precip, n = 7, fill = NA,align = "right")) |> 
  mutate(doy = lubridate::yday(date))

forecast_temp <- df_combined |> 
  filter(variable == 'temperature_2m') |> 
  summarise(temp_hourly = median(prediction, na.rm = TRUE), .by = c("datetime")) |> # get the median hourly temp across all EMs
  mutate(date = lubridate::as_date(datetime)) |> 
  summarise(temperature = median(temp_hourly, na.rm = TRUE), .by = c("date")) # get median temp across hours of the day

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

## set training as all data prior to start of forecast
train_data <- forecast_drivers |> 
  filter(date < reference_datetime)

## define folds in training data 
folds <- vfold_cv(train_data, v = 5) # orginally set to 10

#set the recipe
rec <- recipe(total_flow ~ precip + sevenday_precip + doy + temperature,
              data = train_data)

rec_preprocess <- rec |> 
  step_normalize(all_numeric_predictors()) #|> 
  #step_dummy(doy)

## define model and tunining parameters (tuning 2/8 parameters right now)
xgboost_mod <- boost_tree(tree_depth = tune(), trees = tune()) |> #, learn_rate = tune()) |> 
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

# select the best tuned hyper-parameters
best_hyperparameters <- inflow_resample_fit %>%
  select_best("rmse")

final_wrorkflow <- xgboost_inflow_wkflow |> 
  finalize_workflow(best_hyperparameters)

## fit the model (using all available data (past and future) for now but could just use training data)
xgboost_inflow_fit <- fit(final_wrorkflow, data = forecast_drivers)

# make predictions for each ensemble member 
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
  arrange(date,ensemble) |> 
  filter(date >= reference_datetime)

#make empty dataframe to store predictions
data_build <- data.frame()

for (i in unique(forecast_met_ens$ensemble)){
  
  ens_df <- forecast_met_ens |> 
    filter(ensemble == i)
  
  ens_inflow <- predict(xgboost_inflow_fit, new_data = ens_df)
  
  ens_predictions <- cbind(ens_df,ens_inflow) |> 
    rename(prediction = .pred) |> 
    mutate(prediction = ifelse(prediction < 0, 0, prediction))
  
  
  data_build <- bind_rows(data_build,ens_predictions)
  
}

final_predictions <- data_build 

final_predictions$reference_datetime <- reference_datetime
final_predictions$family <- 'ensemble'
final_predictions$variable <- 'FLOW'
final_predictions$model_id <- 'inflow-xbgoost'
final_predictions$flow_type <- 'inflow'
final_predictions$flow_number <- 1
final_predictions$parameter <- final_predictions$ensemble
final_predictions$site_id <- 'CANN'
final_predictions$datetime <- final_predictions$date  

final_predictions <- final_predictions |> select(model_id, site_id, reference_datetime, datetime, family, parameter, variable, prediction, flow_type, flow_number)

arrow::write_dataset(final_predictions, path = s3_save_path)

} # end function



# # save data and write to bucket
# write.csv(final_predictions, 'inflow_predictions.csv', row.names = FALSE)
# 

# extra plotting code

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





# # inflow_plot
# 
# 
# inflow_plot <- ggplot(final_predictions, aes(x=date, y=prediction, group = factor(ensemble))) +
#   geom_line()
# inflow_plot
# 
# 
# ## plot reforecast
# reforecast_df <- final_predictions |> 
#   left_join(site_inflow_wide, by = c('date')) |> 
#   drop_na(total_flow) |> 
#   group_by(date) |> 
#   mutate(avg_predicted = median(prediction)) |> 
#   ungroup()
# 
# reforecast_plot <- ggplot(reforecast_df, aes(x=date, y=prediction, group = factor(ensemble))) +
#   geom_line() +
#   geom_line(aes(y = total_flow), color = "red") +
#   geom_line(aes(y = avg_predicted), color = "blue")
# 
# reforecast_plot
# 
# ## plot precip
# precip_plot <- ggplot(final_predictions, aes(x=date, y=precip, group = factor(ensemble))) +
#   geom_line()
# precip_plot
# 
# precip7 <- ggplot(final_predictions, aes(x=date, y= sevenday_precip, group = factor(ensemble))) +
#   geom_line()
# precip7

# t <- arrow::s3_bucket(paste0("scores/parquet"),
#                       endpoint_override = "s3.flare-forecast.org",
#                       anonymous = TRUE)
# 
# df_future <- arrow::open_dataset(t) |> 
#   filter(site_id == 'fcre') |> 
#   collect()
