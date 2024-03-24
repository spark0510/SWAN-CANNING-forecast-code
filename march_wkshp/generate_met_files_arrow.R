generate_met_files_arrow <- function (obs_met_file = NULL, out_dir, start_datetime, end_datetime = NA, 
                                      forecast_start_datetime = NA, forecast_horizon = 0, site_id, 
                                      use_s3 = FALSE, bucket = NULL, endpoint = NULL, local_directory = NULL, 
                                      use_forecast = TRUE, use_ler_vars = FALSE, use_hive_met = TRUE) 
{
  lake_name_code <- site_id
  start_datetime <- lubridate::as_datetime(start_datetime)
  if (is.na(forecast_start_datetime)) {
    end_datetime <- lubridate::as_datetime(end_datetime)
    forecast_start_datetime <- end_datetime
  }
  else {
    forecast_start_datetime <- lubridate::as_datetime(forecast_start_datetime)
    end_datetime <- forecast_start_datetime + lubridate::days(forecast_horizon)
  }
  if (!is.na(forecast_start_datetime) & forecast_horizon > 
      0) {
    forecast_date <- lubridate::as_date(forecast_start_datetime)
    forecast_hour <- lubridate::hour(forecast_start_datetime)
    if (forecast_hour != 0) {
      stop("Only forecasts that start at 00:00:00 UTC are currently supported")
    }
    if (use_s3) {
      if (is.null(bucket) | is.null(endpoint)) {
        stop("inflow forecast function needs bucket and endpoint if use_s3=TRUE")
      }
      vars <- FLAREr:::arrow_env_vars()
      if (use_hive_met) {
        forecast_dir <- arrow::s3_bucket(bucket = file.path(bucket, 
                                                            paste0("stage2/parquet/0/", forecast_date), 
                                                            paste0("", lake_name_code)), endpoint_override = endpoint, 
                                         anonymous = TRUE)
      }
      else {
        forecast_dir <- arrow::s3_bucket(bucket = file.path(bucket, 
                                                            "stage2/parquet", forecast_hour, forecast_date, 
                                                            lake_name_code), endpoint_override = endpoint, 
                                         anonymous = TRUE)
      }
      FLAREr:::unset_arrow_vars(vars)
    }
    else {
      if (is.null(local_directory)) {
        stop("inflow forecast function needs local_directory if use_s3=FALSE")
      }
      forecast_dir <- arrow::SubTreeFileSystem$create(file.path(local_directory, 
                                                                "stage2/parquet", forecast_hour, forecast_date))
    }
  }
  if (!use_forecast | forecast_horizon == 0) {
    forecast_dir <- NULL
  }
  if (is.null(obs_met_file)) {
    if (use_s3) {
      if (use_hive_met) {
        past_dir <- arrow::s3_bucket(bucket = file.path(bucket, 
                                                        paste0("stage3/parquet/", lake_name_code)), 
                                     endpoint_override = endpoint, anonymous = TRUE)
      }
      else {
        past_dir <- arrow::s3_bucket(bucket = file.path(bucket, 
                                                        "stage3/parquet", lake_name_code), endpoint_override = endpoint, 
                                     anonymous = TRUE)
      }
    }
    else {
      past_dir <- arrow::SubTreeFileSystem$create(file.path(local_directory, 
                                                            "stage3/parquet", lake_name_code))
    }
  }
  full_time <- seq(start_datetime, end_datetime, by = "1 hour")
  if (use_forecast) {
    if (forecast_start_datetime > start_datetime) {
      full_time_hist <- seq(start_datetime, forecast_start_datetime, 
                            by = "1 hour")
    }
    else {
      full_time_hist <- NULL
    }
  }
  else {
    full_time_hist <- seq(start_datetime, end_datetime, by = "1 hour")
  }
  if (!is.null(obs_met_file)) {
    target <- dplyr::select(dplyr::mutate(dplyr::filter(dplyr::mutate_at(dplyr::mutate(dplyr::rename(dplyr::mutate(dplyr::arrange(tidyr::pivot_wider(readr::read_csv(obs_met_file, 
                                                                                                                                                                     show_col_types = FALSE), names_from = variable, values_from = observation), 
                                                                                                                                  datetime), WindSpeed = wind_speed), AirTemp = air_temperature, 
                                                                                                     ShortWave = surface_downwelling_shortwave_flux_in_air, 
                                                                                                     LongWave = surface_downwelling_longwave_flux_in_air, 
                                                                                                     RelHum = relative_humidity, Rain = precipitation_flux, 
                                                                                                     time = datetime), AirTemp = AirTemp - 273.15, RelHum = RelHum * 
                                                                                         100, RelHum = ifelse(RelHum > 100, 100, RelHum), 
                                                                                       Rain = ifelse(use_ler_vars, Rain * (60 * 60), Rain * 
                                                                                                       (60 * 60 * 24)/1000), Snow = 0), dplyr::vars(all_of(c("AirTemp", 
                                                                                                                                                             "ShortWave", "LongWave", "RelHum", "WindSpeed"))), 
                                                                         list(~round(., 2))), time %in% full_time_hist), Rain = round(Rain, 
                                                                                                                                      5), time = strftime(time, format = "%Y-%m-%d %H:%M", 
                                                                                                                                                          tz = "UTC")), time, AirTemp, ShortWave, LongWave, 
                            RelHum, WindSpeed, Rain, Snow)
    if (any(target$RelHum <= 0)) {
      idx <- which(target$RelHum <= 0)
      target$RelHum[idx] <- NA
      target$RelHum <- zoo::na.approx(target$RelHum, rule = 2)
    }
  }
  else if (!is.null(full_time_hist)) {
    target <- dplyr::ungroup(dplyr::slice(dplyr::group_by(dplyr::select(dplyr::mutate(dplyr::mutate_at(dplyr::mutate(dplyr::rename(dplyr::mutate(dplyr::arrange(tidyr::pivot_wider(dplyr::filter(dplyr::collect(dplyr::select(dplyr::filter(arrow::open_dataset(past_dir), 
                                                                                                                                                                                                                                            site_id == lake_name_code), datetime, parameter, 
                                                                                                                                                                                                                              variable, prediction)), datetime %in% full_time_hist), 
                                                                                                                                                                                   names_from = variable, values_from = prediction), 
                                                                                                                                                                parameter, datetime), WindSpeed = sqrt(eastward_wind^2 + 
                                                                                                                                                                                                         northward_wind^2)), AirTemp = air_temperature, ShortWave = surface_downwelling_shortwave_flux_in_air, 
                                                                                                                                   LongWave = surface_downwelling_longwave_flux_in_air, 
                                                                                                                                   RelHum = relative_humidity, Rain = precipitation_flux, 
                                                                                                                                   ensemble = parameter, time = datetime), AirTemp = AirTemp - 
                                                                                                                       273.15, RelHum = RelHum * 100, RelHum = ifelse(RelHum > 
                                                                                                                                                                        100, 100, RelHum), Rain = ifelse(use_ler_vars, Rain * 
                                                                                                                                                                                                           (60 * 60), Rain * (60 * 60 * 24)/1000), Snow = 0), 
                                                                                                       dplyr::vars(all_of(c("AirTemp", "ShortWave", "LongWave", 
                                                                                                                            "RelHum", "WindSpeed"))), list(~round(., 2))), 
                                                                                      Rain = round(Rain, 5), time = format(time, format = "%Y-%m-%d %H:%M", 
                                                                                                                           tz = "UTC")), ensemble, time, AirTemp, ShortWave, 
                                                                        LongWave, RelHum, WindSpeed, Rain, Snow), ensemble), 
                                          -dplyr::n()))
    n_gaps <- tsibble::count_gaps(tsibble::as_tsibble(dplyr::mutate(target, 
                                                                    time = lubridate::ymd_hm(time)), index = time, key = ensemble))
    if (nrow(n_gaps) > 0) {
      n_gaps <- pull(dplyr::summarise(n_gaps, n_gaps = max(.n, 
                                                           na.rm = T)))
      message("up to ", n_gaps, " timesteps of missing data were interpolated per ensemble in stage 3 data")
    }
    target <- dplyr::mutate(dplyr::as_tibble(dplyr::mutate(tsibble::fill_gaps(tsibble::as_tsibble(dplyr::mutate(target, 
                                                                                                                time = lubridate::ymd_hm(time)), index = time, key = ensemble)), 
                                                           across(AirTemp:Snow, imputeTS::na_interpolation))), 
                            time = format(time, format = "%Y-%m-%d %H:%M", tz = "UTC"))
  }
  else {
    target <- NULL
  }
  if (is.null(forecast_dir)) {
    if (!is.null(obs_met_file)) {
      current_filename <- "met.csv"
      current_filename <- file.path(out_dir, current_filename)
      write.csv(target, file = current_filename, quote = FALSE, 
                row.names = FALSE)
    }
    else {
      ensemble_members <- unique(target$ensemble)
      current_filename <- purrr::map_chr(ensemble_members, 
                                         function(ens, out_dir, target) {
                                           df <- dplyr::arrange(dplyr::select(dplyr::filter(target, 
                                                                                            ensemble == ens), -ensemble), time)
                                           if (use_ler_vars) {
                                             df <- dplyr::rename(df, datetime = time, 
                                                                 Air_Temperature_celsius = AirTemp, Shortwave_Radiation_Downwelling_wattPerMeterSquared = ShortWave, 
                                                                 Longwave_Radiation_Downwelling_wattPerMeterSquared = LongWave, 
                                                                 Relative_Humidity_percent = RelHum, Ten_Meter_Elevation_Wind_Speed_meterPerSecond = WindSpeed, 
                                                                 Precipitation_millimeterPerHour = Rain, 
                                                                 Snowfall_millimeterPerHour = Snow)
                                           }
                                           missing_data_check(df)
                                           fn <- paste0("met_", stringr::str_pad(ens, 
                                                                                 width = 2, side = "left", pad = "0"), ".csv")
                                           fn <- file.path(out_dir, fn)
                                           write.csv(df, file = fn, quote = FALSE, row.names = FALSE)
                                           return(fn)
                                         }, out_dir, target)
    }
  }
  else {
    forecast <- dplyr::ungroup(dplyr::slice(dplyr::group_by(dplyr::select(dplyr::mutate(dplyr::mutate_at(dplyr::mutate(dplyr::rename(dplyr::mutate(dplyr::arrange(tidyr::pivot_wider(dplyr::collect(dplyr::select(dplyr::filter(arrow::open_dataset(forecast_dir), 
                                                                                                                                                                                                                                site_id == lake_name_code), datetime, parameter, 
                                                                                                                                                                                                                  variable, prediction)), names_from = variable, values_from = prediction), 
                                                                                                                                                                  parameter, datetime), WindSpeed = sqrt(eastward_wind^2 + 
                                                                                                                                                                                                           northward_wind^2)), AirTemp = air_temperature, ShortWave = surface_downwelling_shortwave_flux_in_air, 
                                                                                                                                     LongWave = surface_downwelling_longwave_flux_in_air, 
                                                                                                                                     RelHum = relative_humidity, Rain = precipitation_flux, 
                                                                                                                                     ensemble = parameter, time = datetime), AirTemp = AirTemp - 
                                                                                                                         273.15, RelHum = RelHum * 100, RelHum = ifelse(RelHum > 
                                                                                                                                                                          100, 100, RelHum), Rain = ifelse(use_ler_vars, Rain * 
                                                                                                                                                                                                             (60 * 60), Rain * (60 * 60 * 24)/1000), Snow = 0), 
                                                                                                         dplyr::vars(all_of(c("AirTemp", "ShortWave", "LongWave", 
                                                                                                                              "RelHum", "WindSpeed"))), list(~round(., 2))), 
                                                                                        Rain = round(Rain, 5), time = strftime(time, format = "%Y-%m-%d %H:%M", 
                                                                                                                               tz = "UTC")), ensemble, time, AirTemp, ShortWave, 
                                                                          LongWave, RelHum, WindSpeed, Rain, Snow), ensemble), 
                                            -dplyr::n()))
    ensemble_members <- unique(forecast$ensemble)
    current_filename <- purrr::map_chr(ensemble_members, 
                                       function(ens, out_dir, forecast, target) {
                                         if ("ensemble" %in% names(target)) {
                                           target <- dplyr::select(dplyr::filter(target, 
                                                                                 ensemble == ens), -ensemble)
                                         }
                                         df <- dplyr::arrange(dplyr::bind_rows(dplyr::select(dplyr::filter(forecast, 
                                                                                                           ensemble == ens), -ensemble), target), time)
                                         if (max(forecast$time) < strftime(end_datetime - 
                                                                           lubridate::hours(1), format = "%Y-%m-%d %H:%M", 
                                                                           tz = "UTC")) {
                                           stop(paste0("Weather forecasts do not cover full forecast horizon. Current max time: ", 
                                                       max(forecast$time), " ; Requested max time: ", 
                                                       strftime(end_datetime - lubridate::hours(1), 
                                                                format = "%Y-%m-%d %H:%M", tz = "UTC")))
                                         }
                                         if (use_ler_vars) {
                                           df <- dplyr::rename(df, datetime = time, Air_Temperature_celsius = AirTemp, 
                                                               Shortwave_Radiation_Downwelling_wattPerMeterSquared = ShortWave, 
                                                               Longwave_Radiation_Downwelling_wattPerMeterSquared = LongWave, 
                                                               Relative_Humidity_percent = RelHum, Ten_Meter_Elevation_Wind_Speed_meterPerSecond = WindSpeed, 
                                                               Precipitation_millimeterPerHour = Rain, Snowfall_millimeterPerHour = Snow)
                                         }
                                         FLAREr:::missing_data_check(df)
                                         fn <- paste0("met_", stringr::str_pad(ens, width = 2, 
                                                                               side = "left", pad = "0"), ".csv")
                                         fn <- file.path(out_dir, fn)
                                         write.csv(df, file = fn, quote = FALSE, row.names = FALSE)
                                         return(fn)
                                       }, out_dir = out_dir, forecast, target)
  }
  return(list(filenames = current_filename, historical_met_error = FALSE))
}
