## function to collect and clean insitu data

collect_insitu_targets <- function(obs_download, site_location, assign_depth){
  
  # remove duplicates 
  obs_dedup <- obs_download |> 
    distinct(datetime, Height, variable, .keep_all = TRUE) |> 
    select(-QC, -Date)
  
  obs_df_wide <- obs_dedup |> pivot_wider(names_from = variable, values_from = Data) |> rename(salt = `Salinity (ppt)`, temperature = 'Temperature')
  
  obs_df <- obs_df_wide |> pivot_longer(cols = c('temperature','salt'),
                                        names_to = 'variable', 
                                        values_to = 'observation')
  
  ## assign columns
  obs_df$site_id <- site_location
  
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
  
  return(cleaned_insitu_file)
}
