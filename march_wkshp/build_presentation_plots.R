library(tidyverse)
library(arrow)


### FREYA'S CODE

library(tidyverse)

# Hindcasts -----------
s3_hindcast_forecasts <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/hindcast/forecasts', 
                                          endpoint_override = 'renc.osn.xsede.org')

s3_hindcast_scores <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/hindcast/scores', 
                                       endpoint_override = 'renc.osn.xsede.org')

### ALEX PLOTTING CODE ####

hindcast <- arrow::open_dataset(s3_hindcast_forecasts) |> 
  filter(site_id == 'ALEX',
         depth == 0.5, 
         variable == 'temperature') |> 
  collect()

hindcast_scores <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'ALEX',
         depth == 0.5, 
         variable == 'temperature') |> 
  collect()

par <- c('lw_factor', 'zone1temp', 'zone2temp')

hindcast_pars <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'ALEX',
         variable %in% par) |> 
  collect()

forecast_start <- unique(hindcast_scores$reference_date)

# plot the full year + forecast period
ggplot(hindcast_scores, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-02-17')) +
  ylab('Temperature (degC)') +
  theme_bw()


ggplot(hindcast_scores, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  coord_cartesian(xlim = c(max(hindcast_scores$datetime) - days(120),
                           max(hindcast_scores$datetime))) +
  ylab('Temperature (degC)') +
  theme_bw() + 
  geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed')
  
# plot salt
hindcast_salt <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'ALEX', 
         variable == 'salt',
         depth == 0.5,
         reference_datetime == as_datetime('2024-02-17')) |>  
  collect()

ggplot(hindcast_salt, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-02-17')) +
  ylab('Salinity (ppt)') +
  theme_bw()

# water level plot
hindcast_water_level <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'ALEX', 
         variable == 'depth', 
         reference_datetime == as_datetime('2024-02-17')) |>  
  collect()

ggplot(hindcast_water_level, aes(x=datetime)) +
  geom_line(aes(y=mean, colour = model_id)) +
  #geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-02-17')) +
  ylab('Water Level (m)') +
  theme_bw()

# parameter evolution
hindcast_pars |> 
  filter(model_id == 'glm_flare_v1') |> 
  ggplot(aes(x=datetime, y=mean, colour = model_id)) +
  geom_line() +
  facet_wrap(~variable, scales = 'free')

# Reforecasts --------
s3_reforecast_forecasts <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/reforecast/forecasts', 
                                            endpoint_override = 'renc.osn.xsede.org')

s3_reforecast_scores <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/reforecast/scores', 
                                         endpoint_override = 'renc.osn.xsede.org')

reforecasts <- arrow::open_dataset(s3_reforecast_forecasts) |> 
  filter(site_id == 'ALEX',
         depth == 0.5, 
         variable == 'temperature') |> 
  collect()

reforecasts_scores <- arrow::open_dataset(s3_reforecast_scores) |> 
  filter(site_id == 'ALEX',
         depth == 0.5, 
         variable == 'temperature') |> 
  collect() 

# calculate overall metrics
reforecasts_scores |> 
  mutate(sq_err = (mean - observation)^2) |> 
  summarise(rmse = sqrt(mean(sq_err)),
            crps = mean(crps))

reforecasts |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime, y= prediction, group = parameter)) +
  geom_line() +
  geom_vline(xintercept = as_datetime('2023-09-01'), linetype = 'dashed')

reforecasts_scores |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime, y= mean)) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5), alpha = 0.3) +
  geom_line() +
  geom_point(aes(y=observation), colour = 'red') +
  geom_vline(xintercept = as_datetime('2023-09-01'), linetype = 'dashed')

reforecasts_scores |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime, y= crps)) +
  geom_line() +
  geom_vline(xintercept = as.Date('2023-09-01'))


## plot example forecast 
reforecasts_scores |>
  filter(reference_date == '2023-10-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #coord_cartesian(xlim = c(max(hindcast_scores$datetime) - days(120),
   #                        max(hindcast_scores$datetime))) +
  ylab('Temperature (degC)') +
  theme_bw() + 
  geom_vline(xintercept = as_datetime('2023-10-01'), linetype = 'dashed')

# score over horizon
reforecasts_scores |> 
  filter(horizon > 0) |>
  mutate(sq_err = (mean - observation)^2) |> 
  group_by(horizon) |> 
  summarise(rmse = sqrt(mean(sq_err)),
            crps = mean(crps),
            logs = mean(logs), .groups = 'drop') |> 
  ggplot(aes(x=horizon, y=crps)) +
  geom_line() +
  theme_bw()

reforecasts_scores |> 
  filter(horizon > 0) |>
  mutate(sq_err = (mean - observation)^2) |> 
  group_by(horizon) |> 
  summarise(rmse = sqrt(mean(sq_err)),
            crps = mean(crps),
            logs = mean(logs), .groups = 'drop') |> 
  pivot_longer(cols = rmse:logs, names_to = 'metric', values_to = 'score') |> 
  ggplot(aes(x=horizon, y=score)) +
  geom_line() +
  facet_wrap(~metric, scales = 'free') +
  labs(title = reforecasts_scores$site_id[1],
       subtitle = paste0('Forecasts generated for ', 
                         min(reforecasts_scores$reference_date), 
                         ' to ', 
                         max(reforecasts_scores$reference_date)),
       x = 'forecast horizon (days ahead)') +
  theme_bw()





#### SWAN-CANNING PLOTTING CODE #####
# Hindcasts -----------
s3_hindcast_forecasts <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/hindcast/forecasts', 
                                          endpoint_override = 'renc.osn.xsede.org')

s3_hindcast_scores <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/hindcast/scores', 
                                       endpoint_override = 'renc.osn.xsede.org')
hindcast_sc <- arrow::open_dataset(s3_hindcast_forecasts) |> 
  filter(site_id == 'CANN',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect()

hindcast_scores_sc <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'CANN',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect()

par_sc <- c('lw_factor', 'zone1temp', 'zone2temp')

hindcast_pars_sc <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'CANN',
         variable %in% par_sc) |> 
  collect()

forecast_start <- unique(hindcast_scores_sc$reference_date)

# plot the full year + forecast period
ggplot(hindcast_scores_sc, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-03-01')) +
  ylab('Temperature (degC)') +
  theme_bw()


ggplot(hindcast_scores_sc, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  coord_cartesian(xlim = c(max(hindcast_scores$datetime) - days(120),
                           max(hindcast_scores$datetime))) +
  ylab('Temperature (degC)') +
  theme_bw() + 
  geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed')

# plot salt
hindcast_salt_sc <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'CANN', 
         variable == 'salt',
         depth == 1.5) |>  
  collect()

ggplot(hindcast_salt_sc, aes(x=datetime)) +
  geom_point(aes(y=(observation/1000)), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-03-20')) +
  ylab('Salinity (ppt)') +
  theme_bw()

# water level plot
hindcast_water_level_sc <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'CANN', 
         variable == 'depth') |>  
  collect()

ggplot(hindcast_water_level_sc, aes(x=datetime)) +
  geom_line(aes(y=mean, colour = model_id)) +
  #geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-03-01')) +
  ylab('Water Level (m)') +
  theme_bw()

# parameter evolution
hindcast_pars_sc |> 
  filter(model_id == 'glm_annual_run') |> 
  ggplot(aes(x=datetime, y=mean, colour = model_id)) +
  geom_line() +
  facet_wrap(~variable, scales = 'free')

# Reforecasts --------
s3_reforecast_forecasts <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/reforecast/forecasts', 
                                            endpoint_override = 'renc.osn.xsede.org')

s3_reforecast_scores <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/reforecast/scores', 
                                         endpoint_override = 'renc.osn.xsede.org')

reforecasts <- arrow::open_dataset(s3_reforecast_forecasts) |> 
  filter(site_id == 'CANN',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect()

reforecasts_scores <- arrow::open_dataset(s3_reforecast_scores) |> 
  filter(site_id == 'CANN',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect() 


reforecasts_scores |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  #coord_cartesian(xlim = c(max(hindcast_scores$datetime) - days(120),
  #                        max(hindcast_scores$datetime))) +
  ylab('Temperature (degC)') +
  theme_bw() + 
  geom_vline(xintercept = as_datetime('2023-09-01'), linetype = 'dashed')


# calculate overall metrics
reforecasts_scores |> 
  mutate(sq_err = (mean - observation)^2) |> 
  summarise(rmse = sqrt(mean(sq_err)),
            crps = mean(crps))

reforecasts |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime, y= prediction, group = parameter)) +
  geom_line() +
  geom_vline(xintercept = as_datetime('2023-09-01'), linetype = 'dashed')

reforecasts_scores |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime, y= mean)) +
  geom_ribbon(aes(ymax = quantile97.5, ymin = quantile02.5), alpha = 0.3) +
  geom_line() +
  geom_point(aes(y=observation), colour = 'red') +
  geom_vline(xintercept = as_datetime('2023-09-01'), linetype = 'dashed')

reforecasts_scores |>
  filter(reference_date == '2023-09-01', 
         model_id == 'glm_flare_example') |> 
  ggplot(aes(x=datetime, y= crps)) +
  geom_line() +
  geom_vline(xintercept = as.Date('2023-09-01'))

# score over horizon
reforecasts_scores |> 
  filter(horizon > 0) |>
  mutate(sq_err = (mean - observation)^2) |> 
  group_by(horizon) |> 
  summarise(rmse = sqrt(mean(sq_err)),
            crps = mean(crps),
            logs = mean(logs), .groups = 'drop') |> 
  ggplot(aes(x=horizon, y=crps)) +
  geom_line() +
  theme_bw()

reforecasts_scores |> 
  filter(horizon > 0) |>
  mutate(sq_err = (mean - observation)^2) |> 
  group_by(horizon) |> 
  summarise(rmse = sqrt(mean(sq_err)),
            crps = mean(crps),
            logs = mean(logs), .groups = 'drop') |> 
  pivot_longer(cols = rmse:logs, names_to = 'metric', values_to = 'score') |> 
  ggplot(aes(x=horizon, y=score)) +
  geom_line() +
  facet_wrap(~metric, scales = 'free') +
  labs(title = reforecasts_scores$site_id[1],
       subtitle = paste0('Forecasts generated for ', 
                         min(reforecasts_scores$reference_date), 
                         ' to ', 
                         max(reforecasts_scores$reference_date)),
       x = 'forecast horizon (days ahead)') +
  theme_bw()




##### FCR PLOTTING CODE ####
# Hindcasts -----------
s3_hindcast_forecasts <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/hindcast/forecasts', 
                                          endpoint_override = 'renc.osn.xsede.org')

s3_hindcast_scores <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/hindcast/scores', 
                                       endpoint_override = 'renc.osn.xsede.org')

hindcast <- arrow::open_dataset(s3_hindcast_forecasts) |> 
  filter(site_id == 'fcre',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect()

hindcast_scores <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'fcre',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect()

par <- c('lw_factor', 'zone1temp', 'zone2temp')

hindcast_pars <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'fcre',
         variable %in% par) |> 
  collect()

forecast_start <- unique(hindcast_scores$reference_date)

# plot the full year + forecast period
ggplot(hindcast_scores, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  geom_vline(xintercept = as_datetime(forecast_start), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-03-20')) +
  ylab('Temperature (degC)') +
  theme_bw()


ggplot(hindcast_scores, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  coord_cartesian(xlim = c(max(hindcast_scores$datetime) - days(120),
                           max(hindcast_scores$datetime))) +
  ylab('Temperature (degC)') +
  theme_bw() + 
  geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed')

# plot salt
hindcast_salt <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'ALEX', 
         variable == 'salt',
         depth == 0.5,
         reference_datetime == as_datetime('2024-02-17')) |>  
  collect()

ggplot(hindcast_salt, aes(x=datetime)) +
  geom_point(aes(y=observation), alpha = 0.5) +
  geom_line(aes(y=mean, colour = model_id)) +
  geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-03-20')) +
  ylab('Salinity (ppt)') +
  theme_bw()

# water level plot
hindcast_water_level <- arrow::open_dataset(s3_hindcast_scores) |> 
  filter(site_id == 'ALEX', 
         variable == 'depth', 
         reference_datetime == as_datetime('2024-02-17')) |>  
  collect()

ggplot(hindcast_water_level, aes(x=datetime)) +
  geom_line(aes(y=mean, colour = model_id)) +
  #geom_ribbon(aes(ymin = quantile02.5, ymax = quantile97.5, fill = model_id), alpha = 0.2) +
  geom_vline(xintercept = as_datetime(forecast_start[2]), linetype = 'dashed') +
  xlim(as_datetime('2023-03-02'), as_datetime('2024-03-20')) +
  ylab('Water Level (m)') +
  theme_bw()

# parameter evolution
hindcast_pars |> 
  filter(model_id == 'glm_flare_v1') |> 
  ggplot(aes(x=datetime, y=mean, colour = model_id)) +
  geom_line() +
  facet_wrap(~variable, scales = 'free')

### REFORECAST 
s3_reforecast_forecasts <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/reforecast/forecasts', 
                                            endpoint_override = 'renc.osn.xsede.org')

s3_reforecast_scores <- arrow::s3_bucket('bio230121-bucket01/march24-workshop/reforecast/scores', 
                                         endpoint_override = 'renc.osn.xsede.org')

reforecasts <- arrow::open_dataset(s3_reforecast_forecasts) |> 
  filter(site_id == 'fcre',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect()

reforecasts_scores <- arrow::open_dataset(s3_reforecast_scores) |> 
  filter(site_id == 'fcre',
         depth == 1.5, 
         variable == 'temperature') |> 
  collect() 



## PLOT WEEKLY FORECASTS FOR QUINN ##
reforecasts_weekly <- reforecasts_scores |> 
  mutate(weekly_date = floor_date(reference_datetime, "week")) |>
  group_by(weekly_date, datetime) |>
  slice_head(n=1)

  # group_by(weekly_date, parameter) |> 
  # mutate(weekly_temp = mean(prediction)) |> 
  # ungroup() |> 
  # distinct(weekly_date, parameter, .keep_all = TRUE)


ggplot(reforecasts_weekly, aes(x= datetime,y = mean)) + 
  geom_line() +
  geom_point(aes(y=observation), alpha = 0.5) +
  facet_wrap(~weekly_date, scales = 'free')



par <- c('lw_factor', 'zone1temp', 'zone2temp')

reforecast_pars <- arrow::open_dataset(s3_reforecast_scores) |> 
  filter(site_id == 'fcre',
         variable %in% par) |> 
  collect() |> 
  mutate(weekly_date = floor_date(reference_datetime, "week")) |>
  group_by(weekly_date, datetime, variable) |>
  slice_head(n=1)


lw_factor <- reforecast_pars |> filter(variable == par[1])
ggplot(lw_factor, aes(x= datetime,y = mean)) + 
  geom_line() +
  #geom_point(aes(y=observation), alpha = 0.5) +
  facet_wrap(~weekly_date, scales = 'free')


z1_factor <- reforecast_pars |> filter(variable == par[2])
ggplot(z1_factor, aes(x= datetime,y = mean)) + 
  geom_line() +
  #geom_point(aes(y=observation), alpha = 0.5) +
  facet_wrap(~weekly_date, scales = 'free')

z2_factor <- reforecast_pars |> filter(variable == par[3])
ggplot(z2_factor, aes(x= datetime,y = mean)) + 
  geom_line() +
  #geom_point(aes(y=observation), alpha = 0.5) +
  facet_wrap(~weekly_date, scales = 'free')
