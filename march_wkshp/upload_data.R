## Move hindcasts into the march worksop bucket 

s3 <- arrow::s3_bucket('bio230121-bucket01', endpoint_override = 'renc.osn.xsede.org')

#s3_start <- arrow::s3_bucket('bio230121-bucket01/flare/scores/parquet/site_id=CANN', endpoint_override = 'renc.osn.xsede.org')

# hindcast
arrow::open_dataset(file.path(lake_directory, 'forecasts/parquet/')) |> 
  filter(model_id %in% c('glm_annual_run', 'glm_annual_run_noDA')) |> 
  arrow::write_dataset(s3$path('march24-workshop/hindcast/forecasts'), partitioning = 'site_id')

arrow::open_dataset(file.path(lake_directory, 'scores/parquet/')) |> 
  filter(model_id %in% c('glm_annual_run', 'glm_annual_run_noDA')) |> 
  arrow::write_dataset(s3$path('march24-workshop/hindcast/scores'), partitioning = 'site_id')
