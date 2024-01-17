#' awss3Connect_sensorcode 
#'
#' @description Establishes connection to the S3 bucket and fetches data in ARMS format (sensor code as filename) 
#'
#' @return The return value, if any, from executing the function.
#' @import aws.s3 readr
#' @noRd


library('aws.s3')

# To enforce HTTPS, should be set to TRUE
Sys.setenv('USE_HTTPS' = TRUE)

# Set details for bucket origin
Sys.setenv(
  'AWS_DEFAULT_REGION' = '', 
  'AWS_S3_ENDPOINT' = 'projects.pawsey.org.au', 
  'AWS_ACCESS_KEY_ID' = Sys.getenv('SC_AWS_KEY'), 
  'AWS_SECRET_ACCESS_KEY' = Sys.getenv('SC_AWS_SECRET')
)


awss3Connect_sensorcode <- function(sensorCodes, code_df){
  
  # Now set bucket contents as objects
  bucket <- 'scevo-data'
  
  fetchedData  <- data.frame()
  
  for (i in sensorCodes) {
    object <- paste0("/arms/",i,".csv")
    #object <- paste0("/arms/",sensorCodes[i],".csv")
    sensorData <- aws.s3::s3read_using(FUN = utils::read.csv,
                                       check.names = FALSE,
                                       #encoding = "UTF-8",
                                       # show_col_types = FALSE,
                                       # lazy = FALSE,
                                       # progress = FALSE,
                                       object = object,
                                       bucket = bucket,
                                       filename = basename(object),
                                       opts = list(
                                         base_url = "projects.pawsey.org.au",
                                         region = "",
                                         key = Sys.getenv('SC_AWS_KEY'),
                                         secret = Sys.getenv('SC_AWS_SECRET')))
    #sensorData <- sensorData[,c('Date','Data')]
    sensorData$s_table_name <- i
    sensorData$variable <- code_df[which(code_df$s_table_name == i),'s_graph_value'][[1]]
    fetchedData <- rbind(fetchedData, sensorData)
  }
  
  fetchedData$datetime  <- as.POSIXlt(
    fetchedData$Date*86400,
    origin=structure(-210866760000,
                     class=c("POSIXct", "POSIXt"),
                     tzone="Australia/Perth"),
    tz="Australia/Perth"
  )
  
  fetchedData <- fetchedData |> dplyr::select(datetime, variable, Height, Data)
  
  return(fetchedData)
}

