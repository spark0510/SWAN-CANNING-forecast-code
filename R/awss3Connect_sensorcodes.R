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
  'AWS_ACCESS_KEY_ID' = '2f1a9d81bdf24a178b2bd18d530e959b', 
  'AWS_SECRET_ACCESS_KEY' = 'e062073c1faf488cb4209ba8de2eb483'
)


awss3Connect_sensorcode <- function(sensorCodes){
  
  # Now set bucket contents as objects
  bucket <- 'scevo-data'
  
  ## REMOVED fetchedData here -- contained additional AWS secrets
  
  fetchedData  <- data.frame()
  
  for (i in sensorCodes) {
    object <- paste0("/arms/",i,".csv")
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
    sensorData <- sensorData[,c('Date','Data')]
    sensorData$s_table_name <- i
    fetchedData <- rbind(fetchedData, sensorData)
  }
  
  fetchedData$datetime  <- as.POSIXlt(
    fetchedData$Date*86400,
    origin=structure(-210866760000,
                     class=c("POSIXct", "POSIXt"),
                     tzone="Australia/Perth"),
    tz="Australia/Perth"
  )
  
  return(fetchedData)
}

