#' awss3Connect_sensorcode 
#'
#' @description Establishes connection to the S3 bucket and fetches data in ARMS format (sensor code as filename) 
#'
#' @return The return value, if any, from executing the function.
#' @import aws.s3 readr
#' @noRd

awss3Connect_sensorcode <- function(sensorCodes, code_df, server_name){
  
  # Now set bucket contents as objects
  #bucket <- 'scevo-data'
  
  fetchedData  <- data.frame()
  
  for (i in sensorCodes) {
    object <- paste0("/arms/",i,".csv")
    print(object)
    #object <- paste0("/arms/",sensorCodes[i],".csv")
    #sensorData <- aws.s3::s3read_using(FUN = utils::read.csv,
    #                                   check.names = FALSE,
                                       #encoding = "UTF-8",
                                       # show_col_types = FALSE,
                                       # lazy = FALSE,
                                       # progress = FALSE,
    #                                   object = object,
    #                                   bucket = bucket,
    #                                   filename = basename(object),
    #                                   opts = list(
    #                                     base_url = Sys.getenv('SC_S3_ENDPOINT'),
    #                                     region = "",
    #                                     key = Sys.getenv('SC_AWS_KEY'),
    #                                     secret = Sys.getenv('SC_AWS_SECRET')))
    FaaSr::faasr_get_files(server_name=server_name , 
                           remote_folder="", 
                           remote_file=object, 
                           local_folder=".", 
                           local_file=basename(object))    
    sensorData <- utils::read.csv(basename(object))
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
  
  fetchedData <- fetchedData #|> 
    #dplyr::select(datetime, variable, Height, Data)
  
  return(fetchedData)
}

