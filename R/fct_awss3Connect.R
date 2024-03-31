#' awss3Connect 
#'
#' @description Establishes connection to the S3 bucket and fetches data 
#'
#' @return The return value, if any, from executing the function.
#' @import aws.s3 readr
#' @noRd

awss3Connect <- function(filename, server_name){
  
  # Now set bucket contents as objects
  #bucket <- 'scevo-data'

  #filename = 'data-warehouse/dbca/wiski/DBCA_data_export_2023-07-19_1615.csv'
  # fetchedData <- aws.s3::s3read_using(FUN = utils::read.csv,
  #                                     check.names = FALSE,
  #                                     encoding = "UTF-8",
  #                                # show_col_types = FALSE,
  #                                # lazy = FALSE,
  #                                # progress = FALSE,
  #                                object = filename,
  #                                bucket = bucket,
  #                                filename = basename(filename),
  #                                opts = list(
  #                                  base_url = "projects.pawsey.org.au",
  #                                  region = ""))
  
  
  #fetchedData <- aws.s3::s3read_using(FUN = utils::read.csv,
  #                                    check.names = FALSE,
                                      #encoding = "UTF-8",
                                      # show_col_types = FALSE,
                                      # lazy = FALSE,
                                      # progress = FALSE,
  #                                    object = filename,
  #                                    bucket = bucket,
  #                                    filename = basename(filename),
  #                                    opts = list(
  #                                      base_url = Sys.getenv('SC_S3_ENDPOINT'),
  #                                      region = "",
  #                                      key = Sys.getenv('SC_AWS_KEY'),
  #                                      secret = Sys.getenv('SC_AWS_SECRET')))
  FaaSr::faasr_get_files(server_name=server_name , 
                         remote_folder="", 
                         remote_file=filename, 
                         local_folder=".", 
                         local_file=basename(filename))
  fetchedData <- utils::read.csv(basename(filename))
 
  return(fetchedData)
}
  
