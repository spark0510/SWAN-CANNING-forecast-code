#' awss3Connect 
#'
#' @description Establishes connection to the S3 bucket and fetches data 
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


awss3Connect <- function(filename){
  
  # Now set bucket contents as objects
  bucket <- 'scevo-data'

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
  
  
  fetchedData <- aws.s3::s3read_using(FUN = utils::read.csv,
                                      check.names = FALSE,
                                      #encoding = "UTF-8",
                                      # show_col_types = FALSE,
                                      # lazy = FALSE,
                                      # progress = FALSE,
                                      object = filename,
                                      bucket = bucket,
                                      filename = basename(filename),
                                      opts = list(
                                        base_url = "projects.pawsey.org.au",
                                        region = "",
                                        key = "2f1a9d81bdf24a178b2bd18d530e959b",
                                        secret = "e062073c1faf488cb4209ba8de2eb483"))

 
  return(fetchedData)
}
  
