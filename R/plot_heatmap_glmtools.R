plot_nc_heatmap <- function(file, var_name, reference, legend.title , interval,
                             text.size, show.legend, legend.position, plot.title,
                             color.palette, color.direction, zlim) {
  
  surface <- get_surface_height(file)
  max_depth <- max(surface[, 2])
  min_depth <- 0
  z_out <- seq(min_depth, max_depth,by = interval) # Set plotting interval
  # Get data from .nc file
  data = get_var(file, var_name = var_name, z_out = z_out, reference = reference)
  # Get units
  units = sim_var_units(file, var_name = var_name)
  
  if (reference == 'surface'){
    names.df = data.frame(names = names(data)[-1], depth.numeric = z_out, stringsAsFactors = F)
    # ylabel = 'Depth (m)'
  }
  if (reference == 'bottom'){
    names.df = data.frame(names = names(data)[-1], depth.numeric = rev(z_out), stringsAsFactors = F)
    # ylabel = 'Elevation (m)'
  }
  
  dataLong = gather(data = data, 
                    key = "depth", value = !!var_name, -all_of("DateTime")) %>%
    left_join(names.df, by = c('depth' = 'names')) 
  
  if(is.null(legend.title)) {
    legend.title = .unit_label(file, var_name)
  }
  .plot_df_heatmap(dataLong, var_name, legend.title, text.size, show.legend, legend.position, plot.title,
                   color.palette, color.direction, zlim)
}
