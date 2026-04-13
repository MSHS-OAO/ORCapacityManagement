prime_time_location <- function(data){
  # Assumes the data here is from DB Table
  data <- data %>% mutate(
    Weekday = weekdays(SURGERY_DATE),
    ABSOLUTE_SLOT_START = as.character(SURGERY_DATE),
    ABSOLUTE_SLOT_END = as.character(SURGERY_DATE),
    ABSOLUTE_SLOT_START =  ifelse(str_detect(LOCATION_NAME, pattern = "MSH") & Weekday!='Wednesday',
                                paste(as.character(SURGERY_DATE), '8:00:00'),
                                ifelse(str_detect(LOCATION_NAME, pattern = "MSM") & Weekday=='Wednesday',
                                       paste(as.character(SURGERY_DATE), '7:30:00'),
                                       ifelse(str_detect(LOCATION_NAME, pattern = "MSM") & Weekday!='Wednesday',
                                              paste(as.character(SURGERY_DATE), '8:30:00'), 
                                              ifelse((str_detect(LOCATION_NAME, pattern = "MSDC") | str_detect(LOCATION_NAME, pattern = "MSDUS") |  str_detect(LOCATION_NAME, pattern = "MSW")) & Weekday=='Wednesday',
                                                     paste(as.character(SURGERY_DATE), '8:30:00'),
                                                     ifelse((str_detect(LOCATION_NAME, pattern = "MSDC") | str_detect(LOCATION_NAME, pattern = "MSDUS") |  str_detect(LOCATION_NAME, pattern = "MSW")) & Weekday!='Wednesday',
                                                            paste(as.character(SURGERY_DATE), '7:30:00'),
                                                            ifelse(str_detect(LOCATION_NAME, pattern = "MSQ"),
                                                                   paste(as.character(SURGERY_DATE), '8:00:00'),
                                                                   ifelse(str_detect(LOCATION_NAME, pattern = "MSB"),
                                                                          paste(as.character(SURGERY_DATE), '8:00:00'),
                                                                          ifelse(str_detect(LOCATION_NAME, pattern = "MSH") & Weekday=='Wednesday',
                                                                                 paste(as.character(SURGERY_DATE), '9:00:00'),NA)
                                                                   )
                                                            )
                                                     )
                                              )
                                       )
                                )
    ),
    ABSOLUTE_SLOT_END = ifelse(str_detect(LOCATION_NAME, pattern = "MSH") & Weekday!='Wednesday',
                             paste(as.character(SURGERY_DATE), '18:00:00'),
                             ifelse(str_detect(LOCATION_NAME, pattern = "MSM") & Weekday=='Wednesday',
                                    paste(as.character(SURGERY_DATE), '17:30:00'),
                                    ifelse(str_detect(LOCATION_NAME, pattern = "MSM") & Weekday!='Wednesday',
                                           paste(as.character(SURGERY_DATE), '17:30:00'), 
                                           ifelse((str_detect(LOCATION_NAME, pattern = "MSDC") | str_detect(LOCATION_NAME, pattern = "MSDUS") |  str_detect(LOCATION_NAME, pattern = "MSW")) & Weekday=='Wednesday',
                                                  paste(as.character(SURGERY_DATE), '17:30:00'),
                                                  ifelse((str_detect(LOCATION_NAME, pattern = "MSDC") | str_detect(LOCATION_NAME, pattern = "MSDUS") |  str_detect(LOCATION_NAME, pattern = "MSW")) & Weekday!='Wednesday',
                                                         paste(as.character(SURGERY_DATE), '17:30:00'), 
                                                         ifelse(str_detect(LOCATION_NAME, pattern = "MSQ"),
                                                                paste(as.character(SURGERY_DATE), '18:00:00'), 
                                                                ifelse(str_detect(LOCATION_NAME, pattern = "MSB"),
                                                                       paste(as.character(SURGERY_DATE), '15:00:00'),
                                                                       ifelse(str_detect(LOCATION_NAME, pattern = "MSH") & Weekday=='Wednesday',
                                                                              paste(as.character(SURGERY_DATE), '18:00:00'), NA)))))))),
    ABSOLUTE_SLOT_START =  as.POSIXct(ABSOLUTE_SLOT_START,format='%Y-%m-%d %H:%M:%S',tz = "America/New_York"),
    ABSOLUTE_SLOT_END = as.POSIXct(ABSOLUTE_SLOT_END,format='%Y-%m-%d %H:%M:%S',tz = "America/New_York"),
    
  )
}