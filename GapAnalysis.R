rm(list = ls())
library(timeDate)
library(readxl)
library(bizdays)
library(dplyr)
library(lubridate)
library(reshape2)
library(knitr)
# library(gdtools)
# library(kableExtra)
library(kableExtra, "~/R/x86_64-pc-linux-gnu-library/4.2")
library(formattable)
library(rmarkdown)
library(stringr)
library(writexl)
library(gsubfn)
library(tidyr)
library(pool)
library(DBI)
library(odbc)
library(dbplyr)
library(glue)
library(assertr)
library(doParallel)
# library(blastula)
library(readr)
library(zip)
library(here)
library(hms)
library(ggplot2)
library(patchwork)
# library(ggh4x) # for x axis




recastTimezone.POSIXct <- function(x, tz) return(
  as.POSIXct(as.character(x), origin = as.POSIXct("1970-01-01"), tz = tz))


# Establish DB Connection and Get data ----
dsn <- "OAO Cloud DB Production"
conn <- dbConnect(odbc(), dsn)
source_table_name <- 'MS_INSIGHT.OR_QUALITY_DASHBOARD_CASE_DETAILS'

current_date <- Sys.Date()
# sched_date <- '2024-09-01'
sched_date <- as.character(current_date - months(13))
status <- 'Completed'
query <- glue("SELECT * 
              FROM {source_table_name} 
              WHERE SURGERY_DATE >=TO_DATE('{sched_date}','YYYY-MM-DD') AND 
              CASE_STATUS = '{status}' AND
              FACILITY not like '%IR%' AND FACILITY not like '%L&D%';") 
schedule_data <- dbGetQuery(conn, query)
dbDisconnect(conn)

# dbReadTable(conn, "MS_INSIGHT.OR_QUALITY_DASHBOARD_CASE_DETAILS")


# Set the path to Shared Drive -----
if ("Presidents" %in% list.files("/SharedDrive/")) {
  user_directory <- paste0("/SharedDrive/deans/Presidents/HSPI-PM/",
                           "Operations Analytics and Optimization/Projects/",
                           "System Operations/OR Capacity Management/")
} else {
  user_directory <- paste0("/SharedDrive/deans/Presidents/HSPI-PM/",
                           "Operations Analytics and Optimization/Projects/",
                           "System Operations/OR Capacity Management/")
}

# MSHS Colors ----
mshs_colors <- c("#221F72", "#00AEFF", "#D80B8C", "#7F7F7F")


# Get the cascasde data  and processing ----
ORSchedules <- read_xlsx(paste0(user_directory,"/SupplementData/ORSchedules.xlsx"),col_types = c("text","numeric","text", "text"))
ORSchedules <- ORSchedules %>%
  mutate(TimeStartPSXCT = as.POSIXct(format(as.POSIXct(`Time Start`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         TimeEndPSXCT = as.POSIXct(format(as.POSIXct(`Time End`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         TimeMinutes = as.numeric(difftime(TimeEndPSXCT, TimeStartPSXCT, units = "mins")),
         TotalAvailableTimeMinutes =TimeMinutes*`# ORs` ) %>%
  arrange(TimeStartPSXCT)

ORSchedulesKeepAllOpenPrimeTime <- ORSchedules %>%
  group_by(Location) %>%
  mutate(`# ORs` = max(`# ORs`, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(TotalAvailableTimeMinutes =TimeMinutes*`# ORs` )

ORAvailabilityKeepAllOpenPrimeTime <- ORSchedulesKeepAllOpenPrimeTime %>%
  group_by(Location) %>%
  summarise(TotalAvailableTimeMinutesAggKeepAllOpenPrimeTime = sum(TotalAvailableTimeMinutes))



# TAT Data
TAT_Hospital <- read_xlsx(paste0(user_directory,"/SupplementData/TAT.xlsx"), sheet = 'TAT Location',col_types = c("text","numeric"))
TAT_Cluster <- read_xlsx(paste0(user_directory,"/SupplementData/TAT.xlsx"), sheet = 'TAT Cluster',col_types = c("text","numeric"))

# Case Cluster Data
ClusterInfo <- read_xlsx(paste0(user_directory,"/SupplementData/CaseClusterInfo.xlsx"),col_types = c("text","text","text","text","text"))


# Subset Necessary Columns ----
schedule_data_needed_cols <- schedule_data %>%
  select(FACILITY,
         HOSPITAL,
         CLUSTER_NAME,
         OR_CASE_ID,
         OR_ID, # Essentially room number, will be present unless it's L(abour)&D(elivery) or Interventional Radiology OR_ID -> CLuster Name -> Facility -> Hospital
         PATIENT_IN_ROOM_DTTM,
         PATIENT_OUT_ROOM_DTTM,
         MINUTES_IN_ROOM_TO_OUT_ROOM,
         SURGERY_DATE) 





schedule_data_needed_cols <- left_join(schedule_data_needed_cols,ClusterInfo,
                                       by = c("OR_CASE_ID" = "OR Case ID"))


schedule_data_needed_cols <- left_join(schedule_data_needed_cols,TAT_Cluster,
                                       by = c("Cluster" = "Cluster"))
schedule_data_needed_cols <- left_join(schedule_data_needed_cols,TAT_Hospital,
                                       by = c("FACILITY" = "Location"))
schedule_data_needed_cols <-  schedule_data_needed_cols %>%
  mutate(`Avg TAT` = if_else(is.na(`Average of TAT Cluster`),`Average of TAT Location`,`Average of TAT Cluster`))

# function to derive Prime Time for Location ----

prime_time_location <- function(data){
  # Assumes the data here is from DB Table
  data <- data %>% mutate(
    Weekday = weekdays(SURGERY_DATE),
    `Available Start` = as.character(SURGERY_DATE),
    `Available End` = as.character(SURGERY_DATE),
    `Available Start` =  ifelse(str_detect(HOSPITAL, pattern = "MSH") & Weekday!='Wednesday',
                                paste(as.character(SURGERY_DATE), '8:00:00'),
                                ifelse(str_detect(HOSPITAL, pattern = "MSM") & Weekday=='Wednesday',
                                       paste(as.character(SURGERY_DATE), '7:30:00'),
                                       ifelse(str_detect(HOSPITAL, pattern = "MSM") & Weekday!='Wednesday',
                                              paste(as.character(SURGERY_DATE), '8:30:00'), 
                                              ifelse((str_detect(HOSPITAL, pattern = "MSDC") | str_detect(HOSPITAL, pattern = "MSDUS") |  str_detect(HOSPITAL, pattern = "MSW")) & Weekday=='Wednesday',
                                                     paste(as.character(SURGERY_DATE), '8:30:00'),
                                                     ifelse((str_detect(HOSPITAL, pattern = "MSDC") | str_detect(HOSPITAL, pattern = "MSDUS") |  str_detect(HOSPITAL, pattern = "MSW")) & Weekday!='Wednesday',
                                                            paste(as.character(SURGERY_DATE), '7:30:00'),
                                                            ifelse(str_detect(HOSPITAL, pattern = "MSQ"),
                                                                   paste(as.character(SURGERY_DATE), '8:00:00'),
                                                                   ifelse(str_detect(HOSPITAL, pattern = "MSB"),
                                                                          paste(as.character(SURGERY_DATE), '8:00:00'),
                                                                          ifelse(str_detect(HOSPITAL, pattern = "MSH") & Weekday=='Wednesday',
                                                                                 paste(as.character(SURGERY_DATE), '9:00:00'),NA)
                                                                   )
                                                            )
                                                     )
                                              )
                                       )
                                )
    ),
    `Available End` = ifelse(str_detect(HOSPITAL, pattern = "MSH") & Weekday!='Wednesday',
                             paste(as.character(SURGERY_DATE), '18:00:00'),
                             ifelse(str_detect(HOSPITAL, pattern = "MSM") & Weekday=='Wednesday',
                                    paste(as.character(SURGERY_DATE), '17:30:00'),
                                    ifelse(str_detect(HOSPITAL, pattern = "MSM") & Weekday!='Wednesday',
                                           paste(as.character(SURGERY_DATE), '17:30:00'), 
                                           ifelse((str_detect(HOSPITAL, pattern = "MSDC") | str_detect(HOSPITAL, pattern = "MSDUS") |  str_detect(HOSPITAL, pattern = "MSW")) & Weekday=='Wednesday',
                                                  paste(as.character(SURGERY_DATE), '17:30:00'),
                                                  ifelse((str_detect(HOSPITAL, pattern = "MSDC") | str_detect(HOSPITAL, pattern = "MSDUS") |  str_detect(HOSPITAL, pattern = "MSW")) & Weekday!='Wednesday',
                                                         paste(as.character(SURGERY_DATE), '17:30:00'), 
                                                         ifelse(str_detect(HOSPITAL, pattern = "MSQ"),
                                                                paste(as.character(SURGERY_DATE), '18:00:00'), 
                                                                ifelse(str_detect(HOSPITAL, pattern = "MSB"),
                                                                       paste(as.character(SURGERY_DATE), '15:00:00'),
                                                                       ifelse(str_detect(HOSPITAL, pattern = "MSH") & Weekday=='Wednesday',
                                                                              paste(as.character(SURGERY_DATE), '18:00:00'), NA)))))))),
    `Available Start` =  as.POSIXct(`Available Start`,format='%Y-%m-%d %H:%M:%S',tz = "America/New_York"),
    `Available End` = as.POSIXct(`Available End`,format='%Y-%m-%d %H:%M:%S',tz = "America/New_York"),
    
  )
  
  
  
  
}

schedule_data_needed_cols <- prime_time_location(schedule_data_needed_cols)

# Proprocess and create the necessary columns ----
schedule_data_needed_cols <- schedule_data_needed_cols %>%
  mutate(ProcedureTimeMinutes = as.numeric(PATIENT_OUT_ROOM_DTTM - PATIENT_IN_ROOM_DTTM)/60, # 45 minutes to accommodate for cleaning time,
         PATIENT_IN_ROOM_DTTM =  force_tz(PATIENT_IN_ROOM_DTTM,tzone = "America/New_York"),
         PATIENT_OUT_ROOM_DTTM = force_tz(PATIENT_OUT_ROOM_DTTM,tzone = "America/New_York"),
         PATIENT_OUT_ROOM_DTTM1 = PATIENT_OUT_ROOM_DTTM + minutes(as.integer(`Avg TAT`)),
         # ThreeHourBlockPart = as.numeric(ProcedureTimeMinutes)/180,
         # Date = as.Date(SURGERY_DATE),
         DayofMonth = day(SURGERY_DATE),
         WeekofYear = week(SURGERY_DATE),
         # TotalAvailableTimeHoursPrimeTime = difftime(`Available End` , `Available Start` ,units = "hours"),
         # TotalAvailableTimeMinutesPrimeTime = difftime(`Available End` , `Available Start` ,units = "mins"),
         PrimeTimeInterval = interval(`Available Start`, `Available End`),
         ProcedureInterval =  interval(PATIENT_IN_ROOM_DTTM,PATIENT_OUT_ROOM_DTTM,tzone = "America/New_York"),
         overlap_primetime_procedure = intersect(PrimeTimeInterval, ProcedureInterval),
         overlap_duration_minutes = as.numeric(int_length(overlap_primetime_procedure))/60,
         overlap_duration_minutes = replace_na(overlap_duration_minutes,0) + 38
  )


# Derive Relavent Metrics for Gap Analysis ----

or_location_map <- schedule_data_needed_cols %>%
  distinct(HOSPITAL,`OR Room`, OR_ID) %>%
  drop_na()

# Derive Capacity for ORs not used on days of year
schedule_data_needed_cols_no_use_days <- schedule_data_needed_cols %>%
  group_by(HOSPITAL,SURGERY_DATE,OR_ID) %>%
  summarise(NumberofCases = n()) %>%
  ungroup() %>%
  filter(!is.na(OR_ID)) %>%
  complete(nesting(HOSPITAL,OR_ID), SURGERY_DATE = seq(min(SURGERY_DATE), max(SURGERY_DATE), by = "day")) %>%
  filter(is.na(NumberofCases))

schedule_data_needed_cols_no_use_days <- prime_time_location(schedule_data_needed_cols_no_use_days)

schedule_data_needed_cols_no_use_days <- schedule_data_needed_cols_no_use_days %>%
  mutate( PrimeTimeInterval = interval(`Available Start`, `Available End`),
          RecoverableMinutesNoUse = as.numeric(int_length(PrimeTimeInterval))/60) %>%
  filter(!Weekday %in% c("Saturday","Sunday")) %>%
  select(HOSPITAL,SURGERY_DATE,OR_ID,RecoverableMinutesNoUse)

schedule_data_needed_cols_gaps <- schedule_data_needed_cols %>%
  filter(!is.na(overlap_primetime_procedure)) %>%
  group_by(OR_ID, SURGERY_DATE) %>%
  arrange(PATIENT_IN_ROOM_DTTM) %>%
  mutate(`Previous Patient Out` = lag(PATIENT_OUT_ROOM_DTTM1),
         `Earliest Case Start` = min(PATIENT_IN_ROOM_DTTM),
         `Latest Case End` = max(PATIENT_OUT_ROOM_DTTM1)) %>%
  filter(!is.na(`Previous Patient Out`)) %>%
  ungroup() %>%
  select(HOSPITAL,
         Cluster,
         OR_ID,
         OR_CASE_ID,
         SURGERY_DATE,
         PATIENT_IN_ROOM_DTTM,
         PATIENT_OUT_ROOM_DTTM1,
         `Available Start`,
         `Available End`,
         `Previous Patient Out`,
         `Earliest Case Start`, 
         `Latest Case End`) %>%
  mutate(GapTime = (PATIENT_IN_ROOM_DTTM - `Previous Patient Out`)/60,
         GapTime = ifelse(GapTime >= 0, GapTime,0),
         `GapTimeContinuousBlock (> 180 Min)` = ifelse(GapTime >= 180, GapTime,0),
         RecoverableDuringStart = (`Earliest Case Start` - `Available Start`)/60,
         RecoverableDuringStart = ifelse(RecoverableDuringStart >= 0, RecoverableDuringStart,0),
         RecoverableDuringEnd = (`Available End` - `Latest Case End`)/60,
         RecoverableDuringEnd = ifelse(RecoverableDuringEnd >= 0, RecoverableDuringEnd,0))



# Total Gap Minutes ----
gap_minutes <- schedule_data_needed_cols_gaps %>%
  group_by(HOSPITAL,OR_ID,SURGERY_DATE) %>%
  summarise(GapTimeTotal = sum(GapTime),
            `GapTimeContinuousBlock (> 180 Min)` = sum(`GapTimeContinuousBlock (> 180 Min)`),
            Cases = n()+1)


# Total Recoverable at Start Minutes ----
recoverable_start_minutes <- schedule_data_needed_cols_gaps %>%
  group_by(HOSPITAL,OR_ID,SURGERY_DATE) %>%
  distinct(RecoverableDuringStart) %>%
  summarise(RecoverableDuringStartTotal = sum(RecoverableDuringStart))



# Total Recoverable at Start Minutes ----
recoverable_end_minutes <- schedule_data_needed_cols_gaps %>%
  group_by(HOSPITAL,OR_ID,SURGERY_DATE) %>%
  distinct(RecoverableDuringEnd) %>%
  summarise(RecoverableDuringEndTotal = sum(RecoverableDuringEnd))


# Total Recoverable Minutes ----
# Combine all three above
recoverable_minutes <-   rbind(left_join(left_join(gap_minutes,recoverable_start_minutes),
                                             recoverable_end_minutes),
                                   schedule_data_needed_cols_no_use_days) %>%
  replace(is.na(.), 0) %>%
  mutate(RecoverableMinutesNoUse = ifelse(is.na(RecoverableMinutesNoUse),0, RecoverableMinutesNoUse),
         TotalRecoverableMinutes = GapTimeTotal+RecoverableDuringStartTotal+RecoverableDuringEndTotal+RecoverableMinutesNoUse,
         Weekday = weekdays(SURGERY_DATE),
         RecoverableBlocks = as.integer(TotalRecoverableMinutes/225))%>%
  select(HOSPITAL,
         OR_ID, 
         SURGERY_DATE, 
         Weekday,
         GapTimeTotal,
         Cases,
         `GapTimeContinuousBlock (> 180 Min)`,
         RecoverableDuringStartTotal,
         RecoverableDuringEndTotal,
         RecoverableMinutesNoUse,
         RecoverableBlocks)

write_xlsx(recoverable_minutes,"BlockAnalysis.xlsx")

recoverable_minutes_hospital <- recoverable_minutes %>%
  group_by(HOSPITAL,SURGERY_DATE) %>%
  summarise(TotalRecoverableMinutes = sum(TotalRecoverableMinutes)) %>%
  ungroup() %>%
  mutate(Weekday = weekdays(SURGERY_DATE),
         TotalRecoverableBlocks = as.integer(TotalRecoverableMinutes/225) ) %>% # 3 Hour 45 Minutes
  filter(!Weekday %in% c("Saturday", "Sunday"))  



# Prime Time Utilization --- 
EffectivePrimeTimeMinutes <- schedule_data_needed_cols %>%
  select(HOSPITAL,
         Weekday,
         `Available Start`,
         `Available End`) %>% 
  mutate(`Available Start` = format(`Available Start`, "%H:%M:%S"),
         `Available End` = format(`Available End`, "%H:%M:%S"),
         Weekday = factor(Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))) %>%
  distinct()

EffectivePrimeTimeMinutesKeepAllOpen <- left_join(EffectivePrimeTimeMinutes,
                                                  ORSchedulesKeepAllOpenPrimeTime %>%
                                                    select(Location,
                                                           `# ORs`,
                                                           `Time Start`,
                                                           `Time End`),
                                                  by =  c("HOSPITAL" = "Location")) %>%
  mutate(`Time Start` = as.POSIXct(format(as.POSIXct(`Time Start`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         `Time End` = as.POSIXct(format(as.POSIXct(`Time End`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         `Available Start` = as.POSIXct(`Available Start`,format = "%H:%M:%S"),
         `Available End` = as.POSIXct(`Available End`,format = "%H:%M:%S"),
         PrimeTime = interval(`Available Start`, `Available End`),
         Cascade = interval(`Time Start`, `Time End`),
         overlap_interval = intersect(PrimeTime, Cascade),
         overlap_duration_minutes = as.numeric(int_length(overlap_interval))/60,
         overlap_duration_minutes = ifelse(is.na(overlap_duration_minutes),0,overlap_duration_minutes),
         TotalAvailableTimeMinutesPrimeTimeKeepAllOpen = overlap_duration_minutes*`# ORs` ) %>%
  select(HOSPITAL,
         Weekday,
         TotalAvailableTimeMinutesPrimeTimeKeepAllOpen) %>%
  distinct() %>%
  group_by(HOSPITAL,
           Weekday) %>%
  summarise(TotalAvailableTimeMinutesPrimeTimeKeepAllOpen = sum(TotalAvailableTimeMinutesPrimeTimeKeepAllOpen))

EffectivePrimeTimeMinutes <- left_join(EffectivePrimeTimeMinutes,
                                       ORSchedules %>%
                                         select(Location,
                                                `# ORs`,
                                                `Time Start`,
                                                `Time End`),
                                       by =  c("HOSPITAL" = "Location")) %>%
  mutate(`Time Start` = as.POSIXct(format(as.POSIXct(`Time Start`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         `Time End` = as.POSIXct(format(as.POSIXct(`Time End`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         `Available Start` = as.POSIXct(`Available Start`,format = "%H:%M:%S"),
         `Available End` = as.POSIXct(`Available End`,format = "%H:%M:%S"),
         PrimeTime = interval(`Available Start`, `Available End`),
         Cascade = interval(`Time Start`, `Time End`),
         overlap_interval = intersect(PrimeTime, Cascade),
         overlap_duration_minutes = as.numeric(int_length(overlap_interval))/60,
         overlap_duration_minutes = ifelse(is.na(overlap_duration_minutes),0,overlap_duration_minutes),
         TotalAvailableTimeMinutesPrimeTimeCascadeOverlap = overlap_duration_minutes*`# ORs` )


EffectivePrimeTimeMinutesV1 <- EffectivePrimeTimeMinutes %>%
  select(HOSPITAL,
         Weekday,
         TotalAvailableTimeMinutesPrimeTimeCascadeOverlap) %>%
  distinct() %>%
  group_by(HOSPITAL,
           Weekday) %>%
  summarise(TotalAvailableTimeMinutesPrimeTimeCascadeOverlap = sum(TotalAvailableTimeMinutesPrimeTimeCascadeOverlap))


summary_used <- schedule_data_needed_cols %>%
  group_by(HOSPITAL,
           Weekday,
           DayofMonth,
           WeekofYear,
           SURGERY_DATE)%>%
  summarise(ProcessTimeMin = sum(overlap_duration_minutes))


summary_used_prime_time <-  left_join(summary_used,
                                      EffectivePrimeTimeMinutesV1) %>%
  mutate(UtilizationNormalizedPrimeTimeCascade = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesPrimeTimeCascadeOverlap))%>%
  drop_na() %>%
  mutate(Weekday = factor(Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))) %>%
  filter(!Weekday %in% c("Saturday", "Sunday"))

summary_used_prime_time <-  left_join(summary_used_prime_time,
                                      EffectivePrimeTimeMinutesKeepAllOpen) %>%
  mutate(UtilizationNormalizedPrimeTimeAllOpen = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesPrimeTimeKeepAllOpen)) %>%
  filter(!Weekday %in% c("Saturday", "Sunday")) 


summary_used_prime_time <- left_join(summary_used_prime_time,
                                     recoverable_minutes_hospital %>% select(HOSPITAL,
                                                                             SURGERY_DATE, 
                                                                             Weekday, 
                                                                             TotalRecoverableBlocks)) %>%
  mutate(ProcessTimeMinThreeHourBlocks = ProcessTimeMin + TotalRecoverableBlocks*225,
         UtilizationNormalizedPrimeTimeCascadeRecoverable = ProcessTimeMinThreeHourBlocks/as.numeric(TotalAvailableTimeMinutesPrimeTimeCascadeOverlap),
         UtilizationNormalizedPrimeTimeAllOpenRecoverable = ProcessTimeMinThreeHourBlocks/as.numeric(TotalAvailableTimeMinutesPrimeTimeKeepAllOpen))
  
summary_used_prime_time  <- summary_used_prime_time %>%
  select(HOSPITAL, Weekday, SURGERY_DATE, UtilizationNormalizedPrimeTimeCascade,UtilizationNormalizedPrimeTimeAllOpen, UtilizationNormalizedPrimeTimeAllOpenRecoverable)%>%
  pivot_longer( cols = c(`UtilizationNormalizedPrimeTimeCascade`, `UtilizationNormalizedPrimeTimeAllOpen`,UtilizationNormalizedPrimeTimeAllOpenRecoverable),
                names_to = "AllOpenvsCascadevsRecoverable",
                values_to = "Utilization") %>%
  mutate(AllOpenvsCascadevsRecoverable = ifelse(str_detect(AllOpenvsCascadevsRecoverable, pattern = "Recoverable"),"All Open Recoverable",ifelse(str_detect(AllOpenvsCascadevsRecoverable, pattern = "Cascade"),"Cascade", "All Rooms Open")))%>%
  mutate(Weekday = factor(Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))


get_plot <- function(SITE){
  plot_box <- ggplot(summary_used_prime_time %>% filter(HOSPITAL == SITE), aes(y = AllOpenvsCascadevsRecoverable, x = Utilization,fill = AllOpenvsCascadevsRecoverable)) +
    geom_boxplot(varwidth = TRUE,colour = "#221F72",outlier.alpha = 0.4,notch = TRUE) +
    scale_fill_manual(values = rep('#00AEFF', each = 3)) +
    labs(title =  SITE, x = "Prime Time Utilization (TAT Incl)",y = NULL) + 
    theme(legend.position = "none") +    
    # theme_bw() +
    theme(
      plot.background = element_blank(),
      panel.grid.major = element_blank(),
      panel.grid.minor = element_blank(),
      panel.border = element_blank(),
      strip.text.y = element_text(
        size = 5,
        color = "#221F72"
      )
    ) +
    # xlim(0, 0.6) +
    facet_grid(rows = vars(Weekday),scales = "free") +
    geom_vline(xintercept = 0.75, linetype = "dashed", color = "red")
}

MSB <- get_plot("MSB")
MSBI <- get_plot("MSBI")
MSH <- get_plot("MSH")
MSM <- get_plot("MSM")
MSQ <- get_plot("MSQ")
MSW <- get_plot("MSW")


ggsave("MSB_MSBI_with_TAT_recoverable.png",MSB | MSBI,width = 2705, height = 1709, units = "px")
# ggsave("MSBI_with_TAT.pdf",MSBI)
# ggsave("MSH_with_TAT.pdf",MSH)
# ggsave("MSM_with_TAT.pdf",MSM)
ggsave("MSH_MSM_with_TAT_recoverable.png",MSH|MSM,width = 2705, height = 1709, units = "px")
ggsave("MSQ_MSW_with_TAT_recoverable.png",MSQ | MSW,width = 2705, height = 1709, units = "px")

