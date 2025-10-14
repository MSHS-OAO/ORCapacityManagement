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
 


# Establish DB Connection and Get data ----
dsn <- "OAO Cloud DB Production"
conn <- dbConnect(odbc(), dsn)
source_table_name <- 'MS_INSIGHT.OR_QUALITY_DASHBOARD_CASE_DETAILS'
sched_date <- '2024-09-01'
status <- 'Completed'
query <- glue("SELECT * 
              FROM {source_table_name} 
              WHERE SURGERY_DATE >=TO_DATE('{sched_date}','YYYY-MM-DD') AND 
              CASE_STATUS = '{status}';") 
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

ORAvailability <- ORSchedules %>%
  group_by(Location) %>%
  summarise(TotalAvailableTimeMinutesAgg = sum(TotalAvailableTimeMinutes))

ORAvailabilityKeepAllOpenPrimeTime <- ORSchedulesKeepAllOpenPrimeTime %>%
  group_by(Location) %>%
  summarise(TotalAvailableTimeMinutesAggKeepAllOpenPrimeTime = sum(TotalAvailableTimeMinutes))


# data_msh <- data %>%
#   filter(Hospital == 'OR MSH')
# 
# data_msh_complete <- data_msh %>%
#   filter(!is.na(LOG_ID),
#          LOG_ID == 1445485)


# Subset Necessary Columns ----
schedule_data_needed_cols <- schedule_data %>%
  select(FACILITY,
         HOSPITAL,
         CLUSTER_NAME,
         PAYOR_GROUP_IP,
         FACILITY_MSX_OP,
         PAYOR_GROUP_OP,
         PAT_MRN_ID,
         PRIMARY_SURGEON,
         SURGEON_SPECIALTY,
         PRIMARY_PROC_CODE,
         PRIMARY_PROC_CODE,
         OR_CASE_ID,
         OR_ID, # Essentially room number, will be present unless it's L(abour)&D(elivery) or Interventional Radiology OR_ID -> CLuster Name -> Facility -> Hospital
         ANESTHESIA_TYPE,
         SERVICE_NAME,
         SCHED_IN_ROOM_DTTM,
         SCHED_START_TIME,
         PATIENT_IN_ROOM_DTTM,
         PATIENT_OUT_ROOM_DTTM,
         MINUTES_IN_ROOM_TO_OUT_ROOM,
         CASE_STATUS,
         SURGERY_DATE,
         RACE_ETHNIC_GROUP,
         SEX,
         AGE_GROUP,
         LANGUAGE_GROUP,
         PAYOR_GROUP,
         PAYOR,
         SURGEON_NPI,
         PRIMARY_PHYSICIAN_IS,
         TIME_SCHEDULED,
         HOSP_ADMSN_TIME,
         HOSP_DISCH_TIME) 

# Proprocess and create the necessary columns ----
schedule_data_needed_cols <- schedule_data_needed_cols %>%
  mutate(ProcedureTimeMinutes = 45+as.numeric(PATIENT_OUT_ROOM_DTTM - PATIENT_IN_ROOM_DTTM)/60, # 45 minutes to accommodate for cleaning time
         # ThreeHourBlockPart = as.numeric(ProcedureTimeMinutes)/180,
         # Date = as.Date(SURGERY_DATE),
         `Available Start` = as.character(SURGERY_DATE),
         `Available End` = as.character(SURGERY_DATE),
         Weekday = weekdays(SURGERY_DATE),
         DayofMonth = day(SURGERY_DATE),
         WeekofYear = week(SURGERY_DATE),
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
         # TotalAvailableTimeHoursPrimeTime = difftime(`Available End` , `Available Start` ,units = "hours"),
         # TotalAvailableTimeMinutesPrimeTime = difftime(`Available End` , `Available Start` ,units = "mins"),
         PrimeTimeCase = ifelse(PATIENT_IN_ROOM_DTTM>=`Available Start` & PATIENT_OUT_ROOM_DTTM<=`Available End`,TRUE,FALSE)
         
  )

schedule_data_needed_cols <- left_join(schedule_data_needed_cols,
                                       ORAvailability,
                                       by = c("HOSPITAL" = "Location"))
schedule_data_needed_cols <- left_join(schedule_data_needed_cols,
                                       ORAvailabilityKeepAllOpenPrimeTime,
                                       by = c("HOSPITAL" = "Location"))

# schedule_data_needed_cols <- schedule_data_needed_cols %>%
#   mutate(ORScheduleTimeStart =   as.POSIXct(paste(as.character(SURGERY_DATE), `Time Start`),format = '%Y-%m-%d %I:%M:%S %p'),
#          ORScheduleTimeEnd =  as.POSIXct(paste(as.character(SURGERY_DATE), `Time End`),format = '%Y-%m-%d %I:%M:%S %p')) 
  

TotalAvailableTime <- schedule_data_needed_cols %>%
  select(HOSPITAL,
         Weekday,
         SURGERY_DATE,
         # TotalAvailableTimeMinutesPrimeTime,
         # TotalAvailableTimeHoursPrimeTime,
         `Available Start`,
         `Available End`,
         TotalAvailableTimeMinutesAgg,
         TotalAvailableTimeMinutesAggKeepAllOpenPrimeTime) %>% distinct()




EffectivePrimeTimeMinutes <- schedule_data_needed_cols %>%
  select(HOSPITAL,
         Weekday,
         `Available Start`,
         `Available End`) %>% 
  mutate(`Available Start` = format(`Available Start`, "%H:%M:%S"),
         `Available End` = format(`Available End`, "%H:%M:%S"),
         Weekday = factor(Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))) %>%
  distinct()


EffectivePrimeTimeMinutesKeepAllOpenPrimeTime <- left_join(EffectivePrimeTimeMinutes,
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
         TotalAvailableTimeMinutesKeepAllOpenPrimeTime = overlap_duration_minutes*`# ORs` )

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

EffectivePrimeTimeMinutesKeepAllOpenPrimeTimeV1 <- EffectivePrimeTimeMinutesKeepAllOpenPrimeTime %>%
  select(HOSPITAL,
         Weekday,
         TotalAvailableTimeMinutesKeepAllOpenPrimeTime) %>%
  distinct() %>%
  group_by(HOSPITAL,
           Weekday) %>%
  summarise(TotalAvailableTimeMinutesKeepAllOpenPrimeTime = sum(TotalAvailableTimeMinutesKeepAllOpenPrimeTime))

summary_used <- schedule_data_needed_cols %>%
  group_by(HOSPITAL,
           Weekday,
           DayofMonth,
           WeekofYear,
           SURGERY_DATE)%>%
  summarise(ProcessTimeMin = sum(ProcedureTimeMinutes))




summary_used_prime_time <- schedule_data_needed_cols %>%
  filter(PrimeTimeCase == TRUE) 
summary_used_prime_time <- summary_used_prime_time %>%
  group_by(HOSPITAL,
           Weekday,
           DayofMonth,
           WeekofYear,
           SURGERY_DATE)%>%
  summarise(ProcessTimeMin = sum(ProcedureTimeMinutes))


 
summary_used <-  left_join(summary_used,
                           TotalAvailableTime) %>%
  mutate(UtilizationNormalized = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesAgg),
         UtilizationNormalizedKeepAllOpenPrimeTime = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesAggKeepAllOpenPrimeTime))%>%
  drop_na() %>%
  mutate(Weekday = factor(Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))
  

# summary_used <-  left_join(summary_used_weekday,
#             EffectivePrimeTimeMinutesV1) %>%
#   mutate(UtilizationNormalizedPrimeTimeCascade = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesPrimeTimeCascadeOverlap))%>%
#   drop_na() 



summary_used_prime_time <-  left_join(summary_used_prime_time,
                                           EffectivePrimeTimeMinutesV1) %>%
  mutate(UtilizationNormalizedPrimeTimeCascade = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesPrimeTimeCascadeOverlap))%>%
  drop_na() %>%
  mutate(Weekday = factor(Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")))

summary_used_prime_time <-  left_join(summary_used_prime_time,
                                      EffectivePrimeTimeMinutesKeepAllOpenPrimeTimeV1) %>%
  mutate(UtilizationNormalizedKeepAllOpenPrimeTime = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesKeepAllOpenPrimeTime))
  
plot_data <- summary_used_prime_time %>%
  select(HOSPITAL,Weekday,SURGERY_DATE,UtilizationNormalizedPrimeTimeCascade,UtilizationNormalizedKeepAllOpenPrimeTime)

plot_data <- pivot_longer(
  plot_data,
  cols = c(`UtilizationNormalizedPrimeTimeCascade`, `UtilizationNormalizedKeepAllOpenPrimeTime`),
  names_to = "AllOpenvsCascade",
  values_to = "Utilization"
)

plot_data <- plot_data %>%
  mutate(AllOpenvsCascade = ifelse(str_detect(AllOpenvsCascade, pattern = "Open"),"No Cascade","Cascade"))

# write_xlsx(summary_used,"SummaryData.xlsx")
# write_xlsx(summary_used_prime_non_prime,"SummaryDataPrimeNonPrime.xlsx")
# write_xlsx(EffectivePrimeTimeMinutes,"EffectivePrimeTimeMinutes.xlsx")



ggplot(summary_used, aes(y = HOSPITAL, x = UtilizationNormalized,fill = HOSPITAL)) +
  geom_boxplot(varwidth = TRUE,colour = "#221F72",outlier.alpha = 0.4,notch = TRUE) +
  scale_fill_manual(values = rep('#00AEFF', each = 6)) +
  labs(title = "Utilization by Days - 24 Hours", x = "Utilization") + 
  theme(legend.position = "none") +    
  # theme_bw() +
  theme(
    plot.background = element_blank(),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    panel.border = element_blank()
  ) +
  xlim(0, 0.8) +
  facet_grid(rows = vars(Weekday)) 





# ggplot(summary_used, aes(x = Weekday, y = UtilizationNormalizedPrimeTimeCascade,fill = Weekday)) +
#   geom_boxplot() +
#   scale_fill_manual(values = rep('#221F72', each = 7)) +
#   labs(title = "Utilization by Days - PrimeTime and Cascades", y = "Utilization", x = "Weekday") + 
#   theme(legend.position = "none") +    
#   # theme_bw() +
#   facet_grid(rows = vars(HOSPITAL))

p1 <- ggplot(summary_used_prime_time, aes(y = HOSPITAL, x = UtilizationNormalizedPrimeTimeCascade,fill = HOSPITAL)) +
  geom_boxplot(varwidth = TRUE,colour = "#221F72",outlier.alpha = 0.4,notch = TRUE) +
  scale_fill_manual(values = rep('#00AEFF', each = 6)) +
  labs(title = "Utilization during Prime Time with Cascades", x = "Utilization") + 
  theme(legend.position = "none") +    
  # theme_bw() +
  theme(
    plot.background = element_blank(),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    panel.border = element_blank()
  ) +
  xlim(0, 0.7) +
  facet_grid(rows = vars(Weekday))

p2 <- ggplot(summary_used_prime_time, aes(y = HOSPITAL, x = UtilizationNormalizedKeepAllOpenPrimeTime,fill = HOSPITAL)) +
  geom_boxplot(varwidth = TRUE,colour = "#221F72",outlier.alpha = 0.4,notch = TRUE) +
  scale_fill_manual(values = rep('#00AEFF', each = 6)) +
  labs(title = "Utilization during Prime Time without Cascades", x = "Utilization") + 
  theme(legend.position = "none") +    
  # theme_bw() +
  theme(
    plot.background = element_blank(),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    panel.border = element_blank()
  ) +
  xlim(0, 0.7) +
  facet_grid(rows = vars(Weekday)) 


p1 + p2

get_plot <- function(SITE){
  plot_box <- ggplot(plot_data %>% filter(HOSPITAL == SITE), aes(y = AllOpenvsCascade, x = Utilization,fill = AllOpenvsCascade)) +
    geom_boxplot(varwidth = TRUE,colour = "#221F72",outlier.alpha = 0.4,notch = TRUE) +
    scale_fill_manual(values = rep('#00AEFF', each = 2)) +
    labs(title =  SITE, x = "Prime Time Utilization (TAT Incl)") + 
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
    xlim(0, 0.6) +
    facet_grid(rows = vars(Weekday),scales = "free") +
    geom_vline(xintercept = 0.4, linetype = "dashed", color = "red")
}

MSB <- get_plot("MSB")
MSBI <- get_plot("MSBI")
MSH <- get_plot("MSH")
MSM <- get_plot("MSM")
MSQ <- get_plot("MSQ")
MSW <- get_plot("MSW")

 
ggsave("MSB_MSBI_MSH_with_TAT.pdf",(MSB | MSBI | MSH))

ggsave("MSM_MSQ_MSW_with_TAT.pdf",(MSM | MSQ | MSW))
