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


dsn <- "OAO Cloud DB Production"
conn <- dbConnect(odbc(), dsn)
source_table_name <- 'MS_INSIGHT.OR_QUALITY_DASHBOARD_CASE_DETAILS'
sched_date <- '2025-01-01'
status <- 'Completed'
query <- glue("SELECT * 
              FROM {source_table_name} 
              WHERE SURGERY_DATE >=TO_DATE('{sched_date}','YYYY-MM-DD') AND 
              CASE_STATUS = '{status}';") 
schedule_data <- dbGetQuery(conn, query)
dbDisconnect(conn)

# dbReadTable(conn, "MS_INSIGHT.OR_QUALITY_DASHBOARD_CASE_DETAILS")


if ("Presidents" %in% list.files("/SharedDrive/")) {
  user_directory <- paste0("/SharedDrive/deans/Presidents/HSPI-PM/",
                           "Operations Analytics and Optimization/Projects/",
                           "System Operations/OR Capacity Management/")
} else {
  user_directory <- paste0("/SharedDrive/deans/Presidents/HSPI-PM/",
                           "Operations Analytics and Optimization/Projects/",
                           "System Operations/OR Capacity Management/")
}

mshs_colors <- c("#221F72", "#00AEFF", "#D80B8C", "#7F7F7F")


ORSchedules <- read_xlsx(paste0(user_directory,"/SupplementData/ORSchedules.xlsx"),col_types = c("text","numeric","text", "text"))
ORSchedules <- ORSchedules %>%
  mutate(TimeStartPSXCT = as.POSIXct(format(as.POSIXct(`Time Start`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         TimeEndPSXCT = as.POSIXct(format(as.POSIXct(`Time End`,format = "%I:%M:%S %p"),format="%H:%M:%S"),format="%H:%M:%S"),
         TimeMinutes = as.numeric(difftime(TimeEndPSXCT, TimeStartPSXCT, units = "mins")),
         TotalAvailableTimeMinutes =TimeMinutes*`# ORs` ) %>%
  arrange(TimeStartPSXCT)

ORAvailability <- ORSchedules %>%
  group_by(Location) %>%
  summarise(TotalAvailableTimeMinutesAgg = sum(TotalAvailableTimeMinutes))

# data_msh <- data %>%
#   filter(Hospital == 'OR MSH')
# 
# data_msh_complete <- data_msh %>%
#   filter(!is.na(LOG_ID),
#          LOG_ID == 1445485)


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


schedule_data_needed_cols <- schedule_data_needed_cols %>%
  mutate(ProcedureTimeMinutes = as.numeric(PATIENT_OUT_ROOM_DTTM - PATIENT_IN_ROOM_DTTM)/60,
         # ThreeHourBlockPart = as.numeric(ProcedureTimeMinutes)/180,
         # Date = as.Date(SURGERY_DATE),
         `Available Start` = as.character(SURGERY_DATE),
         `Available End` = as.character(SURGERY_DATE),
         Weekday = weekdays(SURGERY_DATE),
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
         TotalAvailableTimeHoursPrimeTime = difftime(`Available End` , `Available Start` ,units = "hours"),
         TotalAvailableTimeMinutesPrimeTime = difftime(`Available End` , `Available Start` ,units = "mins"),
         PrimeTimeCase = ifelse(PATIENT_IN_ROOM_DTTM>=`Available Start` & PATIENT_OUT_ROOM_DTTM<=`Available End`,TRUE,FALSE)
         
  )

schedule_data_needed_cols <- left_join(schedule_data_needed_cols,
                                       ORAvailability,
                                       by = c("HOSPITAL" = "Location"))


# schedule_data_needed_cols <- schedule_data_needed_cols %>%
#   mutate(ORScheduleTimeStart =   as.POSIXct(paste(as.character(SURGERY_DATE), `Time Start`),format = '%Y-%m-%d %I:%M:%S %p'),
#          ORScheduleTimeEnd =  as.POSIXct(paste(as.character(SURGERY_DATE), `Time End`),format = '%Y-%m-%d %I:%M:%S %p')) 
  

TotalAvailableTime <- schedule_data_needed_cols %>%
  select(HOSPITAL,
         Weekday,
         SURGERY_DATE,
         TotalAvailableTimeMinutesPrimeTime,
         TotalAvailableTimeHoursPrimeTime,
         `Available Start`,
         `Available End`,
         TotalAvailableTimeMinutesAgg) %>% distinct()


summary_used <- schedule_data_needed_cols %>%
  group_by(HOSPITAL,
           Weekday,
           SURGERY_DATE)%>%
  summarise(ProcessTimeMin = sum(ProcedureTimeMinutes),
            EarliestProcedureStart = min(PATIENT_IN_ROOM_DTTM),
            EarliestProcedureEnd = min(PATIENT_OUT_ROOM_DTTM),
            LatestProcedureStart = max(PATIENT_IN_ROOM_DTTM),
            LatestProcedureEnd = max(PATIENT_OUT_ROOM_DTTM))

summary_used_prime_non_prime <- schedule_data_needed_cols %>%
  group_by(HOSPITAL,
           Weekday,
           SURGERY_DATE,
           PrimeTimeCase)%>%
  summarise(ProcessTimeMin = sum(ProcedureTimeMinutes),
            EarliestProcedureStart = min(PATIENT_IN_ROOM_DTTM),
            EarliestProcedureEnd = min(PATIENT_OUT_ROOM_DTTM),
            LatestProcedureStart = max(PATIENT_IN_ROOM_DTTM),
            LatestProcedureEnd = max(PATIENT_OUT_ROOM_DTTM))


 
summary_used <-  left_join(summary_used,
                           TotalAvailableTime) %>%
  mutate(UtilizationNormalized = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesAgg))%>%
  drop_na()
summary_used$Weekday <- factor(summary_used$Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))

summary_used_prime_non_prime <-  left_join(summary_used_prime_non_prime,
                           TotalAvailableTime) %>%
  mutate(UtilizationNormalized = ProcessTimeMin/as.numeric(TotalAvailableTimeMinutesAgg))%>%
  drop_na()
summary_used_prime_non_prime$Weekday <- factor(summary_used_prime_non_prime$Weekday, levels = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))



write_xlsx(summary_used,"SummaryData.xlsx")
write_xlsx(summary_used_prime_non_prime,"SummaryDataPrimeNonPrime.xlsx")



ggplot(summary_used, aes(x = Weekday, y = UtilizationNormalized,fill = Weekday)) +
  geom_boxplot() +
  scale_fill_manual(values = rep('#221F72', each = 7)) +
  labs(title = "Utilization by Days", y = "Utilization", x = "Weekday") + 
  theme(legend.position = "none") +    
  # theme_bw() +
  facet_grid(rows = vars(HOSPITAL))

