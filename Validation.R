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
library(grid)
# library(ggnewscale)
library(ggtext)
library(tidyverse)
library(shadowtext)
library(bizdays)

# library(magick)

# library(ggh4x) # for x axis




recastTimezone.POSIXct <- function(x, tz) return(
  as.POSIXct(as.character(x), origin = as.POSIXct("1970-01-01"), tz = tz))


# Establish DB Connection and Get data ----
dsn <- "OAO Cloud DB Production"
conn <- dbConnect(odbc(), dsn)
encounter_data_table_name <- 'MS_INSIGHT.OR_QUALITY_DASHBOARD_CASE_DETAILS'
room_master_data_table_name <- 'MS_INSIGHT.OR_QUALITY_DASHBOARD_ROOM_MASTER'
utlization_calculation_table <- 'MS_INSIGHT.OR_QUALITY_ROOM_UTIL_V'


# current_date <- Sys.Date()
# sched_date <- '2024-09-01'
sched_start_date <- '2026-02-01'
sched_end_date <- '2026-02-28'
status <- 'Completed'
room_exclusion_list <- "('MSW OR 23','MSM OR 08','MSM OR 15')"


query <- glue("
              SELECT ENCOUNTERS.OR_CASE_ID,
                     ENCOUNTERS.ENCOUNTER_NO_SRC AS ENCOUNTER_NO_SRC,
                     ENCOUNTERS.ENCOUNTER_ID AS ENCOUNTER_ID,
                     ENCOUNTERS.PAYOR_GROUP_DESC_MSX_OP,
                     ENCOUNTERS.CCM_PAYOR_DESC_MSX_OP,
                     ENCOUNTERS.PAYOR_GROUP_OP,
                     ENCOUNTERS.CLINIC_GROUP_DESC_MSX,
                     ENCOUNTERS.REG_AREA_DESC_SRC,
                     -- Provider Details
                     ENCOUNTERS.ATTENDING_MD_NAME_MSX AS ATTENDING_MD,
                     ENCOUNTERS.ATTENDING_MD_SPEC_SRC AS ATTENDING_MD_SPECIALIZATION,
                     ENCOUNTERS.PRIMARY_SURGEON AS PRIMARY_SURGEON,
                     ENCOUNTERS.SURGEON_SPECIALTY AS PRIMARY_SURGEON_SPECIALTY,
                     -- Patient Details
                     ENCOUNTERS.PAT_CLASS_NAME AS PATIENT_CLASS,
                     ENCOUNTERS.PAT_MRN_ID AS PATIENT_MRN,
                     TO_CHAR(ENCOUNTERS.PAT_DOB,'YYYY-MM-DD') AS PATIENT_DOB,
                     ENCOUNTERS.ADMIT_CSN_ID,
                     -- Procedure Details
                     ENCOUNTERS.TOTAL_TIME_NEEDED,
                     ENCOUNTERS.PRIMARY_PROC_CODE AS PRIMARY_PROCEDURE_CODE,
                     ENCOUNTERS.PRIMARY_PROCEDURE AS PRIMARY_PROCEDURE_DESC,
                     ENCOUNTERS.ANESTHESIA_TYPE AS ANESTHESIA_TYPE,
                     -- Time Data/dates
                     ENCOUNTERS.SCHED_IN_ROOM_DTTM,
                     ENCOUNTERS.SCHED_START_TIME,
                     ENCOUNTERS.PATIENT_IN_ROOM_DTTM,
                     ENCOUNTERS.PATIENT_OUT_ROOM_DTTM,
                     ENCOUNTERS.MINUTES_IN_ROOM_TO_OUT_ROOM,
                     ENCOUNTERS.TURNOVER_FROM_PRIOR_CASE,
                     ENCOUNTERS.SURGERY_DATE,
                     -- Room/Location Details
                     ROOM_MASTER.ROOM_ID AS ROOM_ID,
                     ROOM_MASTER.LOCATION_NM AS LOCATION_NAME,
                     ROOM_MASTER.CLUSTER_NAME AS CLUSTER_NAME,
                     ROOM_MASTER.ROOM_NM AS ROOM_NAME,
                     ENCOUNTERS.ROBOTIC_SURGERY_DAVINCI_YN
              FROM
              {encounter_data_table_name} ENCOUNTERS
              LEFT JOIN
              {room_master_data_table_name} ROOM_MASTER ON ENCOUNTERS.OR_ID = ROOM_MASTER.ROOM_ID
              LEFT JOIN
              {utlization_calculation_table} TEMPLATE_TIME ON ENCOUNTERS.OR_CASE_ID =  TEMPLATE_TIME.LOG_ID
              WHERE
              ENCOUNTERS.SURGERY_DATE >= TO_DATE('{sched_start_date}','YYYY-MM-DD') AND
              ENCOUNTERS.SURGERY_DATE <= TO_DATE('{sched_end_date}','YYYY-MM-DD') AND
              -- Had to use TRIM since oracle adds trailing spaces to days with less characters compared to WEDNESDAY
              -- TRIM(TO_CHAR(ENCOUNTERS.SURGERY_DATE, 'DAY')) NOT IN ('SATURDAY','SUNDAY') AND
              TEMPLATE_TIME.WEEKEND_YN = 'N' AND
              -- Exclusions based on Tableau dashboard
              -- ENCOUNTERS.SURGERY_DATE NOT IN (SELECT DATES FROM HOLIDAYS) AND
              TEMPLATE_TIME.HOLIDAY_YN = 'N' AND
              ENCOUNTERS.CASE_STATUS = '{status}' AND
              -- Exclusions based on Tableau dashboard
              ROOM_MASTER.ROOM_NM NOT IN {room_exclusion_list};
              ")


room_schedules <- glue("
                       SELECT LOG_ID AS OR_CASE_ID,
                              ROOM_ID
                              SNAPSHOT_DATE,
                              ABSOLUTE_SLOT_START,
                              ABSOLUTE_SLOT_END
                       FROM {utlization_calculation_table} TEMPLATE_TIME
                       WHERE
                       TEMPLATE_TIME.SNAPSHOT_DATE >= TO_DATE('{sched_start_date}','YYYY-MM-DD') AND
                       TEMPLATE_TIME.SNAPSHOT_DATE <= TO_DATE('{sched_end_date}','YYYY-MM-DD') AND
                       TEMPLATE_TIME.WEEKEND_YN = 'N' AND
                       TEMPLATE_TIME.HOLIDAY_YN = 'N' AND
                       SLOT_TYPE = 'BLOCK';
                       ")
              


schedule_data <- dbGetQuery(conn, query)
room_schedules_data <- dbGetQuery(conn,room_schedules)
dbDisconnect(conn)
