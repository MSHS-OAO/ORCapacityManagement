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
              SELECT *
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


schedule_data <- dbGetQuery(conn, query)
dbDisconnect(conn)
