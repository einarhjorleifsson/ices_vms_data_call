# ------------------------------------------------------------------------------
# run this as:
#  nohup R < R/stk.R --vanilla > logs/stk_2023-05-29.log &

library(tidyverse)
library(lubridate)
library(mar)
con <- connect_mar()
YEARS <- 2022:2009


LB <- read_rds("data/logbooks.rds")

ais_files <- dir("~/stasi/gis/AIS_TRAIL/trails", full.names = TRUE)


for(y in 1:length(YEARS)) {
  
  YEAR <- YEARS[y]
  print(YEAR)
  
  LB.year <-
    LB %>%
    filter(year %in% YEAR) %>% 
    select(vid, visir, gid, t1, t2) %>%
    pivot_longer(cols = c(t1, t2),
                 names_to = "startend",
                 values_to = "time") %>%
    arrange(vid, time) %>%
    mutate(year = year(time),
           time = round_date(time, "minutes"))
  
  f <- ais_files |> str_detect(paste0("_y", YEAR))

  VMS <-
    ais_files[f] |> 
    map(read_rds) |> 
    bind_rows() |> 
    filter(.cid > 0, 
           v %in% c("end_location", "not")) |> 
    select(vid, time, lon, lat, speed) %>%
    filter(between(lon, -44, 68.50),
           between(lat,  36, 85.50)) %>%
    distinct() %>%
    mutate(vms = TRUE) %>%
    arrange(vid, time) %>%
    select(vid, time, lon, lat, speed, vms) %>% 
    mutate(time = round_date(time, "minutes"))
  
  # loop though each vessel, in some cases it has more than on mid
  VIDs <- 
    VMS |> 
    select(vid) |> 
    distinct() |> 
    # only vessels with logbook records within the year
    inner_join(LB.year |> select(vid) |> distinct()) |> 
    pull(vid) %>%
    unique()
  
  res <- list()
  
  for(v in 1:length(VIDs)) {
    print(paste(YEAR, v, VIDs[v]))
    
    VMS.vessel <-
      VMS %>%
      filter(vid %in% VIDs[v]) %>%
      # HERE: get rid of wacky points via derived speed
      arrange(time) %>%
      mutate(time = round_date(time, "minutes"))
    
    LB.year.vid <-
      LB.year %>%
      filter(vid == VIDs[v])
    
    T1 <- max(min(LB.year.vid$time) - days(2), ymd_hms(paste0(YEAR, "-01-01 00:00:00")))
    T2 <- min(max(LB.year.vid$time) + days(2), ymd_hms(paste0(YEAR, "-12-31 23:59:00")))
    
    res[[v]] <-
      # dataframe of minutes over the whole year
      tibble(time = seq(T1,
                        T2,
                        by = "1 min")) %>%
      left_join(VMS.vessel, by = "time") %>%
      mutate(y = 1:n(),
             vid = VIDs[v])
    
    if(sum(!is.na(res[[v]]$lon)) > 1) {
      
      res[[v]] <-
        res[[v]] %>%
        mutate(lon = approx(y, lon, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               lat = approx(y, lat, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               speed = approx(y, speed, y, method = "linear", rule = 1, f = 0, ties = mean)$y) %>%
        select(-y) %>%
        bind_rows(LB.year.vid) %>%
        arrange(time) %>%
        mutate(x = if_else(startend == "t1", 1, 0, 0)) %>%
        mutate(x = case_when(startend == "t1" ~ 1,
                             startend == "t2" ~ -1,
                             TRUE ~ 0)) %>%
        mutate(x = cumsum(x)) %>%
        fill(visir) %>%
        mutate(visir = ifelse(x == 1 | startend == "t2", visir, NA_integer_)) %>%
        mutate(y = 1:n()) %>%
        mutate(lon = approx(y, lon, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               lat = approx(y, lat, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
               speed = approx(y, speed, y, method = "linear", rule = 1, f = 0, ties = mean)$y) %>%
        select(vid, gid, visir, time, lon, lat, speed, vms) %>% 
        # drop values that have no visir to save space
        filter(!is.na(visir)) %>% 
        fill(gid)
    }
    
  }
  bind_rows(res) %>%
    write_rds(paste0("data/is_vms_visir_y", YEAR, ".rds"))
}

devtools::session_info()
