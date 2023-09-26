# How to run things ------------------------------------------------------------
# run this as:
#  nohup R < R/annexes.R --vanilla > logs/anexes_2023-08-14.log &

lubridate::now()


library(sf)
library(lubridate)
library(tidyverse)
library(icesVocab)
source("R/functions.R")
EXPORT <- TRUE
TODAY <- today() %>% as.character()

if(FALSE) {
  an2 <-
    # 2023-09-15
    # this was part of the code delivery submitted 2023-08-14
    #  comments made that the max interval was very high
    #  it is an artefact of the algorithm upstream, only associated
    #  with records were only 1 vessel was recorded in a annex2 record
    #  this issue needs to be checked for the 2024 datacall
    #  here a cap was put on the interval to 60 minutes
    read_ve <- function(file) {
      
      read.csv(file,
               header = FALSE,
               na.strings = "NULL") %>% 
        tidyr::as_tibble() %>% 
        dplyr::rename(type = 1,
                      country = 2,
                      year = 3,
                      month = 4,
                      n_vessel = 5,
                      vids = 6,
                      csq = 7,
                      dcf4 = 8,
                      dcf5 = 9,
                      dcf6 = 10,
                      lclass = 11,
                      speed = 12,
                      effort = 13,             # units in hours
                      int = 14,                # average interval
                      length = 15,             # average vessel length
                      kw = 16,                 # average kw
                      kwh = 17,                # kw x effort
                      catch = 18,
                      value = 19,
                      gearwidth = 20) %>%
        #                  spread = 21) %>% 
        dplyr::mutate(value = as.numeric(value))
    }
  an1 <- 
    read_ve("delivery/iceland_annex1_2009_2022_2023-08-14.csv") |> 
    mutate(int = ifelse(int > 120, 120, int))
  knitr::kable(do.call(rbind, tapply(an1$int, an1$year, summary)), booktabs = TRUE,
          caption = "Summary statistics of average ping interval")
  ggplot(an1, aes(x = int)) +
    geom_histogram() +
    #scale_x_log10() +
    labs(x = "Average ping interval", y = "Count") +
    facet_wrap( ~ year, ncol = 2)
  an1 %>%
    write_csv("iceland_annex1_2009_2022_2023-09-15.csv",
              na = "",
              col_names = FALSE)
  
}

if(FALSE) {
  an2 <- 
    # this was part of the code delivery submitted 2022-06-09
    #  problem is that read_le generates lon and lat from csquare
    #  read_le("delivery/iceland_annex2_2009_2021_2022-04-30.csv")
    read.csv("delivery/iceland_annex2_2009_2021_2022-04-30.csv",
             header = FALSE,
             na.strings = "NULL")
  an2 %>%
    # the catch column
    mutate(V15 = V15 * 1e3) %>% 
    write_csv(paste0("delivery/iceland_annex2_2009_2021_", TODAY, ".csv"),
              na = "",
              col_names = FALSE)
  an1 <- 
    read_ve("delivery/iceland_annex1_2009_2021_2022-04-30.csv")
  an1 %>%
    mutate(catch = catch * 1e3) %>% 
    write_csv(paste0("delivery/iceland_annex1_2009_2021_", TODAY, ".csv"),
              na = "",
              col_names = FALSE)
  
}


sq <- read_sf("ftp://ftp.hafro.is/pub/data/shapes/ices_rectangles.gpkg")

YEARS <- 2022:2009

# Get the values accepted in this vocabulary dataset
vlen_ices <- getCodeList("VesselLengthClass") ### Get DATSU Vocabulary list for selected dataset
# Filter values that aren't deprecated, overlapped  or not accepted by data call requirements
vlen_icesc <-
  vlen_ices %>%
  # 2023-08-14: changed in accordance with this: https://github.com/ices-eg/ICES-VMS-and-Logbook-Data-Call/blob/main/3_data_submission.R
  filter ( Key %in% c("VL0006", "VL0608", "VL0810", "VL1012", "VL1215" ,"VL1518", "VL1824" ,"VL2440" ,"VL40XX"))%>%
  select(Key)%>%
  arrange(Key)


LGSc <- 
  read_rds("data/logbooks.rds") %>% 
  mutate(month = month(date)) |> 
  # fix upstream, or better still delete upstream
  mutate(month = ifelse(is.na(month), month(date), month)) %>%
  # 2022-04-30 Length class code changed, again!
  mutate(length_class = cut(length,
                            breaks=c(0, 6, 8, 10, 12, 15, 18, 24, 40, 'inf' ), 
                            right = FALSE    ,
                            include.lowest = TRUE,
                            labels =  vlen_icesc$Key))


# Annex 2 - logbooks -----------------------------------------------------------

twovessels <-
  LGSc %>%
  select(year, month, ices, m4, m5, m6, length_class, vid0) %>%
  distinct() %>%
  group_by(year, month, ices, m4, m6, length_class) %>%
  mutate(n_vessel = n_distinct(vid0)) %>%
  ungroup() %>%
  filter(n_vessel %in% 1:2) %>%
  group_by(year, month, ices, m4, m6, length_class) %>%
  mutate(vids = case_when(n_vessel == 1 ~ vid0,
                          n_vessel == 2 ~ paste0(vid0, ";", lead(vid0)))) %>%
  slice(1) %>%
  ungroup() %>% 
  select(-vid0)

annex2 <-
  LGSc %>%
  # First for each vessel
  # 2021-05-11: There was a problem here with the derivation of FishingDays
  #             because a vessel can be in more than one ICES square within
  #             a day. This leads to overestimation of effort.
  #             The solution is to first split a fishing day into each 
  #             activity as a fraction.
  group_by(vid, date) %>% 
  mutate(pdays = 1 / n()) %>% 
  ungroup() %>% 
  group_by(year, month, ices, m4, m5, m6, length_class, vid) %>%
  # 2021-05-11: Here sum fraction of fishing days, not n_distinct(date)
  summarise(FishingDays = sum(pdays),
            kwdays = sum(FishingDays * kw),
            catch = sum(total),
            .groups = "drop") %>%
  # then just summarise over vessels
  group_by(year, month, ices, m4, m5, m6, length_class) %>%
  summarise(n_vessel = n_distinct(vid),
            FishingDays = sum(FishingDays),
            kwdays = sum(kwdays),
            catch = sum(catch),
            .groups = "drop") %>%
  ungroup() %>% 
  left_join(twovessels) %>% 
  mutate(type = "LE",
         country = "IS",
         vms = "Y",
         value = NA_real_,
         lowermeshsize = NA,
         uppermeshsize = NA) %>% 
  # CHECK with datacall where lower/uppermeshsize is in the order of things
  select(type, country, year, month, n_vessel, vids, ices, m4, m5, 
         lowermeshsize, uppermeshsize,
         m6, length_class,
         vms, FishingDays, kwdays, catch, value)

annex2 <- 
  annex2 %>% 
  filter(ices %in% sq$icesname)

annex2 <- 
  annex2 %>% 
  mutate(vids = ifelse(is.na(vids), "-9", vids)) %>% 
  # 2022-04-27: Seems like this is no longer required, see 3_data_submission.R
  select(-c(lowermeshsize, uppermeshsize))

if(EXPORT) {
  annex2 %>%
    write_csv(paste0("delivery/iceland_annex2_2009_2022_", TODAY, ".csv"),
              na = "",
              col_names = FALSE)
  annex2 %>% 
    write_rds(paste0("data/iceland_annex2_2009_2022_", TODAY, ".rds"))
  
}

# end: Annex 2 - logbooks


# Annex 1 - ais/vms ------------------------------------------------------------
speed.criterion <-
  tribble(~gid,  ~s1,  ~s2,
          -199, 1.000, 3.000,
          1,    0.375, 2.750,
          2,    0.125, 2.500,
          3,    0.025, 2.000,
          5,    0.250, 3.250,
          6,    2.625, 5.500,
          7,    2.625, 6.000,
          9,    2.375, 4.000,
          12,    0.050, 2.250,
          14,    1.750, 3.250,
          15,    0.500, 5.500,
          38,    0.250, 4.750,
          40,    0.250, 6.000)

fil <- paste0("data/is_vms_visir_y", YEARS, ".rds")
res <- list()
for(y in 1:length(YEARS)) {
  
  print(YEARS[y])
  
  res[[y]] <- 
    read_rds(fil[y]) %>%
    # 2021-08-17: If no speed reported that is most likely associated with
    #             beginning or end of trips (read days). Should fix this upstream.
    filter(!is.na(speed) & !is.na(lat) & !is.na(lon)) %>% 
    # 2021-08-17: In the delivery in May 2021 af filter was applied excluding
    #             the extrapolated datapoints [  filter(vms)   ]. This caused
    #             problems downstream in the calculation of effort (each
    #             ping was supposed to represent a minute, which it no longer
    #             true once a filter has been applied.
    #             A remedy would be to first count the number of pings above
    #             a speed threshold within a visir (each being a minute) and
    #             then filter the points.
    #             The reason one wants to filter points is that one often gets
    #             wrong extrapolations. So on the TODO list is to create an
    #             algorithm right upstream that takes care of "wacky" points
  #filter(vms) %>% 2021-08-17: Used in the May 2021 delivery
  left_join(speed.criterion,
            by = "gid") %>% 
    mutate(fishing = ifelse(speed >= s1 & speed <= s2, TRUE, FALSE)) %>% 
    group_by(visir) %>% 
    mutate(effort = sum(fishing)) %>% # units are minutes
    ungroup() %>% 
    # 2021-08-17: Now we apply the filters on both the vms and the speed
    filter(speed >= s1 & speed <= s2 & vms == TRUE) %>% 
    group_by(visir) %>% 
    mutate(effort = effort / n()) %>% # Here we finally spread the effort among pings
    ungroup()
  
}
ais <- 
  bind_rows(res) %>% 
  # don't think i need gear here
  select(vid, visir:speed, effort) %>% 
  group_by(visir) %>%
  # each ping is a minute
  # 2021-08-10: In the delivery in May 2021 this number was then summed again
  #             downstream. However, the purpose of this calculation is only
  #             to calculate a statistics for spreading the catches of an 
  #             fishing activity to each ping.
  mutate(n.pings.per.visir = n()) %>%
  # 2023-05-21 Ping interval
  mutate(dt = difftime(lead(time), time, units = "mins") |> as.numeric(),
         dt = replace_na(dt, mean(dt, na.rm = TRUE))) |> 
  ungroup()
n0.ais <- nrow(ais)
rm(res)

LGS <- 
  read_rds("data/logbooks.rds") %>% 
  mutate(m4 = ifelse(m4 == "GSN", "GNS", m4),
         m5 = str_sub(m6, 5, 7)) %>% 
  # 2022-04-30 Length class code changed, again!
  mutate(length_class = cut(length,
                            breaks=c(0, 6, 8, 10, 12, 15, 18, 24, 40, 'inf' ), 
                            right = FALSE    ,
                            include.lowest = TRUE,
                            labels =  vlen_icesc$Key))

ais <- 
  ais %>% 
  left_join(LGS %>%
              select(visir, vid0, gid, catch = total, gear.width, length, length_class, kw, m4, m5, m6)) %>%
  mutate(catch = catch / n.pings.per.visir,
         #towtime = towtime / n.pings,
         csquare = vmstools::CSquare(lon, lat, 0.05))
n1.ais_lgs.merged <- nrow(ais)
print(c(n0.ais, n1.ais_lgs.merged))

ais <- 
  ais %>% 
  filter(lat > 48) %>%
  # get rid of points on greenland
  filter(!(lat > 69.5 & lon < -19)) %>% 
  # get rid of points in sweden & finland 
  filter(!(lon > 18.5 & lat <= 69)) %>% 
  # get rid of points in norway
  filter(!(lon >= 5 & lat <= 66)) %>% 
  mutate(year = year(time),
         month = month(time))
n2.ais.filtered <- nrow(ais)
print(c(n0.ais, n2.ais.filtered))
#ais %>% write_rds("data/is_vms_2009-2020-speed_filter_lgs-merged.rds")
twovessels <-
  ais %>%
  select(year, month, csquare, m4, m5, m6, length_class, vid0) %>%
  distinct() %>%
  group_by(year, month, csquare, m4, m5, m6, length_class) %>%
  mutate(n_vessel = n_distinct(vid0)) %>%
  ungroup() %>%
  filter(n_vessel %in% 1:2) %>%
  group_by(year, month, csquare, m4, m6, length_class) %>%
  mutate(vids = case_when(n_vessel == 1 ~ vid0,
                          n_vessel == 2 ~ paste0(vid0, ";", lead(vid0)))) %>%
  slice(1) %>%
  ungroup() %>% 
  select(-vid0)
annex1 <-
  ais %>%
  group_by(year, month, csquare, m4, m5, m6, length_class) %>%
  summarise(speed  = mean(speed),
            # 2023-05-21  Ping interval
            dt = mean(dt),
            # 2021-08-10: The line below was used to derive effort for the data
            #             delivered in May 2021. However this is TOTALLY wrong
            #             because the n.pings is already the sum of the pings
            #             per fishing activity (visir).
            #time   = sum(n.pings) / 60,  # hours
            # 2021-08-10: Corrected effort statistics
            effort = sum(effort) / 60,    # units in hours
            length = mean(length),
            kw     = mean(kw),
            kwh    = kw * effort,
            catch  = sum(catch),
            spread = mean(gear.width),
            n_vessel = n_distinct(vid),
            .groups = "drop")
n.annex1 <- nrow(annex1)


annex1 <- 
  annex1 %>% 
  left_join(twovessels) %>% 
  mutate(type = "VE",
         country = "IS",
         value = NA,
         lowermeshsize = NA,
         uppermeshsize = NA) %>% 
  select(type, country, year, month, n_vessel, vids, csquare,
         m4, m5, lowermeshsize, uppermeshsize, m6,
         length_class, 
         # 2023-05-21 add the dt
         speed, effort, dt, length, kw, kwh, catch, value, spread)
print(c(n.annex1, nrow(annex1)))

annex1 <- 
  annex1 %>% 
  mutate(vids = ifelse(is.na(vids), "-9", vids)) %>% 
  # 2022-04-27: Seems like this is no longer required, see 3_data_submission.R
  select(-c(lowermeshsize, uppermeshsize))

# 2023-06-01: Put median estimate of ping frequency where missing (apparently 
#             can not be NA)
annex1 <- 
  annex1 |> 
  group_by(csquare) |> 
  mutate(m = median(dt, na.rm = TRUE),
         dt = ifelse(is.na(dt), m, dt)) |> 
  ungroup() |> 
  select(-m) |> 
  mutate(dt = replace_na(dt, 11))

# 2023-06-01: gear width of zero should not be allowed, replace with median
annex1 <- 
  annex1 |> 
  group_by(m4) |> 
  mutate(m = median(spread, na.rm = TRUE)) |> 
  ungroup() |> 
  mutate(spread = case_when(m4 %in% c("DRB", "OTB", "OTM", "SND") & is.na(spread) ~ m,
                            .default = spread)) |> 
  select(-m)


if(EXPORT) {
  annex1 %>% 
    write_csv(paste0("delivery/iceland_annex1_2009_2022_", TODAY, ".csv"),
              na = "", 
              col_names = FALSE)
  annex1 %>% 
    write_rds(paste0("data/iceland_annex1_2009_2022_", TODAY, ".rds"))
}



table(annex1$year, useNA = "ifany")

devtools::session_info()
