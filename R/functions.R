grade <-function (x, dx) {
  if (dx > 1) 
    warning("Not tested for grids larger than one")
  brks <- seq(floor(min(x)), ceiling(max(x)), dx)
  ints <- findInterval(x, brks, all.inside = TRUE)
  x <- (brks[ints] + brks[ints + 1])/2
  return(x)
}
csq2pos <- function(d, dx = 0.05) {
  bind_cols(d,
            vmstools::CSquare2LonLat(d$csq, degrees = dx) %>% 
              select(lon = SI_LONG,
                     lat = SI_LATI)) %>% 
    as_tibble()
}
# borrowed from the geo-package
d2ir <- function (lon, lat = NULL, useI = FALSE) {
  
  # if lon is a dataframe
  if (is.null(lat)) {
    lon <- lat$lon
    lat <- lat$lat
  }
  lat <- lat + 1e-06
  lon <- lon + 1e-06
  outside <- lat < 36 | lat >= 85.5 | lon <= -44 | lon > 68.5
  if (any(outside)) 
    warning("Positions outside of ICES statistical area")
  lat <- floor(lat * 2) - 71
  lat <- ifelse(lat < 10, paste("0", lat, sep = ""), lat)
  if (useI) 
    lettersUsed <- LETTERS[1:12]
  else lettersUsed <- LETTERS[c(1:8, 10:13)]
  lon1 <- lettersUsed[(lon + 60)%/%10]
  lon2 <- ifelse(lon1 == "A", floor(lon%%4), floor(lon%%10))
  ir <- paste(lat, lon1, lon2, sep = "")
  ir[outside] <- NA
  ir
}
ir2d <- function (ir, useI = FALSE) {
  lat <- substring(ir, 1, 2)
  lat <- as.numeric(lat)
  lat <- (lat + 71)/2 + 0.25
  lon1 <- substring(ir, 3, 3)
  lon1 <- toupper(lon1)
  lon1 <- match(lon1, LETTERS)
  if (!useI) 
    lon1 <- ifelse(lon1 > 8, lon1 - 1, lon1)
  lon1 <- lon1 - 2
  lon2 <- substring(ir, 4)
  lon2 <- as.numeric(lon2)
  lon <- ifelse(lon1 < 0, -44 + lon2 + 0.5, -40 + 10 * lon1 + 
                  lon2 + 0.5)
  data.frame(lat = lat, lon = lon)
}
read_le <- function(file) {
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
                  isq = 7,
                  dcf4 = 8,
                  dcf5 = 9,
                  dcf6 = 10,
                  lclass = 11,
                  vms = 12,
                  effort = 13,
                  kwd = 14,
                  catch = 15,
                  value = 16) %>% 
    dplyr::mutate(value = as.numeric(value))
}
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
                  length = 14,             # average vessel length
                  kw = 15,                 # average kw
                  kwh = 16,                # kw x effort
                  catch = 17,
                  value = 18,
                  spread = 19) %>% 
    dplyr::mutate(value = as.numeric(value))
}

# support tables ---------------------------------------------------------------
lclass <- 
  tribble(~lclass, ~length_class,
          "A", "<8",
          "B", "08-10",
          "C", "10-12",
          "D", "12-15",
          "E", ">= 15")