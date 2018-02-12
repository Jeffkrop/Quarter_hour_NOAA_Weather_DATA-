

#Get quarter hour weather data for only MN and WI for years 2012 to 2017  


library(sp)
library(dplyr)
library(tidyr)
library(knitr)
library(parallel)


file <- "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"

repeat{
  try(download.file(file, "isd-history.csv", quiet = TRUE))
  if (file.info("isd-history.csv")$size > 0) {
    break
  }
}

#Create data frame of weather stations that are in the United States and were in operation in the year 2016
all_stations <- read.csv("isd-history.csv") %>%
  filter(CTRY=="US") %>%
  filter(BEGIN <= 20160101 & END >=20161231) %>%
  mutate(st_index=row_number()) %>%
  arrange(STATION.NAME)

# Because the station data is only available for year we can remove the month and day from the begin and end fields. 
all_stations$BEGIN <- as.numeric(substr(all_stations$BEGIN, 1, 4))
all_stations$END <- as.numeric(substr(all_stations$END, 1, 4))

#I need to filter the the stations dataset so that I only get data for MN and WI
stations <- all_stations %>% filter(STATE == "WI" | STATE == "MN")

distances <- all_stations %>% select(USAF, WBAN, STATION.NAME, STATE) %>% 
  ungroup %>%
  as.data.frame()


#Get data

outputs <- as.data.frame(matrix(NA, dim(stations)[1], 2))
names(outputs) <- c("FILE", "STATUS")

system.time(for (y in 2016:2017) {
  y.mi.list <- stations[stations$BEGIN <= y & stations$END >= y, ]
  for (s in 1:dim(y.mi.list)[1]) {
    outputs[s, 1] <- paste(sprintf("%06d", y.mi.list[s,1]), "-", sprintf("%05d", y.mi.list[s,2]), "-", y, ".gz", sep = "")
    wget <- paste("wget -P data/NOAA_temp_data ftp://ftp.ncdc.noaa.gov/pub/data/noaa/", y, "/", outputs[s, 1], sep = "")
    outputs[s, 2] <- try(system(wget, intern = FALSE, ignore.stderr = TRUE))
  }
})

print(outputs %>% group_by(STATUS) %>% summarise(count = n()))


system("gunzip -r data/2017", intern = FALSE, ignore.stderr = TRUE)

NOAA_files2017 <- list.files("data/NOAA_temp_data")
column.widths <- c(4, 6, 5, 4, 2, 2, 2, 2, 1, 6, 7, 5, 5, 5, 4, 3, 1, 1, 4, 1, 5, 1, 1, 1, 6,
                   1, 1, 1, 5, 1, 5, 1, 5, 1)

#The code below is run in PARALLEL meaning it runs on more then one core. My Mac has 8 cores so I am running it on 7 leaving one for the rest of the computer. 
no_cores <- detectCores()-1
cl <- makeCluster(no_cores, type="FORK")
clusterExport(cl, "column.widths")

system.time(Station_data <- do.call("rbind", 
                            parLapply(cl, NOAA_files2017,
                            function(x) 
                            read.fwf(paste("data/NOAA_temp_data/", x, sep=''),  column.widths,
                                    stringsAsFactors = FALSE))))

#Always stop the cluster to free up the cores on the machine. 
stopCluster(cl)

files.to.delete <- dir("/Users/jeffkropelnicki/Desktop/Winfield files/Quarter_Hour_NOAA_Weather_Data/Quarter_hour_NOAA_Weather_DATA/data/NOAA_temp_data",pattern=".gz",recursive=T,full.names=T)
invisible(file.remove(files.to.delete))

Station_data <- Station_data[, c(2:8, 10:11, 13, 16, 19, 29, 31, 33)]
names(Station_data) <- c("USAFID", "WBAN", "YR", "M", "D", "HR", "MIN", "LAT", "LONG", "ELEV",
                         "WIND.DIR", "WIND.SPD", "TEMP", "DEW.POINT", "ATM.PRES")

Station_data$LAT <- Station_data$LAT/1000
Station_data$LONG <- Station_data$LONG/1000
Station_data$WIND.SPD <- Station_data$WIND.SPD/10
Station_data$TEMP <- Station_data$TEMP/10
Station_data$DEW.POINT <- Station_data$DEW.POINT/10

# I want to remove that 999 and put NA in its place. 
Station_data$WIND.DIR[Station_data$WIND.DIR == 999] <- NA
Station_data$TEMP[Station_data$TEMP == 999.9] <- NA
Station_data$WIND.SPD[Station_data$WIND.SPD == 999.9] <- NA
Station_data$DEW.POINT[Station_data$DEW.POINT == 999.9] <- NA
Station_data$ATM.PRES[Station_data$ATM.PRES == 99999] <- NA

# Calculate Relative Humidity before converting temp and dewpoint to Fahrenheit  
Station_data <- Station_data %>% 
  mutate(Relative_Humidity = round(100*(exp((17.625*DEW.POINT)/(243.04+DEW.POINT))/exp((17.625*TEMP)/(243.04+TEMP)))))


Station_data_2017 <- Station_data %>% 
  filter(YR == 2017 & YR == 2016) %>%
  filter(M > 02) %>% 
  filter(M < 09) %>% 
  mutate(temp_fahr = round(TEMP *1.8 + 32)) %>% 
  mutate(wind_spd_mph = round(WIND.SPD * 2.2369, digits = 3)) %>% 
  mutate(dewpoint = round(DEW.POINT*1.8 + 32)) 


write.csv(Station_data, "data.csv")









