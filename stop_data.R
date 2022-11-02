##################################################################################
### Clean start
##################################################################################

rm(list = ls())
gc()


##################################################################################
### Libraries, functions and options
##################################################################################

if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, sf, sp, 
               odbc,
               data.table, # export with UTF-8
               mapview, readxl, httr, tidytransit, devtools)

# detach("package:simplegtfs", unload=TRUE)
# remove.packages("simplegtfs")
# devtools::install_github("bjornsh/simplegtfs", force = TRUE)
library(simplegtfs)


### functions
`%notin%` <- Negate(`%in%`)

# ladda funktioner från Github
eval(parse("https://raw.githubusercontent.com/bjornsh/funktioner/main/func_filer.R", encoding="UTF-8"))
eval(parse("https://raw.githubusercontent.com/bjornsh/funktioner/main/func_vida.R", encoding="UTF-8"))
eval(parse("https://raw.githubusercontent.com/bjornsh/funktioner/main/func_geodata.R", encoding="UTF-8"))
eval(parse("https://raw.githubusercontent.com/bjornsh/funktioner/main/func_gtfs.R", encoding="UTF-8"))
eval(parse("https://raw.githubusercontent.com/bjornsh/funktioner/main/func_download_data_from_github.R", encoding="UTF-8"))



# Options 
options(scipen=999) # avoid scientific notation
options(dplyr.summarise.inform = FALSE) # avoid dplyr message


##################################################################################
### Define variables
##################################################################################

# RKM för GTFS data !!!!!!!!!!!!!!!!!!!!!!!
rkm = "ul"

# om några linjetyper ska tas bort !!!!!!!!!!!!!!!!!!!!!!!
linjetyp_som_ska_bort = c("Sjukresebuss",
                          "Skolbuss",
                          "")

# tidsperiod för incheckningsdata 
# som ingår i beräkningen av antal personer som byter vid en viss hållplats 
hpl_byte_start_datum = "2022-10-03"
hpl_byte_stop_datum = "2022-10-07"



##################################################################################
### Define paths and create data directories (if not already exist)
##################################################################################

dir.create(paste0(getwd(), "/output/"))
dir.create(paste0(getwd(), "/shapefile"))


# define local paths
folder_input = paste0(getwd(), "/input/")
folder_output = paste0(getwd(), "/output/")
folder_shapefile = paste0(getwd(), "/shapefile")
folder_github = "https://github.com/bjornsh/gis_data/raw/main/" 


##################################################################################
### Connect SQL database
##################################################################################
### requires VPN

con <- dbConnect(odbc(),
                 Driver = "SQL Server",
                 Server = rstudioapi::askForPassword("Server address"), 
                 Database = "ULDataLake",
                 UID = rstudioapi::askForPassword("User ID"),
                 PWD = rstudioapi::askForPassword("Database password"))



##################################################################################
### Load data - lokala filer
##################################################################################

### planerade byte vid en hpl
byte = read_delim(paste0(folder_input, "planerade_byte.csv"), 
                  delim = ";",
                  locale =locale("sv",encoding="ISO-8859-1"))


### Alla hållplatser med terminal
terminal = read.delim(paste0(folder_input, "terminal.csv"), sep = ";")


### Alla hållplatser med pendlarparkering
parkering = readxl::read_excel(paste0(folder_input, "pendlarparkering_koordinater.xlsx"), skip = 1)


### OSM destinations
# skapas med https://github.com/bjornsh/osm_poi/blob/main/osm_destination_area.R
osm = read_sf(paste0("C:/OneDrive - Region Uppsala/github/osm_poi/output/destination_area.shp"))


### Hållplatsdatabas, Easit export
hpl_db = read_excel("Z:/a_data/hpl_databas/hallplats_db_20220802.xlsx")


### SCB dag- och nattbefolkning (100x100m)
natt = read_delim("Z:/a_data/scb/aa_befolkning/nattbef_100m_20211231.csv",
                  delim = ",",
                  locale = locale("sv",encoding="ISO-8859-1")) %>% 
  mutate(fBBefolkning = replace(fBBefolkning, fBBefolkning == "..C", "2.5")) %>% 
  select(-Region) %>% 
  rename(ruta = 1,
         nattbef = 2) %>% 
  mutate(nattbef = as.numeric(nattbef))


dag = read_delim("Z:/a_data/scb/aa_befolkning/dagbef_100m_20201231.csv",
                 delim = ",",
                 locale = locale("sv",encoding="ISO-8859-1")) %>% 
  mutate(Personer = replace(Personer, Personer == "..C", "2.5")) %>% 
  select(1, 5) %>% 
  filter(Personer != "-") %>% 
  rename(ruta = 1,
         dagbef = 2) %>% 
  mutate(dagbef = as.numeric(dagbef))




##################################################################################
### Load data - db, API etc 
##################################################################################

# aktuellt namn har NA i ExistsToDate
# keep skript for reference but hpl names are now being fetched from GTFS table 
hpl_alla_namn = tbl(con, dbplyr::in_schema("MasterData", "StopArea")) %>%
  select(Name, PubTransStopAreaNumber, ExistsToDate) %>% 
  filter(!is.na(PubTransStopAreaNumber)) %>%
  rename(hpl_namn = 1, hpl_id = 2) %>% 
  collect() 
  


### APR Påstigande
# rolling average for last 12 months
apr = funk_apr_hpl_latest_month(con = con) %>% 
  # Eftersom det saknas APR data för SL tåg, lägg till ett medelvärde
  mutate(Boardings = ifelse(hpl_id == "780050", Boardings + 2000, # Knivsta station
                          ifelse(hpl_id == "700600", Boardings + 9000, # Uppsala C
                                 Boardings)))


### "faktiska" byte vid en hpl (Atries incheckningsdata)
# antal personer per hpl och en vardagsdygn som 
# registrerades via blippning inom 60min efter föregående incheckning 
byte_fakt = funk_byte(con, 
                      datum_start = hpl_byte_start_datum, 
                      datum_stop = hpl_byte_stop_datum, 
                      max_sec_between_incheck = "3600") %>% 
  mutate(hpl_id = as.numeric(hpl_id))



#### GTFS

# download GTFS
simplegtfs::download_gtfs("ul")

# read GTFS
gtfs <- tidytransit::read_gtfs(paste0("gtfs_data/", list.files("gtfs_data")[1])) %>% 
  # remove all data except for today (ie vanlig arbetsdag)
  filter_feed_by_date(., Sys.Date()) %>% 
  # remove all data related to certain line types
  remove_linetype(., linjetyp_som_ska_bort)
  


### admin boundaries
# kommun
kommun <- load_sf_kommun() %>%  
  # filter alla kommuner i Uppsala län 
  # plus kommuner utanför länet där det finns UL hållplatser
  filter(LANSKOD == "3" |
           KOMMUNNAMN == "Västerås" |
           KOMMUNNAMN == "Gävle" |
           KOMMUNNAMN == "Norrtälje" |
           KOMMUNNAMN == "Sigtuna" |
           KOMMUNNAMN == "Upplands-Bro" |
           KOMMUNNAMN == "Sala") %>% 
  st_transform(4326)


### tätort
tatort <- load_sf_tatort() %>% 
  filter(LANSKOD == "3" |
           NAMN1 == "Västerås" | 
           NAMN1 == "Gävle") %>% 
  st_transform(4326)




##################################################################################
##################################################################################
### Transform data
##################################################################################
##################################################################################

vaghall = hpl_db %>% 
  filter(Status == "Trafikeras",
         !is.na(Väghållare)) %>% 
  select(hpl_id = Hållplatsnummer, Väghållare) %>% 
  distinct() %>% 
  mutate(Väghållare = tolower(gsub( " .*$", "", Väghållare))) %>% 
  # där det finns mer än 1 väghållare sätt som "flera"
  group_by(hpl_id) %>% 
  arrange(Väghållare) %>% 
  summarise(Väghållare = paste(Väghållare, collapse = "/")) %>% 
  mutate(Väghållare = str_replace_all(Väghållare, 
                                      c("enskild/statlig" = "flera",
                                        "enskild/kommunal" = "flera",
                                        "kommunal/statlig" = "flera"))) %>% 
  mutate(hpl_id = as.numeric(hpl_id))



### Transform GTFS data

# df med korrekt hpl namn och ID
hpl_namn = stop_id_name(gtfs) %>% 
  spelling()

# Antal linjer per hållplats per vardagsdygn
antal_linjer_per_hpl = n_lines_stop(gtfs)


### Alla linjer per hpl
linjer_per_hpl = lines_stop(gtfs)



# add metadata to each hpl: kommun and tätort
hpl_kommun_tatort = stop_sf(gtfs) %>% 
  st_join(., kommun) %>% 
  select(1:3) %>% 
  st_join(., tatort) %>% 
  select(hpl_id, kommun = KOMMUNNAMN, tatort = NAMN1)




### hur många kommuner kan man nå från en hpl utan att byta?

# för varje linje, vilka hpl trafikeras och i vilken kommun och tätort finns hpl
linje_hpl_kommun_tatort = gtfs$routes %>% 
  left_join(., gtfs$trips, by = "route_id") %>% 
  left_join(., gtfs$stop_times, by = "trip_id") %>% 
  # create hpl_id
  mutate(hpl_id = as.integer(substr(stop_id, 8, 13))) %>% 
  select(route_short_name, hpl_id) %>%
  # remove duplicates
  distinct() %>% 
  # join kommun/tätort per hpl
  left_join(., stop_sf(gtfs) %>% 
              st_join(., kommun) %>% 
              select(1:3) %>% 
              st_join(., tatort) %>% 
              select(hpl_id, kommun = KOMMUNNAMN, tatort = NAMN1) %>% 
              as.data.frame() %>% 
              select(-geometry), 
            by = "hpl_id") %>% 
  mutate(route_short_name = as.integer(route_short_name))


# vilka linjer trafikerar vilka kommuner
linje_kommun = linje_hpl_kommun_tatort %>% 
  group_by(route_short_name, kommun) %>% 
  tally() %>% 
  select(-n)  %>% 
  filter(!is.na(kommun))

# antal kommuner som kan nås från en hpl   
antal_kommun_per_hpl = linje_hpl_kommun_tatort %>% 
  select(hpl_id, route_short_name) %>% 
  left_join(., linje_kommun, by = "route_short_name") %>% 
  select(-route_short_name) %>% 
  distinct() %>% 
  arrange(hpl_id) %>% 
  group_by(hpl_id) %>% 
  # subtract 1 as all hpl have min 1, ie kommun where hpl is located
  summarise(antal_kommuner = n() - 1) %>% 
  arrange(desc(antal_kommuner))
  


### hur många tätorter kan man nå från en hpl?
# vilka linjer trafikerar vilka tätorter
linje_tatort = linje_hpl_kommun_tatort %>% 
  group_by(route_short_name, tatort) %>% 
  tally() %>% 
  select(-n)  %>%
  mutate(tatort = replace_na(tatort, "Landsbyggd"))

# antal tätorter som kan nås från en hpl   
antal_tatort_per_hpl = linje_hpl_kommun_tatort %>% 
  select(hpl_id, route_short_name) %>% 
  left_join(., linje_tatort, by = "route_short_name") %>% 
  select(-route_short_name) %>% 
  distinct() %>% 
  arrange(hpl_id) %>% 
  group_by(hpl_id) %>% 
  # subtract 1 as all hpl have min 1, 
  # dvs 2 means one can travel to one kommun different from kommun where hpl is
  summarise(antal_tatort = n()) %>% 
  arrange(desc(antal_tatort)) %>% 
  # om en hpl ligger i en tätort måste subtraheras 1 
  # men inte om den är utanför en tätort
  # join original tätort data för hpl
  left_join(., hpl_kommun_tatort %>% 
              as.data.frame() %>% 
              select(hpl_id, tatort),
            by = "hpl_id") %>% 
  mutate(antal_tatort = ifelse(!is.na(tatort), antal_tatort - 1,
                               antal_tatort)) %>% 
  select(-tatort)


### Antal departures per hpl och vardagsdygn
antal_departure_per_hpl = departure_stop(gtfs)



### Linjetyp per hållplats
linjetyp_per_hpl = gtfs$routes %>%
  left_join(., gtfs$trips, by = "route_id") %>% 
  left_join(., gtfs$stop_times, by = "trip_id") %>% 
  mutate(hpl_id = as.integer(substr(stop_id, 8, 13))) %>% 
  select(hpl_id, route_desc) %>%
  distinct()


### Ansl Buss med spår/express-linje
byte_vanlig_express = gtfs$routes %>%
  left_join(., gtfs$trips, by = "route_id") %>% 
  left_join(., gtfs$stop_times, by = "trip_id") %>% 
  mutate(hpl_id = as.integer(substr(stop_id, 8, 13))) %>% 
  mutate(linje_typ = ifelse(route_desc == "Expressbuss" |
                              route_desc == "Upptåget" |
                              route_desc == "SL pendeltåg", "express", "vanlig")) %>% 
  select(hpl_id, linje_typ) %>% 
  distinct() %>% 
  group_by(hpl_id) %>% 
  summarise(antal_linje_typ = n()) %>% 
  mutate(byte_vanlig_express = ifelse(antal_linje_typ > 1, "ja", "nej")) %>% 
  select(-antal_linje_typ)
  


### Turn parkering koordinater into SF
parkering_sf = parkering %>%
  # select variables used in geodata manipulation
  mutate(x = str_replace(X, ",", "."), # replace decimal "," with "."
         y = str_replace(Y, ",", ".")) %>% 
  mutate_at(vars(x, y), as.numeric) %>%   # coordinates must be numeric
  # skapa SF object
  st_as_sf(
    coords = c("Y", "X"),
    agr = "constant",
    crs = 4326,        # assign WGS84 as CRS
    stringsAsFactors = FALSE,
    remove = TRUE
  ) 


### extract hpl koordinater från GTFS
hpl_sf = stop_sf(gtfs)



har_parkering = hpl_sf %>% 
  st_join(., st_buffer(parkering_sf, 500)) %>% 
  filter(!is.na(Station)) %>%
  mutate(parkering = "ja") %>%
  as.data.frame() %>% 
  select(hpl_id, parkering) %>% 
  distinct()
  




##### Pairwise calculation of overlapping routes for all lines using a certain hpl
# 1 = 1 kombination of lines has less overlap than specified below
# 5 = 5 kombinations of lines have less overlap than specified below


# min antal linjer som trafikerar en hpl för att det ingår i analysen
min_antal_linjer_hpl = 2

# buffer distans för att hitta overlap mellan linjer
buffer_meter = 500

# skapa vector med relevanta hpl
urval_hpl = antal_linjer_per_hpl$hpl_id[antal_linjer_per_hpl$antal_linjer >= min_antal_linjer_hpl]

#urval_hpl = "780438"

# beräkna overlap för alla linje kombinationer som trafikerar hpl som ingår i analysen
# 16min för 381 hpl
yyy = c()
resultat_overlap = c()

time_start = Sys.time()

for(i in 1:length(urval_hpl)){
  yyy <- funk_linje_overlap(gtfs, urval_hpl[i], buffer_meter)
  
  resultat_overlap <<- rbind(resultat_overlap, yyy)
}

time_stop = Sys.time()
time_stop - time_start

# write.csv2(resultat_overlap, paste0(folder_input, "resultat_overlap.csv"), row.names = FALSE)
# resultat_overlap = read.csv2(paste0(folder_input, "resultat_overlap.csv"))

# calculate mean overlap for all line combinations 
resultat_mean_overlap = resultat_overlap %>% 
  group_by(hpl_id) %>% 
  summarise(mean_overlap = mean(mean_overlap))


# calculate the number of line combinations with overlapping area of less than X %
resultat_antal_linjer_overlap = resultat_overlap %>%
  # keep all lines with less than X % overlap
  filter(mean_overlap < 0.25) %>% 
  # count the line combinations with less than X % overlap
  group_by(hpl_id) %>% 
  summarise(antal_linje_kombi = n())
  


###### Hpl nära en målpunkt
hpl_near_osm_destination = hpl_sf %>% 
  st_join(., st_transform(osm, 4326)) %>%
  filter(!is.na(FID)) %>% 
  as.data.frame() %>% 
  select(-geometry, -FID) %>% 
  mutate(hpl_malpunkt = "ja")




### Dag- och nattbefolkning inom 1000m av en hållplats

# Convert SCB tbl to SF
dag_sf <- rutid_till_centercoord(dag, position = 1) %>% 
  st_transform(4326)

natt_sf <- rutid_till_centercoord(natt, position = 1) %>% 
  st_transform(4326)



# nattbefolkning (100m centerkoordinater) inom 1000m radius från hållplatsen
hpl_natt = hpl_sf %>% 
  st_buffer(., 1000) %>% 
  st_join(., natt_sf) %>% 
  as.data.frame() %>% 
  select(-geometry) %>% 
  mutate(nattbef = replace_na(nattbef, 0)) %>% 
  group_by(hpl_id) %>% 
  summarise(nattbef = round(sum(nattbef), 0))


# dagbefolkning (100m centerkoordinater) inom 1000m radius från hållplatsen
hpl_dag = hpl_sf %>% 
  st_buffer(., 1000) %>% 
  st_join(., dag_sf) %>% 
  as.data.frame() %>% 
  select(-geometry) %>% 
  mutate(dagbef = replace_na(dagbef, 0)) %>% 
  group_by(hpl_id) %>% 
  summarise(dagbef = round(sum(dagbef), 0))



##################################################################################
##################################################################################
### Resultat
##################################################################################
##################################################################################



### Oviktat resultat, dvs alla kriterier har samma vikt
merge = hpl_sf %>% 
  left_join(., hpl_namn, by = "hpl_id") %>%  
  left_join(., select(as.data.frame(hpl_kommun_tatort), - geometry), by = "hpl_id") %>% 
  left_join(., byte[,c("hpl_id", "planerade_byte")], by = "hpl_id") %>% 
  left_join(., terminal[,c("hpl_id", "terminal")], by = "hpl_id") %>% 
  left_join(., apr[,c("hpl_id", "Boardings", "Alightings")], by = "hpl_id") %>% 
  left_join(., har_parkering, by = "hpl_id") %>%  
  left_join(., antal_linjer_per_hpl, by = "hpl_id") %>%  
  left_join(., antal_departure_per_hpl, by = "hpl_id") %>% 
  left_join(., byte_vanlig_express, by = "hpl_id") %>% 
  left_join(., hpl_near_osm_destination, by = "hpl_id") %>% 
  left_join(., resultat_antal_linjer_overlap, by = "hpl_id") %>% 
  left_join(., byte_fakt, by = "hpl_id") %>% 
  left_join(., hpl_natt, by = "hpl_id") %>% 
  left_join(., hpl_dag, by = "hpl_id") %>% 
  left_join(., vaghall, by = "hpl_id") %>% 
  left_join(., antal_tatort_per_hpl, by = "hpl_id") %>%
  left_join(., antal_kommun_per_hpl, by = "hpl_id") %>% 
  distinct() %>% 
  ### inkludera bara relevanta kommuner
  filter(kommun == "Östhammar" | 
           kommun == "Älvkarleby" |
           kommun == "Knivsta" |
           kommun == "Heby" |
           kommun == "Håbo" |
           kommun == "Tierp" |
           kommun == "Enköping" |
           kommun == "Uppsala" |
           kommun == "Västerås") %>% 
  mutate(tatort_bin = ifelse(is.na(tatort), "Landsbyggd", "Tätort"))

# Ta bort hållplats med > 1 hpl namn stavningsvariant
# eftersom all annat data bör vara samma funkar en enkel distinct metod
merge = distinct(merge, hpl_id, .keep_all = TRUE)



##################################################################################
### exportera data
##################################################################################


# convert to SF, add coordinates and remove geometry
merge1 = cbind(merge, st_coordinates(merge1)) %>% 
  as.data.frame() %>% 
  select(-geometry)  

data.table::fwrite(merge1, "export_merge.csv", 
                   sep = ";", 
                   dec = ",", # export med komma som decimal sep
                   bom = TRUE) # export with UTF-8


