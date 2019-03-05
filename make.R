# Drake Makefile for project

library(here)
library(drake)

source(here("R", "funs.R"))

NHPD_DUMP_LOCATION <- here('data', 'national_housing_preservation_db/nhpd_dump__03_02_2019.xlsx')
CTCAC_FILE_LOCATION <- here('data', 'ctcac_projects__03_03_2019.xlsx')
SOMA_WEST_JOINED_JSON_LOCATION <- here('output', 'soma_west_joined.geojson')
SOMA_WEST_JOINED_CSV_LOCATION <- here('output', 'soma_west_joined.csv')
SOMA_WEST_CTAC_JSON_LOCATION <- here('output', 'soma_west_ctac.geojson')
SOMA_WEST_CTAC_CSV_LOCATION <- here('output', 'soma_west_ctac.csv')
SOMA_WEST_MOHCD_JSON_LOCATION <- here('output', 'soma_west_mohcd.geojson')
SOMA_WEST_MOHCD_CSV_LOCATION <- here('output', 'soma_west_mohcd.csv')

plan <- drake::drake_plan(
  soma_west_parcels = parse_soma_west_tables(),
  soma_west_parcels_clean = clean_soma_west_parcels(soma_west_parcels),
  soma_west_lot_nums = soma_west_parcels_clean$lot_num,
  soma_west_block_nums = soma_west_parcels_clean$block_num,
  soma_west_city_data_parcels = get_sf_parcel_data(soma_west_lot_nums, soma_west_block_nums),
  soma_west_joined = soma_west_parcels_clean %>% 
    inner_join(soma_west_city_data_parcels, by = c('lot_num', 'block_num')) %>%
    dplyr::filter(!sf::st_is_empty(geometry)) %>%
    sf::st_as_sf()
    ,
  sf::st_write(soma_west_joined, file_out(SOMA_WEST_JOINED_JSON_LOCATION), delete_dsn=TRUE),
  readr::write_csv(soma_west_joined %>% as.data.frame %>% select(-geometry), file_out(SOMA_WEST_JOINED_CSV_LOCATION)),
  nhpd = readxl::read_excel(file_in(NHPD_DUMP_LOCATION)),
  nhpd_geo = sf::st_as_sf(nhpd, coords = c('Longitude', 'Latitude'), crs = 4326),
  ctac_projects = readxl::read_excel(file_in(CTCAC_FILE_LOCATION), sheet = "San Francisco"),
  ctac_projects_coded = geocode_ctac(ctac_projects),
  mohcd_projects = get_mohcd_data(),
  soma_west_mohcd = join_mohcd_soma_west(mohcd_projects, soma_west_joined),
  soma_west_ctac = join_ctac_soma_west(ctac_projects_coded, soma_west_joined),
  sf::st_write(soma_west_ctac, file_out(SOMA_WEST_CTAC_JSON_LOCATION), delete_dsn=TRUE),
  readr::write_csv(soma_west_ctac %>% as.data.frame %>% select(-geometry), file_out(SOMA_WEST_CTAC_CSV_LOCATION)),
  sf::st_write(soma_west_mohcd, file_out(SOMA_WEST_MOHCD_JSON_LOCATION), delete_dsn=TRUE),
  readr::write_csv(soma_west_mohcd %>% as.data.frame %>% select(-geometry), file_out(SOMA_WEST_MOHCD_CSV_LOCATION)),
  strings_in_dots = "literals"
)

drake::make(plan, verbose = TRUE)
