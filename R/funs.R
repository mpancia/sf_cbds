library(pdftools)
library(tabulizer)
library(here)
library(tidyverse)
library(magrittr)
library(assertr)
library(assertthat)
library(stringr)
library(ggmap)
library(dotenv)
library(drake)
library(RSocrata)
library(glue)
library(soql)
library(jsonlite)
library(geojsonio)

dotenv::load_dot_env(here(".env"))
GOOGLE_API_KEY <- Sys.getenv("GOOGLE_API_KEY")
SOCRATA_APP_TOKEN <- Sys.getenv("SF_SOCRATA_APP_KEY")
SOCRATA_EMAIL <- Sys.getenv("SF_SOCRATA_EMAIL")
SOCRATA_PASSWORD <- Sys.getenv("SF_SOCRATA_PASSWORD")

SF_PARCEL_SOCRATA_ENDPOINT <- "https://data.sfgov.org/resource/6b2n-v87s.geojson"
SF_MOHCD_SOCRATA_ENDPOINT <- "https://data.sfgov.org/resource/yd5s-bd6e.geojson"

get_sf_parcel_data <- function(lot_nums, block_nums){
  blocklots <- paste0(block_nums, lot_nums)
  blocklot_where <- glue_sql('blklot in ({blocklots*})', blocklots=blocklots, .con = DBI::ANSI()) %>% as.character()
  query <- soql() %>%
    soql::soql_add_endpoint(SF_PARCEL_SOCRATA_ENDPOINT) %>%
    soql::soql_where(blocklot_where) %>%
    soql::soql_order('blklot') %>%
    soql::soql_limit(50000) %>%
    as.character()
  response <- httr::GET(query, httr::authenticate(SOCRATA_EMAIL, SOCRATA_PASSWORD))
  cont <- httr::content(response, as = 'text')
  df  <- sf::st_read(cont)
  df <- df[!sf::st_is_empty(df$geometry),]
  df
}

get_mohcd_data <- function(){
  query <- soql() %>%
    soql::soql_add_endpoint(SF_MOHCD_SOCRATA_ENDPOINT) %>%
    soql::soql_limit(50000) %>%
    as.character()
  response <- httr::GET(query, httr::authenticate(SOCRATA_EMAIL, SOCRATA_PASSWORD))
  cont <- httr::content(response, as = 'text')
  df  <- sf::st_read(cont)
  df <- df[!sf::st_is_empty(df$geometry),]
  df
}

parse_soma_west_tables <- function(){
  location <- here("data", "engineer_reports/soma_west_engineer_report__10_01_2018.pdf")
  public_colnames <- c("parcel", "address", "owner", "assessment_total", "assessment_pct")
  public_table_1 <- tabulizer::extract_tables(file =location, 
                                             pages = 23, 
                                             output = "data.frame")[[1]] %>%
    set_colnames(public_colnames)
  colnames(public_table_1) <- public_colnames
  public_table_2 <- tabulizer::extract_tables(file =location, 
                                             pages = 24,
                                             area = list(c(50.48884,69.84585,291.57809,548.58012)))[[1]] %>%
    as.data.frame %>%
    set_colnames(public_colnames) %>%
    dplyr::filter(str_length(parcel) > 0)
  public_tables <- dplyr::bind_rows(public_table_1, public_table_2)
  public_tables$parcel[[2]] <- paste(public_tables$parcel[[1]], public_tables$parcel[[2]])
  public_tables$address[[2]] <- paste(public_tables$address[[2]], public_tables$address[[3]])
  public_tables %<>% dplyr::slice(c(-1, -3)) %>%
    dplyr::mutate(assessment_pct = readr::parse_number(assessment_pct),
                  assessment_total = readr::parse_number(assessment_total),
                  property_type = "public") %>%
    verify(sum(.$assessment_pct) == 11.76)
  first_private_page <- 26
  first_private_page_area <- c(top = 204.8275862069, left = 73.862070491008, bottom = 728.543610547667, right = 552.59634726388)
  private_pages <- 27:51
  private_colnames <- rep(c("parcel", "address", "assessment_total", "assessment_pct"), 2)
  first_private_table <- tabulizer::extract_tables(file = location,
                                                   area = list(first_private_page_area),
                                                   pages = first_private_page, 
                                                   output = "data.frame")[[1]] %>%
    set_colnames(private_colnames) %>%
    bind_rows(.[,1:4], .[,5:8])
  private_table_list <- tabulizer::extract_tables(file = location,
                                                  pages = private_pages) %>%
    lapply(function(x) as.data.frame(x, stringsAsFactors = FALSE))
  
  private_table_cols <- private_table_list %>% 
    lapply(ncol) %>% 
    unlist %>% 
    unique
  assert_that(length(private_table_cols) == 1) # Check to make sure tables have same width
  
  private_tables <- bind_rows(private_table_list) %>% 
    set_colnames(private_colnames) %>%
    bind_rows(.[,1:4], .[,5:8]) %>%
    bind_rows(first_private_table) %>%
    dplyr::mutate(assessment_pct = readr::parse_number(assessment_pct),
                  assessment_total = readr::parse_number(assessment_total),
                  property_type = "private") %>%
    distinct() %>%
    mutate(rn = row_number())
  
  bad_rows <- private_tables %>%
    .[!complete.cases(private_tables), ]
  
  extra_addresses <- bad_rows %>% 
    dplyr::filter(str_length(parcel) == 0)
  
  extra_parcels <- bad_rows %>% 
    dplyr::filter(! rn %in% extra_addresses$rn)
  
  private_tables_len <- nrow(private_tables)
  private_tables <- private_tables %>% 
    dplyr::filter(! rn %in% extra_addresses$rn) %>%
    verify(nrow(.) == (private_tables_len - nrow(extra_addresses)))
  private_tables_len <- nrow(private_tables)
  
  for(i in 1:(nrow(extra_parcels))){
    curr_rn <- extra_parcels[i, 'rn']
    curr_parcel <- extra_parcels[i, 'parcel']
    curr_row <- private_tables %>% dplyr::filter(rn == curr_rn)
    prev_row <- private_tables %>% dplyr::filter(rn == curr_rn -1 )
    if((curr_rn + 1) %in% private_tables$rn) {
      next_row <- private_tables %>% dplyr::filter(rn == curr_rn + 1)
      if(str_length(next_row$parcel) < 5){
        private_tables[private_tables$rn == next_row$rn, 'parcel'] <- paste(curr_parcel, next_row$parcel)
        private_tables <- private_tables %>% 
          dplyr::filter(rn != curr_rn)
      }
    } 
  }
  assert_that(nrow(private_tables) == private_tables_len - nrow(extra_parcels))
  joined_table <- bind_rows(public_tables, private_tables %>% select(-rn)) %>%
    distinct
  joined_table
}

clean_soma_west_parcels <- function(soma_west_parcels){
  soma_west_parcels_split <- soma_west_parcels$parcel %>% 
    str_squish() %>%
    str_split_fixed("[:space:]", 2)
  soma_west_parcels %<>%
    mutate(block_num = soma_west_parcels_split[,1],
           lot_num = soma_west_parcels_split[,2]) %>%
    mutate(lot_num = replace(lot_num, str_length(lot_num) == 0, NA))
  soma_west_parcels
}

geocode_ctac <- function(ctac_projects){
  register_google(GOOGLE_API_KEY)
  geocodes <- ggmap::geocode(ctac_projects$`Project Address`, source='google')  
  ctac_projects$lon <- geocodes$lon
  ctac_projects$lat <- geocodes$lat
  uncoded_projects <- ctac_projects %>% dplyr::filter(is.na(lon))
  while(nrow(uncoded_projects) > 0){
    ctac_projects <- ctac_projects %>% dplyr::filter(!is.na(lon))
    geocode_retry <- ggmap::geocode(uncoded_projects$`Project Address`, source='google')
    uncoded_projects$lon <- geocode_retry$lon
    uncoded_projects$lat <- geocode_retry$lat
    ctac_projects <- bind_rows(ctac_projects, uncoded_projects)
    uncoded_projects <- ctac_projects %>% dplyr::filter(is.na(lon))
  }
  ctac_projects_coded <- sf::st_as_sf(ctac_projects, coords = c("lon", "lat"), crs = 4326)
  ctac_projects_coded
}

join_ctac_soma_west <- function(ctac_projects_coded, soma_west_joined){
  ctac_projects_coded <- sf::st_transform(ctac_projects_coded, sf::st_crs(soma_west_joined))
  sf::st_join(soma_west_joined, ctac_projects_coded, left = FALSE) %>%
    group_by(parcel) %>%
    slice(c(1))
}

join_mohcd_soma_west <- function(mohcd_projects, soma_west_joined){
  mohcd_projects <- sf::st_transform(mohcd_projects, sf::st_crs(soma_west_joined))
  joined <- sf::st_join(soma_west_joined, mohcd_projects, left = FALSE) %>%
    group_by(parcel) %>%
    slice(c(1)) %>% 
    select(-`X..computed_region_fyvs_ahh9`,
           -`X..computed_region_6qbp_sg9q`,
           -`X..computed_region_p5aj_wyqh`,
           -`latitude`,
           -`longitude`,
           -`X..computed_region_bh8s_q3mv`,
           -`X..computed_region_yftq_j783`,
           -`X..computed_region_26cr_cadq`,
           -`project_location_city`,
           -`mapblklot`,
           -`multigeom`,
           -`asr_secure`,
           -`to_st`,
           -`from_st`,
           -`street`,
           -`st_type`,
           -`project_location_address`,
           -`project_location_zip`,
           -`project_address`,
           -`street_type`,
           -`street_number`,
           -`street_name`,
           -`datemap_dr`,
           -`datemap_ad`,
           -`planning_neighborhood`,
           -`project_location_state`,
           -`daterec_ad`,
           -`daterec_dr`,
           -`blklot`,
           -`target_fid`,
           -`join_count`,
           -`odd_even`,
           -`supervisor_district`,
           -`neighborhood`,
           -`owner`,
           -`X..computed_region_rxqg_mtj9`,
           -`X..computed_region_ajp5_b2md`,
           -`X..computed_region_qgnn_b9vv`)
  joined %>% 
    mutate_at(vars(contains('unit')), .funs = list(as.numeric)) %>%
    mutate_at(vars(contains('total')), .funs = list(as.numeric))
}
