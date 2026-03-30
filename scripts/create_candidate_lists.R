library(tidyverse)
library(readr)
library(readxl)
library(stringr)
#install.packages("fuzzyjoin")
library(fuzzyjoin)


#list of candidates from 2026
candidate_list_2026 <- read_excel("viz/data/cand_lists/2026 Primary Candidate List posting FINAL.xlsx")

list_2026 <- candidate_list_2026 %>% 
  mutate(name = paste(`First Name`, `Last Name`)) %>% 
  mutate(name = ifelse(is.na(Suffix), name, 
                       paste0(`First Name`," ", `Last Name`, ", ", Suffix))) %>% 
  mutate(race = case_when(Office == "US" ~ "US Senator", 
                          Office == "CG" ~ "US Representative",
                          Office == "GOV" ~ "Governor", 
                          Office == "SS" ~ "Senator", 
                          Office == "SR" ~ "Representative", 
                          Office == "CC" ~ "County Commissioner",
                          Office == "SH" ~ "Sheriff", 
                          Office == "DA" ~ "District Attorney", 
                          Office == "RD" ~ "Register of Deeds", 
                          Office == "CT" ~ "Country Treasurer", 
                          Office == "JP" ~ "Probate Judge", 
                          Office == "RP" ~ "Register of Probate")) %>% 
  filter(race %in% c("Governor", "Senator", "Representative")) %>% 
  mutate(party = case_when(Party == "D" ~ "Democratic", 
                           Party == "R" ~ "Republican")) %>% 
  rename(district = Dist) %>% 
  select(c(filer_name = name), race, district, party) %>% 
  mutate(list = "2026")

#find the list from 2024
candidate_summaries <- read_csv("data/state_2024_2025/candidate_summaries.csv")

candidate_list <- candidate_summaries %>% 
  select(candidate_name = `Candidate Name`, race = `Office Name`, district = District, 
         party = Party, incumbent = Incumbent, finace_type = `Finance Type`, 
         election_year = `Election Year`, id = `ID Number`, jurisdiction = Jurisdiction, 
         gender = Gender) 


list_2024 <- candidate_list %>% 
  filter( jurisdiction == "STATE") %>% 
  select(candidate_name, race, district, party) %>% 
  mutate(filer_name = sub("^([^,]+),\\s*(.+)$", "\\2 \\1", candidate_name)) %>% 
  select(filer_name, race, district, party) %>% 
  mutate(list = "2024")

#nonmatching candidates
candidate_list_nonmatch <-  read_excel("viz/data/cand_lists/2026_candidates_nonmatch.xlsx")

#bind lists from 2024 and 2026 together 
list_26_24 <- list_2026 %>% 
  rbind(list_2024) %>% 
  rbind(candidate_list_nonmatch) %>% 
  unique() 
  

#look at fuzzy matches
df_clean <- list_26_24 %>%
  mutate(
    name_clean = filer_name %>%
      str_to_lower() %>%
      str_replace_all("[[:punct:]]", "") %>%
      str_squish()
  )

fuzzy_matches <- stringdist_inner_join(
  df_clean,
  df_clean,
  by = c("race", "district", "name_clean"),
  method = "jw",      # Jaro-Winkler (good for names)
  max_dist = 0.12,    # smaller = stricter
  distance_col = "dist"
) %>%
  filter(filer_name.x != filer_name.y) %>%
  mutate(pair = paste(pmin(filer_name.x, filer_name.y),
                      pmax(filer_name.x, filer_name.y))) %>%
  distinct(pair, .keep_all = TRUE) %>%
  select(
    filer_name_1 = filer_name.x,
    filer_name_2 = filer_name.y,
    race = race.x,
    district = district.x,
    distance = dist
  ) %>%
  arrange(distance)

#look at names that appear in multiple races
both_chambers <- list_26_24 %>%
  filter(race %in% c("Representative", "Senator")) %>%
  group_by(filer_name) %>%
  summarise(
    races = paste(sort(unique(race)), collapse = ", "),
    n_races = n_distinct(race),
    .groups = "drop"
  ) %>%
  filter(n_races > 1) %>%
  arrange(filer_name)

#I googled these people and made sure we are keeping the correct observation 
keep_senator <- c("Amy Roeder","Elizabeth Caruso","John Andrews","Joshua Morris",
                  "Mana Hared Abdi","Matthew Rush","Michele Meyer", "Kimberly Pomerleau")

keep_rep <- c( "Russell Black","Susan Bernard", "Suzanne Andresen")

keep_gov <- c("James Delmas Libby", "Kenneth A. Capron", "W. Edward Crockett")

remove_names <- c("Corinna Cole", "Kimberly J. Pomerleau", 	
                  "Kenneth A Capron", "Michael D Perkins", "Michael D. Perkins", "Richard G Mason", 
                  "Richard G. Mason")


list_26_24_final <- list_26_24 %>% 
  #filter out the incorrect rows for candidates who have switched races
  filter(
    !(filer_name %in% c(keep_senator, keep_rep, keep_gov)) |
      (filer_name %in% keep_senator & race == "Senator") |
      (filer_name %in% keep_rep & race == "Representative") |
      (filer_name %in% keep_rep & race == "Governor")
  ) %>% 
  filter(!filer_name %in% c(remove_names)) %>% 
  #make all string lower case and remove punctuation and remove white space
  mutate(
    filer_name = filer_name %>%
      str_to_lower() %>%                
      str_replace_all("[[:punct:]]", "") %>% 
      str_squish()                      
  ) %>% 
  select(-list) %>% 
  unique()


write.csv(list_2024, file = "viz/data/cand_lists/candidate_list_2024.csv", row.names = FALSE)
write.csv(list_2026, file = "viz/data/cand_lists/candidate_list_2026.csv", row.names = FALSE)
write.csv(list_26_24_final, file = "viz/data/cand_lists/candidate_list_2026_and_2024.csv", row.names = FALSE)
