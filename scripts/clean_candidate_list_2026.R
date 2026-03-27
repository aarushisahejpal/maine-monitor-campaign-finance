library(tidyverse)
library(readr)
library(readxl)

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
  select(c(filer_name = name), race, district, party)

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
  select(filer_name, race, district, party) 

#nonmatching candidates
candidate_list_nonmatch <-  read_excel("viz/data/cand_lists/2026_candidates_nonmatch.xlsx")

#bind lists from 2024 and 2026 otgether 
list_26_24 <- list_2026 %>% 
  rbind(list_2024) %>% 
  rbind(candidate_list_nonmatch) %>% 
  unique()



write.csv(list_2024, file = "viz/data/cand_lists/candidate_list_2024.csv", row.names = FALSE)
write.csv(list_2026, file = "viz/data/cand_lists/candidate_list_2026.csv", row.names = FALSE)
write.csv(list_26_24, file = "viz/data/cand_lists/candidate_list_2026_and_2024.csv", row.names = FALSE)
