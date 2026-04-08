library(readr)
library(tidyverse)
library(purrr)
#install.packages("writexl")
library(writexl)
library(stringr)

state_df <- read_csv("data/state_2026/transactions.csv") %>% 
  mutate(amount_n = parse_number(amount))

candidate_list <- read_csv("viz/data/cand_lists/candidate_list_2026_and_2024.csv") %>% 
  unique()


committee_strings <- c("ACTBLUE", "Maine", "Voter", "MAINE", "committee", 
                       "COMMITTEE", "Committee", "Action", "PAC", 
                       "Fight", "Safe", "Fund", "Association", "Democrat", "Livelihood", 
                       "Call", "Dream", "DEMOCRAT", "VOTE", "BQC", "TAX", "Leadership", 
                       "Flag", "Caring", "Future", "House", "Advancing", "Hill", "INC.", 
                       "ACTION")

#obtain the totals donated directly to each candidate
cand_sum <- state_df %>% 
  filter(transaction_type == "Monetary Contribution") %>% 
  group_by(`filer_name`) %>% 
  summarize(tot_con = sum(amount_n, na.rm = T)) %>% 
  #this filters out all the committees/PACs
  filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>% 
  mutate(
    filer_name = filer_name %>%
      str_to_lower() %>%                 # make lowercase
      str_replace_all("[[:punct:]]", "") %>%  # remove punctuation
      str_squish()                      # remove extra spaces
  ) %>% 
  left_join(candidate_list, by = "filer_name") %>% 
  filter(race %in% c("Governor", "Senator", "Representative")) %>% 
  mutate(district = as.numeric(district)) %>% 
  mutate(contributor = "direct")

#obtain the totals donated by committees/PACs 
committee_sum <- state_df %>% 
  filter(transaction_type == "Monetary Contribution") %>% 
  #filter for filers with committees/PACs in them
  filter(str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>% 
  #then group by source_payee and sum 
  group_by(filer_name, source_payee) %>% 
  summarize(tot_con = sum(amount_n, na.rm = T)) %>% 
  ungroup() %>% 
  mutate(
    source_payee = source_payee %>%
      str_to_lower() %>%                 # make lowercase
      str_replace_all("[[:punct:]]", "") %>%  # remove punctuation
      str_squish()                      # remove extra spaces
  ) %>% 
  unique() %>% 
  #merge in the candidate list
  left_join(candidate_list, by = c("source_payee" = "filer_name")) %>% 
  #filter out any rows that didn't match, because these are not candidates (I say with 80% certainty, so we should check) %>% 
  filter(!is.na(race))

committee_sum_for_join <- committee_sum %>% 
  rename("candidate" = "source_payee", "contributor" = "filer_name") 

cand_con_total <- cand_sum %>% 
  rename(candidate = filer_name) %>% 
  rbind(committee_sum_for_join) %>% 
  group_by(candidate, race, district, party) %>% 
  #this sums the totals given directly to the candidates and indirectly to the PAC/committee, who then give to the candidaet
  summarize(tot_con_per_cand = sum(tot_con, na.rm = T)) %>% 
  filter(race %in% c("Governor", "Senator", "Representative")) %>% 
  mutate(district = as.numeric(district)) %>% 
  arrange(race, district, -tot_con_per_cand) 


# export an excel sheet with the total contributions for each canddiates;
# each race and district is a separate sheet
cand_sheets <- cand_con_total %>%
  mutate(
    sheet_name = case_when(
      race == "Governor" ~ "Governor",
      race == "Representative" ~ paste(race, district, sep = "_"),
      race == "Senator" ~ paste(race, district, sep = "_")
    ),
    sheet_name = str_replace_all(sheet_name, " ", "_")
  )

#SOMETHING IS GOING WRONG HERE
sheets <- cand_sheets %>%
  group_by(sheet_name) %>%
  group_split() %>%
  set_names(unique(cand_sheets$sheet_name))

write_xlsx(sheets, "viz/data/clean/maine_races_by_district.xlsx")


#identify top 5 contributors by filer_name (candidate)
cand_contributors <- state_df %>%
  filter(transaction_type == "Monetary Contribution") %>%
  group_by(filer_name, source_payee) %>%
  summarise(total_contributed = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  # filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>% 
  mutate(
    filer_name = filer_name %>%
      str_to_lower() %>%                 # make lowercase
      str_replace_all("[[:punct:]]", "") %>%  # remove punctuation
      str_squish()                      # remove extra spaces
  ) %>% 
  unique() %>% 
  left_join(candidate_list, by = "filer_name") %>% 
  filter(race %in% c("Governor", "Senator", "Representative")) %>% 
  rename(candidate = filer_name, 
         entity = source_payee)

cand_comm_contribs <- state_df %>% 
  filter(transaction_type == "Monetary Contribution") %>% 
  #filter for filers with committees/PACs in them
  filter(str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>% 
  #then group by source_payee and sum 
  group_by(filer_name, source_payee) %>% 
  summarise(total_contributed = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  mutate(
    source_payee = source_payee %>%
      str_to_lower() %>%                 # make lowercase
      str_replace_all("[[:punct:]]", "") %>%  # remove punctuation
      str_squish()                      # remove extra spaces
  ) %>% 
  unique() %>% 
  left_join(candidate_list, by = c("source_payee" = "filer_name")) %>% 
  filter(race %in% c("Governor", "Senator", "Representative")) %>% 
  rename(candidate = source_payee, 
         entity = filer_name)
  

cand_top5_contribs <- cand_contributors %>% 
  rbind(cand_comm_contribs) %>% 
  group_by(candidate) %>%
  slice_max(total_contributed, n = 5, with_ties = FALSE) %>%
  arrange(race, district, candidate, desc(total_contributed)) 


#export datasets
write.csv(cand_con_total, file = "viz/data/clean/maine_total_contributions_by_filer.csv", row.names = FALSE)
write.csv(cand_top5_contribs, file = "viz/data/clean/maine_top_5contributors_by_filer.csv", row.names = FALSE)



