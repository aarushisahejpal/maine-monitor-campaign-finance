library(readr)
library(tidyverse)

state_df <- read_csv("data/state_2026/transactions.csv") %>% 
  mutate(amount_n = parse_number(amount))

candidate_list <- read_csv("data/candidate_list_2026_and_2024.csv")

#sum contributions by filer_name (candidate)
cand_tot <- state_df %>% 
  filter(transaction_type == "Monetary Contribution") %>% 
  group_by(`filer_name`) %>% 
  summarize(tot_con = sum(amount_n, na.rm = T)) %>% 
  arrange(-tot_con)
  

strings_to_remove <- c("ACTBLUE", "Maine", "Voter", "MAINE", "committee", 
                       "COMMITTEE", "Committee", "Action", "PAC", 
                       "Fight", "Safe", "Fund", "Association", "Democrat", "Livelihood", 
                       "Call", "Dream", "DEMOCRAT", "VOTE", "BQC", "TAX", "Leadership", 
                       "Flag", "Caring", "Future", "House", "Advancing", "Hill")


cand_tot_final <- cand_tot %>% 
  filter(!str_detect(filer_name, paste(strings_to_remove, collapse = "|"))) %>% 
  left_join(candidate_list, by = "filer_name")


#identify top 5 contributors by filer_name (candidate)
top_contributors <- state_df %>%
  filter(transaction_type == "Monetary Contribution") %>%
  group_by(filer_name, source_payee) %>%
  summarise(total_contributed = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  group_by(filer_name) %>%
  slice_max(total_contributed, n = 5, with_ties = FALSE) %>%
  arrange(filer_name, desc(total_contributed)) %>% 
  filter(!str_detect(filer_name, paste(strings_to_remove, collapse = "|"))) %>% 




#export datasets
write.csv(cand_tot, file = "data/clean/total_contributions_by_filer.csv", row.names = FALSE)
write.csv(top_contributors, file = "data/clean/top_5contributors_by_filer.csv", row.names = FALSE)



