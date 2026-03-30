library(readr)
library(tidyverse)


fed_df <- read_csv("data/federal_2026/receipts.csv") 


cand_sum <- fed_df %>% 
  filter(contribution_receipt_amount >=0) %>% 
  group_by(candidate_name, office, district) %>% 
  summarize(tot_con = sum(contribution_receipt_amount, na.rm = T)) %>% 
  ungroup() %>% 
  arrange(office, district, -tot_con)


cand_top5_contribs <- fed_df %>% 
  group_by(candidate_name, contributor_name, office, district) %>%
  summarize(contribution_amount = sum(contribution_receipt_amount, na.rm = T)) %>% 
  ungroup() %>% 
  group_by(candidate_name) %>% 
  slice_max(contribution_amount, n = 5, with_ties = FALSE) %>%
  arrange(candidate_name, desc(contribution_amount)) %>% 
  ungroup()


write.csv(cand_sum, file = "viz/data/clean/total_contributions_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_top5_contribs, file = "viz/data/clean/top_5contributors_by_federal_candidate.csv", row.names = FALSE)


