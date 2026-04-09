library(readr)
library(tidyverse)


# ── RECEIPTS (contributions) ──
fed_df <- read_csv("data/federal_2026/receipts.csv")

cand_sum <- fed_df %>%
  filter(contribution_receipt_amount >= 0) %>%
  group_by(candidate_name, office, district, party) %>%
  summarize(tot_con = sum(contribution_receipt_amount, na.rm = T), .groups = "drop") %>%
  arrange(office, district, -tot_con)

cand_top5_contribs <- fed_df %>%
  group_by(candidate_name, contributor_name, office, district) %>%
  summarize(contribution_amount = sum(contribution_receipt_amount, na.rm = T), .groups = "drop") %>%
  group_by(candidate_name) %>%
  slice_max(contribution_amount, n = 5, with_ties = FALSE) %>%
  arrange(candidate_name, desc(contribution_amount)) %>%
  ungroup()

# Contributions by source (line_number_label categorizes contributor type)
cand_by_source <- fed_df %>%
  filter(contribution_receipt_amount >= 0) %>%
  mutate(
    source_type = case_when(
      str_detect(line_number_label, "(?i)individual") ~ "Individual",
      str_detect(line_number_label, "(?i)party") ~ "Party Committee",
      str_detect(line_number_label, "(?i)other.*committee|PAC") ~ "PAC/Committee",
      str_detect(line_number_label, "(?i)candidate") ~ "Candidate/Self",
      TRUE ~ "Other"
    )
  ) %>%
  group_by(candidate_name, office, district, source_type) %>%
  summarize(total = sum(contribution_receipt_amount, na.rm = T), .groups = "drop") %>%
  arrange(candidate_name, -total)

# Contributions by state (in-state vs out-of-state)
cand_by_state <- fed_df %>%
  filter(contribution_receipt_amount >= 0) %>%
  mutate(
    geo = case_when(
      contributor_state == "ME" ~ "In State",
      is.na(contributor_state) | contributor_state == "" ~ "Unknown",
      TRUE ~ "Out of State"
    )
  ) %>%
  group_by(candidate_name, office, district, geo) %>%
  summarize(total = sum(contribution_receipt_amount, na.rm = T), .groups = "drop") %>%
  arrange(candidate_name, -total)


# ── EXPENDITURES (disbursements) ──
fed_exp <- read_csv("data/federal_2026/expenditures.csv")

cand_exp_sum <- fed_exp %>%
  filter(disbursement_amount >= 0) %>%
  group_by(candidate_name, office, district, party) %>%
  summarize(tot_exp = sum(disbursement_amount, na.rm = T), .groups = "drop") %>%
  arrange(office, district, -tot_exp)

cand_top5_payees <- fed_exp %>%
  filter(disbursement_amount >= 0) %>%
  group_by(candidate_name, recipient_name, office, district) %>%
  summarize(total_paid = sum(disbursement_amount, na.rm = T), .groups = "drop") %>%
  group_by(candidate_name) %>%
  slice_max(total_paid, n = 5, with_ties = FALSE) %>%
  arrange(candidate_name, desc(total_paid)) %>%
  ungroup()

# Expenditures by purpose category
cand_exp_by_purpose <- fed_exp %>%
  filter(disbursement_amount >= 0) %>%
  mutate(
    purpose = case_when(
      is.na(disbursement_purpose_category) | disbursement_purpose_category == "" ~ "OTHER",
      TRUE ~ toupper(disbursement_purpose_category)
    )
  ) %>%
  group_by(candidate_name, office, district, purpose) %>%
  summarize(total = sum(disbursement_amount, na.rm = T), .groups = "drop") %>%
  arrange(candidate_name, -total)


# ── EXPORT ──
write.csv(cand_sum, file = "viz/data/clean/total_contributions_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_top5_contribs, file = "viz/data/clean/top_5contributors_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_by_source, file = "viz/data/clean/contributions_by_source_federal.csv", row.names = FALSE)
write.csv(cand_by_state, file = "viz/data/clean/contributions_by_state_federal.csv", row.names = FALSE)
write.csv(cand_exp_sum, file = "viz/data/clean/total_expenditures_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_top5_payees, file = "viz/data/clean/top_5payees_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_exp_by_purpose, file = "viz/data/clean/expenditures_by_purpose_federal.csv", row.names = FALSE)
