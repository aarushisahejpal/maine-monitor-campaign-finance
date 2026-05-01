library(readr)
library(tidyverse)


# ── RECEIPTS (contributions) ──
fed_df <- read_csv("data/federal_2026/receipts.csv")

# Filter out memo/earmarked items to avoid double-counting
# (e.g. ActBlue earmarks show up as both a memo and actual contribution)
fed_df_no_memo <- fed_df %>%
  filter(is.na(memo_text) | !str_detect(toupper(memo_text), "EARMARKED|MEMO"))

# Official totals come from total_contributions_by_federal_candidate.csv (FEC API)
# so we skip computing cand_sum from Schedule A

cand_top5_contribs <- fed_df_no_memo %>%
  filter(contribution_receipt_amount >= 0) %>%
  group_by(candidate_name, contributor_name, office, district) %>%
  summarize(contribution_amount = sum(contribution_receipt_amount, na.rm = T), .groups = "drop") %>%
  group_by(candidate_name) %>%
  arrange(desc(contribution_amount), contributor_name) %>%
  filter(contribution_amount >= 3000 | row_number() <= 5) %>%
  arrange(candidate_name, desc(contribution_amount), contributor_name) %>%
  ungroup()

# Contributions by source (line_number_label categorizes contributor type)
cand_by_source <- fed_df_no_memo %>%
  filter(contribution_receipt_amount >= 0) %>%
  mutate(
    source_type = case_when(
      str_detect(line_number_label, "(?i)individual") ~ "Individual",
      str_detect(line_number_label, "(?i)party") ~ "Party Committee",
      str_detect(line_number_label, "(?i)other.*committee|PAC") ~ "PAC/Committee",
      str_detect(line_number_label, "(?i)transfer") ~ "Transfers (Own Committees)",
      str_detect(line_number_label, "(?i)candidate") ~ "Candidate/Self",
      TRUE ~ "Other"
    )
  ) %>%
  group_by(candidate_name, office, district, source_type) %>%
  summarize(total = sum(contribution_receipt_amount, na.rm = T), .groups = "drop") %>%
  arrange(candidate_name, -total)

# Contributions by state (in-state vs out-of-state)
cand_by_state <- fed_df_no_memo %>%
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


# Small-dollar totals (contributions of $50 or less) per candidate
small_dollar_federal <- fed_df_no_memo %>%
  filter(contribution_receipt_amount > 0 & contribution_receipt_amount <= 50) %>%
  group_by(candidate_name, office, district, party) %>%
  summarise(small_dollar_total = sum(contribution_receipt_amount, na.rm = TRUE), .groups = "drop")

# ActBlue/WinRed platform totals (from unfiltered data to capture all earmarked donations)
platform_totals <- fed_df %>%
  filter(contribution_receipt_amount > 0) %>%
  filter(str_detect(toupper(contributor_name), "ACTBLUE|WINRED")) %>%
  mutate(platform = case_when(
    str_detect(toupper(contributor_name), "ACTBLUE") ~ "ActBlue",
    str_detect(toupper(contributor_name), "WINRED") ~ "WinRed",
    TRUE ~ contributor_name
  )) %>%
  group_by(candidate_name, office, district, platform) %>%
  summarise(platform_total = sum(contribution_receipt_amount, na.rm = TRUE), .groups = "drop") %>%
  rename(contributor_name = platform)

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
  arrange(desc(total_paid), recipient_name) %>%
  filter(total_paid >= 3000 | row_number() <= 5) %>%
  arrange(candidate_name, desc(total_paid), recipient_name) %>%
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
# Note: total_contributions and total_expenditures CSVs come from official FEC API (Python script)
write.csv(cand_top5_contribs, file = "viz/data/clean/top_5contributors_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_by_source, file = "viz/data/clean/contributions_by_source_federal.csv", row.names = FALSE)
write.csv(cand_by_state, file = "viz/data/clean/contributions_by_state_federal.csv", row.names = FALSE)
write.csv(cand_top5_payees, file = "viz/data/clean/top_5payees_by_federal_candidate.csv", row.names = FALSE)
write.csv(cand_exp_by_purpose, file = "viz/data/clean/expenditures_by_purpose_federal.csv", row.names = FALSE)
write.csv(small_dollar_federal, file = "viz/data/clean/small_dollar_by_federal_candidate.csv", row.names = FALSE)
write.csv(platform_totals, file = "viz/data/clean/platform_totals_federal.csv", row.names = FALSE)
write.csv(small_dollar_federal, file = "viz/data/clean/small_dollar_by_federal_candidate.csv", row.names = FALSE)
