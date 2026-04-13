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

# Load MCEA designations (TF = Traditional, MCEA = Maine Clean Election Act)
mcea_data <- read_csv("data/mcea_designations.csv") %>%
  select(candidate_clean, finance_type) %>%
  distinct()

# --- NAME CLEANING ---
# 1. Remove "(Special)" / "special" suffix from candidate names
# 2. Keep a display_name that preserves hyphens (e.g. Majerus-Collins)
# 3. Merge duplicate entries that only differ by "special" or middle name/initial

# Build a display name lookup from the raw filer names (before punctuation removal)
# so we can show proper names like "Majerus-Collins" on charts
state_df <- state_df %>%
  mutate(
    filer_name_clean = filer_name %>%
      str_to_lower() %>%
      str_remove_all("\\s*\\(special\\)\\s*") %>%  # remove (special)
      str_replace_all("[[:punct:]]", "") %>%
      str_squish(),
    # keep a display version with hyphens intact
    filer_name_display = filer_name %>%
      str_to_title() %>%
      str_remove_all("\\s*\\(Special\\)\\s*") %>%
      str_squish()
  )

# Clean candidate list: remove "special" suffix, deduplicate
candidate_list <- candidate_list %>%
  mutate(
    filer_name = filer_name %>%
      str_remove_all("\\s+special$") %>%  # remove trailing "special"
      str_squish()
  ) %>%
  # deduplicate: keep longest name per race+district+party (most complete version)
  group_by(race, district, party) %>%
  arrange(desc(nchar(filer_name))) %>%
  # for each unique candidate, find variants that are substrings of the longest name
  ungroup() %>%
  unique()

# Apply manual name merge mapping (reviewed by a human)
# This file maps variant names to a canonical name
if (file.exists("data/name_merges.csv")) {
  name_merges <- read_csv("data/name_merges.csv")
  candidate_list <- candidate_list %>%
    left_join(name_merges, by = c("filer_name" = "variant_name")) %>%
    mutate(filer_name = coalesce(canonical_name, filer_name)) %>%
    select(-canonical_name) %>%
    distinct()
}


committee_strings <- c("ACTBLUE", "Maine", "Voter", "MAINE", "committee", 
                       "COMMITTEE", "Committee", "Action", "PAC", 
                       "Fight", "Safe", "Fund", "Association", "Democrat", "Livelihood", 
                       "Call", "Dream", "DEMOCRAT", "VOTE", "BQC", "TAX", "Leadership", 
                       "Flag", "Caring", "Future", "House", "Advancing", "Hill", "INC.", 
                       "ACTION")

#obtain the totals donated directly to each candidate
cand_sum <- state_df %>%
  filter(transaction_type == "Monetary Contribution") %>%
  #this filters out all the committees/PACs
  filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
  # use cleaned name (no punctuation, no "special") and aggregate
  mutate(
    filer_name = filer_name_clean %>%
      str_remove_all("\\s+special$")  # merge special election entries
  ) %>%
  group_by(filer_name) %>%
  summarize(tot_con = sum(amount_n, na.rm = T)) %>%
  left_join(candidate_list, by = "filer_name") %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  mutate(district = as.numeric(district)) %>%
  mutate(contributor = "direct")

#obtain the totals donated by committees/PACs
committee_sum <- state_df %>%
  filter(transaction_type == "Monetary Contribution") %>%
  #filter for filers with committees/PACs in them
  filter(str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
  #clean the source_payee name (recipient candidate)
  mutate(
    source_payee = source_payee %>%
      str_to_lower() %>%
      str_replace_all("[[:punct:]]", "") %>%
      str_remove_all("\\s+special$") %>%  # merge special election entries
      str_squish()
  ) %>%
  #then group by source_payee and sum
  group_by(filer_name, source_payee) %>%
  summarize(tot_con = sum(amount_n, na.rm = T)) %>%
  ungroup() %>%
  unique() %>%
  #merge in the candidate list
  left_join(candidate_list, by = c("source_payee" = "filer_name")) %>%
  #filter out any rows that didn't match, because these are not candidates
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
  mutate(
    filer_name = filer_name_clean %>%
      str_remove_all("\\s+special$")
  ) %>%
  group_by(filer_name, source_payee) %>%
  summarise(total_contributed = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  unique() %>%
  left_join(candidate_list, by = "filer_name") %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  rename(candidate = filer_name,
         entity = source_payee)

cand_comm_contribs <- state_df %>%
  filter(transaction_type == "Monetary Contribution") %>%
  #filter for filers with committees/PACs in them
  filter(str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
  mutate(
    source_payee = source_payee %>%
      str_to_lower() %>%
      str_replace_all("[[:punct:]]", "") %>%
      str_remove_all("\\s+special$") %>%
      str_squish()
  ) %>%
  group_by(filer_name, source_payee) %>%
  summarise(total_contributed = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  unique() %>%
  left_join(candidate_list, by = c("source_payee" = "filer_name")) %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  rename(candidate = source_payee,
         entity = filer_name)
  

all_contribs <- cand_contributors %>%
  rbind(cand_comm_contribs)

# Small-dollar totals (contributions of $50 or less) per candidate
small_dollar <- state_df %>%
  filter(transaction_type == "Monetary Contribution") %>%
  filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
  filter(amount_n <= 50 & amount_n > 0) %>%
  mutate(
    filer_name = filer_name_clean %>%
      str_remove_all("\\s+special$")
  ) %>%
  group_by(filer_name) %>%
  summarise(small_dollar_total = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  left_join(candidate_list, by = "filer_name") %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  rename(candidate = filer_name) %>%
  select(candidate, race, district, party, small_dollar_total)

# Top 5 named donors (exclude the aggregate "Contributors giving $50 or less" row)
cand_top5_contribs <- all_contribs %>%
  filter(!str_detect(tolower(entity), "contributors giving")) %>%
  group_by(candidate) %>%
  slice_max(total_contributed, n = 5, with_ties = FALSE) %>%
  arrange(race, district, candidate, desc(total_contributed))


# filter out candidates who lost primary (after June primary, update data/candidate_status.csv)
candidate_status <- read_csv("data/candidate_status.csv")

cand_con_total <- cand_con_total %>%
  left_join(candidate_status %>% select(candidate, show_on_page), by = "candidate") %>%
  filter(is.na(show_on_page) | show_on_page != "no")

cand_top5_contribs <- cand_top5_contribs %>%
  left_join(candidate_status %>% select(candidate, show_on_page), by = "candidate") %>%
  filter(is.na(show_on_page) | show_on_page != "no")

# Build a display name lookup from raw filer names (preserves hyphens, proper casing)
display_names <- state_df %>%
  mutate(
    clean = filer_name_clean %>% str_remove_all("\\s+special$"),
    display = filer_name_display %>% str_remove_all("\\s+Special$")
  ) %>%
  select(clean, display) %>%
  distinct() %>%
  # pick the longest display name per clean name (most complete)
  group_by(clean) %>%
  slice_max(nchar(display), n = 1, with_ties = FALSE) %>%
  ungroup()

# Add display names to outputs
cand_con_total <- cand_con_total %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  mutate(display_name = coalesce(display, candidate)) %>%
  select(-display)

cand_top5_contribs <- cand_top5_contribs %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  mutate(display_name = coalesce(display, candidate)) %>%
  select(-display)

# ── EXPENDITURE ANALYSIS ──
# Total expenditures per candidate
cand_exp_total <- state_df %>%
  filter(transaction_type == "Expenditure") %>%
  filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
  mutate(
    filer_name = filer_name_clean %>%
      str_remove_all("\\s+special$")
  ) %>%
  group_by(filer_name) %>%
  summarize(tot_exp = sum(amount_n, na.rm = T)) %>%
  left_join(candidate_list, by = "filer_name") %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  mutate(district = as.numeric(district)) %>%
  arrange(race, district, -tot_exp)

# Top 5 payees per candidate
cand_top5_payees <- state_df %>%
  filter(transaction_type == "Expenditure") %>%
  filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
  mutate(
    filer_name = filer_name_clean %>%
      str_remove_all("\\s+special$")
  ) %>%
  group_by(filer_name, source_payee) %>%
  summarise(total_paid = sum(amount_n, na.rm = TRUE), .groups = "drop") %>%
  unique() %>%
  left_join(candidate_list, by = "filer_name") %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  rename(candidate = filer_name, entity = source_payee) %>%
  group_by(candidate) %>%
  slice_max(total_paid, n = 5, with_ties = FALSE) %>%
  arrange(race, district, candidate, desc(total_paid))

# Apply candidate status filter
cand_exp_total <- cand_exp_total %>%
  rename(candidate = filer_name) %>%
  left_join(candidate_status %>% select(candidate, show_on_page), by = "candidate") %>%
  filter(is.na(show_on_page) | show_on_page != "no")

cand_top5_payees <- cand_top5_payees %>%
  left_join(candidate_status %>% select(candidate, show_on_page), by = "candidate") %>%
  filter(is.na(show_on_page) | show_on_page != "no")

# Add display names
cand_exp_total <- cand_exp_total %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  mutate(display_name = coalesce(display, candidate)) %>%
  select(-display)

cand_top5_payees <- cand_top5_payees %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  mutate(display_name = coalesce(display, candidate)) %>%
  select(-display)

# Add display names + status filter to small_dollar
small_dollar <- small_dollar %>%
  left_join(candidate_status %>% select(candidate, show_on_page), by = "candidate") %>%
  filter(is.na(show_on_page) | show_on_page != "no") %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  mutate(display_name = coalesce(display, candidate)) %>%
  select(-display)

#export datasets
# Add MCEA/TF designation to all outputs
cand_con_total <- cand_con_total %>%
  left_join(mcea_data, by = c("candidate" = "candidate_clean")) %>%
  mutate(finance_type = coalesce(finance_type, "Unknown"))

cand_top5_contribs <- cand_top5_contribs %>%
  left_join(mcea_data, by = c("candidate" = "candidate_clean")) %>%
  mutate(finance_type = coalesce(finance_type, "Unknown"))

small_dollar <- small_dollar %>%
  left_join(mcea_data, by = c("candidate" = "candidate_clean")) %>%
  mutate(finance_type = coalesce(finance_type, "Unknown"))

cand_exp_total <- cand_exp_total %>%
  left_join(mcea_data, by = c("candidate" = "candidate_clean")) %>%
  mutate(finance_type = coalesce(finance_type, "Unknown"))

cand_top5_payees <- cand_top5_payees %>%
  left_join(mcea_data, by = c("candidate" = "candidate_clean")) %>%
  mutate(finance_type = coalesce(finance_type, "Unknown"))

#export datasets
write.csv(cand_con_total, file = "viz/data/clean/maine_total_contributions_by_filer.csv", row.names = FALSE)
write.csv(cand_top5_contribs, file = "viz/data/clean/maine_top_5contributors_by_filer.csv", row.names = FALSE)
write.csv(small_dollar, file = "viz/data/clean/maine_small_dollar_by_filer.csv", row.names = FALSE)
write.csv(cand_exp_total, file = "viz/data/clean/maine_total_expenditures_by_filer.csv", row.names = FALSE)
write.csv(cand_top5_payees, file = "viz/data/clean/maine_top_5payees_by_filer.csv", row.names = FALSE)



