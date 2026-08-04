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
    is_special = str_detect(filer_name, regex("\\(special\\)", ignore_case = TRUE)),
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

# Exclude special election transactions from main analysis
state_df <- state_df %>% filter(!is_special)

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
  # Also apply merges to transaction filer names so they match candidate list
  state_df <- state_df %>%
    left_join(name_merges, by = c("filer_name_clean" = "variant_name")) %>%
    mutate(filer_name_clean = coalesce(canonical_name, filer_name_clean)) %>%
    select(-canonical_name)
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

# Add ALL candidates with $0 contributions who aren't already in the data
# (MCEA candidates + TF candidates who only have expenditures)
# This must run BEFORE the filing overlay so candidates with no transactions
# but with filing data get their totals updated.
missing_cands <- candidate_list %>%
  select(filer_name, race, district, party) %>%
  distinct() %>%
  filter(race %in% c("Governor", "Senator", "Representative")) %>%
  filter(!filer_name %in% cand_con_total$candidate) %>%
  mutate(tot_con_per_cand = 0, district = as.numeric(district)) %>%
  rename(candidate = filer_name)

if (nrow(missing_cands) > 0) {
  cand_con_total <- bind_rows(cand_con_total, missing_cands) %>%
    arrange(race, district, -tot_con_per_cand)
  cat(paste0("Added ", nrow(missing_cands), " candidates with $0 contributions\n"))
}

# ── FILING SUMMARY OVERLAY ──
# Filing summaries include unitemized contributions that don't appear as
# individual transactions. Use filing total as baseline, then add any new
# transactions that came in after the filing period ended.
if (file.exists("data/state_2026/filing_summaries.csv")) {
  filing_summaries <- read_csv("data/state_2026/filing_summaries.csv") %>%
    mutate(
      candidate_clean = filer_name %>%
        str_to_lower() %>%
        str_replace_all("[[:punct:]]", "") %>%
        str_squish(),
      filing_end = as.Date(filing_period_end, format = "%m/%d/%Y")
    )

  # Build name mapping: filer URL names -> candidate_list names via filer_url bridge
  # The filer URL list and filing summaries share filer_url; candidate_list has
  # the canonical cleaned names used in cand_con_total. Match within same
  # office+district using last-name overlap for cases where middle names differ.
  if (file.exists("data/state_2026/all_filer_urls_2026.csv")) {
    filer_urls <- read_csv("data/state_2026/all_filer_urls_2026.csv") %>%
      mutate(
        url_name_clean = name %>%
          str_to_lower() %>%
          str_replace_all("[[:punct:]]", "") %>%
          str_squish(),
        race_norm = case_when(
          office == "Senator" ~ "Senator",
          office == "Representative" ~ "Representative",
          office == "Governor" ~ "Governor",
          TRUE ~ office
        ),
        district = as.numeric(district)
      )

    cand_list_for_match <- candidate_list %>%
      filter(race %in% c("Governor", "Senator", "Representative")) %>%
      mutate(district = as.numeric(district)) %>%
      select(filer_name, race, district) %>%
      distinct()

    name_map <- tibble(filing_name = character(), cand_list_name = character())
    for (i in seq_len(nrow(filer_urls))) {
      url_clean <- filer_urls$url_name_clean[i]
      url_race <- filer_urls$race_norm[i]
      url_dist <- filer_urls$district[i]
      if (url_clean %in% cand_con_total$candidate) {
        name_map <- bind_rows(name_map, tibble(filing_name = url_clean, cand_list_name = url_clean))
        next
      }
      same_district <- cand_list_for_match %>%
        filter(race == url_race, district == url_dist | (is.na(district) & is.na(url_dist)))
      if (nrow(same_district) == 0) next
      url_parts <- str_split(url_clean, "\\s+")[[1]]
      for (j in seq_len(nrow(same_district))) {
        cl_name <- same_district$filer_name[j]
        cl_parts <- str_split(cl_name, "\\s+")[[1]]
        if (all(url_parts %in% cl_parts) || all(cl_parts %in% url_parts)) {
          name_map <- bind_rows(name_map, tibble(filing_name = url_clean, cand_list_name = cl_name))
          break
        }
      }
    }
    name_map <- distinct(name_map)
    cat(paste0("Filing name mapping: ", nrow(name_map), " matched\n"))

    filing_summaries <- filing_summaries %>%
      left_join(name_map, by = c("candidate_clean" = "filing_name")) %>%
      mutate(candidate_clean = coalesce(cand_list_name, candidate_clean)) %>%
      select(-cand_list_name)
  }

  # Calculate incremental DIRECT contributions after filing period end
  # (where candidate is the filer — their own page's transactions)
  incremental_direct <- state_df %>%
    filter(transaction_type %in% c("Monetary Contribution", "In-Kind Contribution", "Loan")) %>%
    filter(!str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
    mutate(
      tx_date = as.Date(date, format = "%m/%d/%Y"),
      filer_clean = filer_name_clean %>% str_remove_all("\\s+special$")
    ) %>%
    inner_join(filing_summaries %>% select(candidate_clean, filing_end),
               by = c("filer_clean" = "candidate_clean")) %>%
    filter(tx_date > filing_end) %>%
    group_by(filer_clean) %>%
    summarize(incremental_con = sum(amount_n, na.rm = TRUE))

  # PAC/committee contributions to each candidate (candidate as source_payee)
  # These are NOT included in the candidate's filing summary — they're on the PAC side
  pac_to_candidate <- state_df %>%
    filter(transaction_type == "Monetary Contribution") %>%
    filter(str_detect(filer_name, paste(committee_strings, collapse = "|"))) %>%
    mutate(
      source_payee = source_payee %>%
        str_to_lower() %>%
        str_replace_all("[[:punct:]]", "") %>%
        str_remove_all("\\s+special$") %>%
        str_squish()
    ) %>%
    left_join(candidate_list, by = c("source_payee" = "filer_name")) %>%
    filter(!is.na(race)) %>%
    group_by(source_payee) %>%
    summarize(pac_con = sum(amount_n, na.rm = TRUE))

  # Apply: filing total (direct) + PAC contributions + incremental direct after filing
  skipped <- 0
  applied <- 0
  for (i in seq_len(nrow(filing_summaries))) {
    cand_clean <- filing_summaries$candidate_clean[i]
    filing_total <- filing_summaries$total_contributions[i]
    if (is.na(filing_total)) next
    incr <- incremental_direct %>% filter(filer_clean == cand_clean) %>% pull(incremental_con)
    if (length(incr) == 0) incr <- 0
    pac <- pac_to_candidate %>% filter(source_payee == cand_clean) %>% pull(pac_con)
    if (length(pac) == 0) pac <- 0
    new_total <- filing_total + pac + incr

    if (cand_clean %in% cand_con_total$candidate) {
      old_total <- cand_con_total %>% filter(candidate == cand_clean) %>% pull(tot_con_per_cand)
      cat(paste0("Filing overlay: ", cand_clean,
                 " | old: $", formatC(old_total, format = "f", digits = 2, big.mark = ","),
                 " | filing: $", formatC(filing_total, format = "f", digits = 2, big.mark = ","),
                 " | PAC: $", formatC(pac, format = "f", digits = 2, big.mark = ","),
                 " | incr after ", filing_summaries$filing_period_end[i], ": $",
                 formatC(incr, format = "f", digits = 2, big.mark = ","),
                 " | new: $", formatC(new_total, format = "f", digits = 2, big.mark = ","), "\n"))
      cand_con_total <- cand_con_total %>%
        mutate(tot_con_per_cand = ifelse(candidate == cand_clean, new_total, tot_con_per_cand))
      applied <- applied + 1
    } else {
      skipped <- skipped + 1
    }
  }
  cat(paste0("Filing overlay applied to ", applied, " candidates, skipped ", skipped, " (not in candidate list)\n"))
  cand_con_total <- cand_con_total %>% arrange(race, district, -tot_con_per_cand)
}

# (moved $0 candidate addition above the filing overlay)

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
         entity = source_payee) %>%
  mutate(entity = str_replace_all(entity, " \\. ", " ") %>% str_squish())

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
# Apply different thresholds by race to match footer language
cand_top5_contribs <- all_contribs %>%
  filter(!str_detect(tolower(entity), "contributors giving")) %>%
  filter(!entity %in% c("-", "--", "", NA)) %>%
  left_join(candidate_list %>% select(filer_name, race) %>% distinct(), by = c("candidate" = "filer_name")) %>%
  mutate(race = coalesce(race.y, race.x)) %>%
  select(-race.x, -race.y) %>%
  group_by(candidate) %>%
  arrange(desc(total_contributed), entity) %>%
  mutate(rn = row_number()) %>%
  # Keep if: above threshold, OR in top 5, OR tied with the 5th entry
  filter(
    (race == "Governor" & total_contributed >= 3000) |
    (race %in% c("Senator", "Representative") & total_contributed >= 100) |
    rn <= 5 |
    total_contributed == total_contributed[min(5, n())] |
    is.na(race)
  ) %>%
  select(-rn) %>%
  ungroup() %>%
  arrange(race, district, candidate, desc(total_contributed), entity)


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

# Override with editorially reviewed display names
if (file.exists("data/display_name_overrides.csv")) {
  name_overrides <- read_csv("data/display_name_overrides.csv")
  display_names <- display_names %>%
    left_join(name_overrides, by = c("clean" = "candidate")) %>%
    mutate(display = coalesce(final_display_name, display)) %>%
    select(-final_display_name)
}

# Load overrides for direct application
direct_overrides <- if (file.exists("data/display_name_overrides.csv")) {
  read_csv("data/display_name_overrides.csv") %>%
    rename(override_name = final_display_name)
} else {
  tibble(candidate = character(), override_name = character())
}

# Add display names to outputs (try override first, then display_names lookup, then raw candidate)
cand_con_total <- cand_con_total %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  left_join(direct_overrides, by = "candidate") %>%
  mutate(display_name = coalesce(override_name, display, candidate)) %>%
  mutate(display_name = ifelse(display_name == tolower(display_name), str_to_title(display_name), display_name)) %>%
  select(-display, -override_name)

cand_top5_contribs <- cand_top5_contribs %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  left_join(direct_overrides, by = "candidate") %>%
  mutate(display_name = coalesce(override_name, display, candidate)) %>%
  mutate(display_name = ifelse(display_name == tolower(display_name), str_to_title(display_name), display_name)) %>%
  select(-display, -override_name)

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

# Apply filing summary overlay to expenditures too
if (exists("filing_summaries")) {
  exp_incremental <- state_df %>%
    filter(transaction_type == "Expenditure") %>%
    mutate(
      tx_date = as.Date(date, format = "%m/%d/%Y"),
      filer_clean = filer_name_clean %>% str_remove_all("\\s+special$")
    ) %>%
    inner_join(filing_summaries %>% select(candidate_clean, filing_end, total_expenditures),
               by = c("filer_clean" = "candidate_clean")) %>%
    filter(tx_date > filing_end) %>%
    group_by(filer_clean) %>%
    summarize(incremental_exp = sum(amount_n, na.rm = TRUE))

  for (i in seq_len(nrow(filing_summaries))) {
    cand_clean <- filing_summaries$candidate_clean[i]
    filing_exp <- filing_summaries$total_expenditures[i]
    incr <- exp_incremental %>% filter(filer_clean == cand_clean) %>% pull(incremental_exp)
    if (length(incr) == 0) incr <- 0
    new_exp <- filing_exp + incr

    if (cand_clean %in% cand_exp_total$filer_name) {
      cand_exp_total <- cand_exp_total %>%
        mutate(tot_exp = ifelse(filer_name == cand_clean, new_exp, tot_exp))
    }
  }
  cand_exp_total <- cand_exp_total %>% arrange(race, district, -tot_exp)
}

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
  mutate(entity = str_replace_all(entity, " \\. ", " ") %>% str_squish()) %>%
  group_by(candidate) %>%
  arrange(desc(total_paid), entity) %>%
  filter(total_paid >= 3000 | row_number() <= 5) %>%
  arrange(race, district, candidate, desc(total_paid), entity)

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
  left_join(direct_overrides, by = "candidate") %>%
  mutate(display_name = coalesce(override_name, display, candidate)) %>%
  mutate(display_name = ifelse(display_name == tolower(display_name), str_to_title(display_name), display_name)) %>%
  select(-display, -override_name)

cand_top5_payees <- cand_top5_payees %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  left_join(direct_overrides, by = "candidate") %>%
  mutate(display_name = coalesce(override_name, display, candidate)) %>%
  mutate(display_name = ifelse(display_name == tolower(display_name), str_to_title(display_name), display_name)) %>%
  select(-display, -override_name)

# Add display names + status filter to small_dollar
small_dollar <- small_dollar %>%
  left_join(candidate_status %>% select(candidate, show_on_page), by = "candidate") %>%
  filter(is.na(show_on_page) | show_on_page != "no") %>%
  left_join(display_names, by = c("candidate" = "clean")) %>%
  left_join(direct_overrides, by = "candidate") %>%
  mutate(display_name = coalesce(override_name, display, candidate)) %>%
  mutate(display_name = ifelse(display_name == tolower(display_name), str_to_title(display_name), display_name)) %>%
  select(-display, -override_name)

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



