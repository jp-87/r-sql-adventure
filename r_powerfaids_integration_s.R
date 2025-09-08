library(DBI)
library(odbc)
library(dplyr)
library(httr)
library(jsonlite)
library(lubridate)
library(tibble)
library(readr)

# Database connection setup for PowerFAIDS
# Note: This should be configured with actual connection parameters
con <- tryCatch({
  dbConnect(odbc(),
            driver = "SQL Server", # or appropriate driver
            server = Sys.getenv("POWERFAIDS_SERVER", "localhost"),
            database = Sys.getenv("POWERFAIDS_DB", "PowerFAIDS"),
            trusted_connection = "yes")
}, error = function(e) {
  warning("Database connection failed: ", e$message)
  NULL
})


map_it <- function(df) {
  # Helper function to capture structure summary
  get_structure_summary <- function(x) {
    summary_str <- capture.output(str(x))
    paste(summary_str, collapse = " ")
  }
  # Calculate statistics for each column
  results <- sapply(df, function(x) {
    c(
      UniqueCount = length(unique(x)),
      NA_Count = sum(is.na(x) | x == ""),
      StructureSummary = get_structure_summary(x)
    )
  }, simplify = FALSE)
  # Convert results to a dataframe and compute percentage NA
  total_rows <- nrow(df)
  unique_df <- data.frame(
    Column = names(results),
    UniqueCounts = sapply(results, `[[`, "UniqueCount"),
    NA_Counts = sapply(results, `[[`, "NA_Count"),
    StructureSummary = sapply(results, `[[`, "StructureSummary"),
    stringsAsFactors = FALSE
  ) %>%
    mutate(Percent_NA_of_Total_Rows = (as.numeric(NA_Counts) / total_rows) * 100) %>%
    select(Column,
           UniqueCounts,
           NA_Counts,
           Percent_NA_of_Total_Rows,
           StructureSummary)
  return(unique_df)
}
table <- function(...) {
  base::table(..., useNA = "ifany")
}

# Fork function: Creates alternative processing paths for data analysis
fork_analysis <- function(data, fork_type = "both") {
  if (is.null(data)) {
    return(list(financial_aid = NULL, demographic = NULL))
  }
  
  results <- list()
  
  # Financial Aid Analysis Fork
  if (fork_type %in% c("financial", "both")) {
    results$financial_aid <- data %>%
      select(starts_with("fm_"), starts_with("im_"), 
             award_year_token, tot_budget, tot_inst_grants_awd, 
             tot_loans_awd, award_calc) %>%
      filter(!is.na(fm_stu_cc_efc) | !is.na(im_stu_cc_efc))
  }
  
  # Demographic Analysis Fork  
  if (fork_type %in% c("demographic", "both")) {
    results$demographic <- data %>%
      select(alternate_id, student_token, award_year_token,
             fm_par_num_in_family, fm_par_marital_status,
             fm_dependency_status, packaging_status) %>%
      filter(!is.na(alternate_id))
  }
  
  return(results)
}




query_2 <- {
  "
SELECT 
    stu_award_year.award_year_token AS award_year_token,
    stu_award_year.stu_award_year_token AS stu_award_year_token,
    stu_award_year.student_token AS student_token,
    stu_award_year.tracking_status AS tracking_status,
    stu_award_year.version_token AS version_token,
    student.alternate_id AS alternate_id,
    stu_award_year.packaging_status AS packaging_status,
    stu_award_year.date_awd_letter_printed AS date_awd_letter,
    stu_award_year.date_packaged AS date_packaged,
    say_fm_fnar_par.total_inc AS fm_par_total_income,
    say_fm_fnar_par.net_worth AS fm_par_net_worth,
    say_fm_fnar_stu.total_inc AS fm_stu_total_income,
    say_fm_fnar_stu.net_worth AS fm_stu_net_worth, 
    say_fm_fnar_stu.adj_bus_frm_eqty AS fm_stu_adj_business_equity,
    say_fm_par.invest_value AS fm_par_invest_value,
    say_fm_par.num_in_college AS fm_par_num_in_college,
    say_fm_par.num_in_family AS fm_par_num_in_family,
    say_fm_par.marital_status AS fm_par_marital_status,
    say_fm_par.agi AS fm_par_agi,
    say_fm_par.par_1_income AS fm_par_1_income,
    say_fm_par.par_2_income AS fm_par_2_income,
    say_fm_par.untaxed_income_total AS fm_par_untaxed_income_total,
    say_fm_stu.agi AS fm_stu_agi,
    say_fm_stu.application_recvd_dt AS fm_application_recvd_dt,
    say_fm_stu.cc_tfc AS fm_stu_cc_efc,
    say_fm_stu.data_valid AS fm_stu_data_valid,
    say_fm_stu.dependency_status AS fm_dependency_status,
    say_fm_stu.fm_pc AS fm_pc,
    say_fm_stu.fm_sc AS fm_sc,
    say_fm_stu.income AS fm_stu_income,
    say_fm_stu.par_1_highest_grade_level AS fm_par_1_highest_grade_level,
    say_fm_stu.par_2_highest_grade_level AS fm_par_2_highest_grade_level,
    say_fm_stu.untaxed_income_total AS fm_stu_untaxed_income_total,
    say_fm_stu.verif_date AS fm_verif_date,
    say_fm_stu.verif_outcome AS fm_verif_outcome,
    say_fm_stu.verification_selection AS fm_verification_selection,
    say_im_stu.cc_tfc AS im_stu_cc_efc,
    say_im_stu.data_valid AS im_stu_data_valid,
    say_im_stu.agi AS im_stu_agi,
    say_im_stu.income AS im_stu_income,
    say_im_par.num_in_college AS im_par_num_in_college,
    say_im_par.num_in_family AS im_par_num_in_family,
    say_im_par.marital_status AS im_par_marital_status,
    say_im_par.agi AS im_par_agi,
    stu_ay_sum_data.tot_budget AS tot_budget,
    stu_ay_sum_data.tot_inst_grants_awd AS tot_inst_grants_awd,
    stu_ay_sum_data.tot_jobs_awd AS tot_jobs_awd,
    stu_ay_sum_data.tot_loans_awd AS tot_loans_awd,
    stu_ay_sum_data.tot_tuition_fees AS tot_tuition_fees,
    stu_pell_data.award_calc AS award_calc

FROM dbo.stu_award_year stu_award_year
     INNER JOIN dbo.student student ON (student.student_token = stu_award_year.student_token)
     JOIN dbo.say_im_stu say_im_stu ON (stu_award_year.stu_award_year_token = say_im_stu.stu_award_year_token)
     JOIN dbo.say_fm_fnar_par say_fm_fnar_par ON (stu_award_year.stu_award_year_token = say_fm_fnar_par.stu_award_year_token)
     JOIN dbo.say_fm_fnar_stu say_fm_fnar_stu ON (stu_award_year.stu_award_year_token = say_fm_fnar_stu.stu_award_year_token)
     JOIN dbo.say_fm_par say_fm_par ON (stu_award_year.stu_award_year_token = say_fm_par.stu_award_year_token)
     JOIN dbo.say_fm_stu say_fm_stu ON (stu_award_year.stu_award_year_token = say_fm_stu.stu_award_year_token)
     JOIN dbo.stu_ay_sum_data stu_ay_sum_data ON (stu_award_year.stu_award_year_token = stu_ay_sum_data.stu_award_year_token)
     JOIN dbo.stu_pell_data stu_pell_data ON (stu_award_year.stu_award_year_token = stu_pell_data.stu_award_year_token)
     JOIN dbo.say_im_par say_im_par ON (stu_award_year.stu_award_year_token = say_im_par.stu_award_year_token)

WHERE (stu_award_year.award_year_token = 2024)
"
}

# Execute query with error handling
result_f <- if (!is.null(con)) {
  tryCatch({
    dbGetQuery(con, query_2)
  }, error = function(e) {
    warning("Query execution failed: ", e$message)
    NULL
  })
} else {
  warning("No database connection available")
  NULL
}

if (!is.null(result_f)) {
  glimpse(result_f)
  
  result_f <- as.data.frame(result_f)
  dim(result_f)
  
  # Convert  date columns manually
  result_f$date_awd_letter <- as.Date(result_f$date_awd_letter, format = "%Y-%m-%d")
  result_f$date_packaged <- as.Date(result_f$date_packaged, format = "%Y-%m-%d")
  # result_f$application_recvd_dt <- as.Date(result_f$application_recvd_dt, format = "%Y-%m-%d")
  # result_f$verif_date <- as.Date(result_f$verif_date, format = "%Y-%m-%d")
  
  
  View(map_it(result_f))
  
  table(result_f$award_year_token)
  table(result_f$packaging_status)
  
  # Create analysis forks for specialized processing
  analysis_forks <- fork_analysis(result_f, "both")
  
  # Process financial aid fork
  if (!is.null(analysis_forks$financial_aid)) {
    message("Financial Aid Analysis Fork - Processing ", 
            nrow(analysis_forks$financial_aid), " records")
    # Additional financial aid specific analysis could go here
  }
  
  # Process demographic fork  
  if (!is.null(analysis_forks$demographic)) {
    message("Demographic Analysis Fork - Processing ", 
            nrow(analysis_forks$demographic), " records")
    # Additional demographic specific analysis could go here
  }
} else {
  message("Skipping data processing due to database connection issues")
}



# Safely disconnect from database
if (!is.null(con)) {
  dbDisconnect(con)
}
