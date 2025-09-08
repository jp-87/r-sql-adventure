-- !preview conn=con


-- Created: 2024-06-26 10:56:49.317
/*
  Financial Aid Snapshot — MSSQL
  - Limits results to TOP (100)
  - Parameterized by @MinAwardYear
  - Consistent table aliases and grouping comments
*/

SET NOCOUNT ON;

DECLARE @MinAwardYear INT = 2015;  -- change as needed

SELECT TOP (100)
    -- Keys & statuses
    ay.award_year_token                           AS award_year_token,
    ay.stu_award_year_token                       AS stu_award_year_token,
    ay.student_token                              AS student_token,
    ay.tracking_status                            AS tracking_status,
    ay.version_token                              AS version_token,

    -- Student identifiers
    st.alternate_id                               AS alternate_id,
    st.person_uuid                                AS alt_uid,

    -- Packaging / letter metadata
    ay.packaging_status                           AS packaging_status,
    ay.date_awd_letter_printed                    AS date_awd_letter,
    ay.date_packaged                              AS date_packaged,

    -- FAFSA / FM flags
    ay.data_valid                                 AS fafsa_data_valid,

    -- FM - financials (parent & student)
    fm_fnar_par.total_inc                         AS fm_par_total_income,
    fm_fnar_par.net_worth                         AS fm_par_net_worth,
    fm_fnar_stu.total_inc                         AS fm_stu_total_income,
    fm_fnar_stu.net_worth                         AS fm_stu_net_worth,

    -- FM - parent household
    fm_par.num_in_college                         AS fm_par_num_in_college,
    fm_par.num_in_family                          AS fm_par_num_in_family,
    fm_par.marital_status                         AS fm_par_marital_status,
    fm_par.agi                                    AS fm_par_agi,
    fm_par.par_1_income                           AS fm_par_1_income,
    fm_par.par_2_income                           AS fm_par_2_income,
    fm_par.untaxed_income_total                   AS fm_par_untaxed_income_total,

    -- FM - student details
    fm_stu.agi                                    AS fm_stu_agi,
    fm_stu.application_recvd_dt                   AS fm_application_recvd_dt,
    fm_stu.cc_tfc                                 AS fm_stu_cc_efc,
    fm_stu.data_valid                             AS fm_stu_data_valid,
    fm_stu.dependency_status                      AS fm_dependency_status,
    fm_stu.fm_pc                                  AS fm_pc,
    fm_stu.fm_sc                                  AS fm_sc,
    fm_stu.income                                 AS fm_stu_income,
    fm_stu.par_1_highest_grade_level              AS fm_par_1_highest_grade_level,
    fm_stu.par_2_highest_grade_level              AS fm_par_2_highest_grade_level,
    fm_stu.untaxed_income_total                   AS fm_stu_untaxed_income_total,
    fm_stu.verif_date                             AS fm_verif_date,
    fm_stu.verif_outcome                          AS fm_verif_outcome,
    fm_stu.verification_selection                 AS fm_verification_selection,

    -- IM (institutional methodology)
    im_stu.cc_tfc                                 AS im_stu_cc_efc,
    im_stu.data_valid                             AS im_stu_data_valid,
    im_stu.agi                                    AS im_stu_agi,
    im_stu.income                                 AS im_stu_income,
    im_par.num_in_college                         AS im_par_num_in_college,
    im_par.num_in_family                          AS im_par_num_in_family,
    im_par.marital_status                         AS im_par_marital_status,
    im_par.agi                                    AS im_par_agi,

    -- Award year summary totals
    ay_sum.tot_budget                             AS tot_budget,
    ay_sum.tot_inst_grants_awd                    AS tot_inst_grants_awd,
    ay_sum.tot_jobs_awd                           AS tot_jobs_awd,
    ay_sum.tot_loans_awd                          AS tot_loans_awd,
    ay_sum.tot_tuition_fees                       AS tot_tuition_fees,
    ay_sum.tot_inst_funds_awd                     AS tot_inst_funds_awd,
    ay_sum.tot_private_funds_awd                  AS tot_private_funds_awd,
    ay_sum.tot_fed_funds_awd                      AS tot_fed_funds_awd,
    ay_sum.tot_state_funds_awd                    AS tot_state_funds_awd,
    ay_sum.tot_private_grants_awd                 AS tot_private_grants_awd,
    ay_sum.modified_dt                            AS modified_dt,
    ay_sum.tot_awards_marked_need_based           AS tot_awards_marked_need_based,

    -- Pell
    pell.award_calc                               AS award_calc,

    -- Custom user fields
    u_int.value_164                               AS tuition,
    u_int.value_194                               AS need_based_grant_calc,
    u_int.value_126                               AS entering_scholarship_offer,
    u_int.value_169                               AS pqr_2_award,
    u_int.value_168                               AS aa_award,
    u_int.value_165                               AS one_year_approved_appeal,
    u_int.value_116                               AS four_year_approved_appeal,
    u_int.value_176                               AS b2b_grant_calc,
    u_date.value_08                               AS file_complete_date,
    u_date.value_04                               AS file_reviewed_date,
    u_ms.value_10                                 AS powercampus_id,
    u_ms.value_133                                AS need_level,

    -- Need calculations
    fm_cc.original_need                           AS fm_original_need,
    im_cc.original_need                           AS im_original_need
FROM       dbo.stu_award_year        AS ay
INNER JOIN dbo.student               AS st        ON st.student_token = ay.student_token
INNER JOIN dbo.say_im_stu            AS im_stu    ON ay.stu_award_year_token = im_stu.stu_award_year_token
INNER JOIN dbo.say_fm_fnar_par       AS fm_fnar_par
                                                ON ay.stu_award_year_token = fm_fnar_par.stu_award_year_token
INNER JOIN dbo.say_fm_fnar_stu       AS fm_fnar_stu
                                                ON ay.stu_award_year_token = fm_fnar_stu.stu_award_year_token
INNER JOIN dbo.say_fm_par            AS fm_par   ON ay.stu_award_year_token = fm_par.stu_award_year_token
INNER JOIN dbo.say_fm_stu            AS fm_stu   ON ay.stu_award_year_token = fm_stu.stu_award_year_token
INNER JOIN dbo.stu_ay_sum_data       AS ay_sum   ON ay.stu_award_year_token = ay_sum.stu_award_year_token
INNER JOIN dbo.stu_pell_data         AS pell     ON ay.stu_award_year_token = pell.stu_award_year_token
INNER JOIN dbo.say_im_par            AS im_par   ON ay.stu_award_year_token = im_par.stu_award_year_token
INNER JOIN dbo.user_int              AS u_int    ON ay.stu_award_year_token = u_int.stu_award_year_token
INNER JOIN dbo.user_date             AS u_date   ON ay.stu_award_year_token = u_date.stu_award_year_token
INNER JOIN dbo.user_mediumstring     AS u_ms     ON ay.stu_award_year_token = u_ms.stu_award_year_token
INNER JOIN dbo.v_fm_cc               AS fm_cc    ON ay.stu_award_year_token = fm_cc.stu_award_year_token
INNER JOIN dbo.v_im_cc               AS im_cc    ON ay.stu_award_year_token = im_cc.stu_award_year_token
WHERE
    ay.award_year_token > @MinAwardYear
ORDER BY
    ay.award_year_token DESC,
    st.alternate_id ASC;