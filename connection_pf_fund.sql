-- !preview conn=con

/*
  Funds & Award Snapshot — MSSQL
  - Limits results to TOP (100)
  - Parameterized by @MinAwardYear
  - Consistent table aliases
*/

SET NOCOUNT ON;

DECLARE @MinAwardYear INT = 2020;  -- change as needed

SELECT TOP (100)
    -- Student
    st.alternate_id                          AS alternate_id,
    st.last_name                             AS last_name,
    st.first_name                            AS first_name,
    st.race                                   AS race_4_aa,
    st.student_token                         AS student_token,

    -- Award / award year
    sa.stu_award_token                       AS stu_award_token,
    ay.award_year_token                      AS award_year_token,
    ay.stu_award_year_token                  AS stu_award_year_token,
    ay.admit_status                          AS bcb_admit_status,

    -- Fund
    sa.fund_ay_token                         AS fund_ay_token,
    sa.status                                AS award_status,
    sa.created_dt                            AS award_created_dt,
    sa.actual_amt                            AS award_actual_amt,
    sa.disbursed_amt                         AS award_disbursed_amt,
    vfms.value_01                            AS category,
    f.fund_short_name                        AS fund_short_name,
    f.fund_long_name                         AS fund_long_name,
    f.fund_type                              AS fund_type,
    f.fund_source                            AS fund_source,

    -- Demographics and FAFSA/IM
    CASE
        WHEN fm.are_you_male = 'Y' THEN 'M'
        WHEN fm.are_you_male = 'N' THEN 'F'
        ELSE NULL
    END                                      AS gender,
    fm.data_valid                            AS fafsa,
    im.cc_tfc                                AS efc,

    -- Custom user fields
    ud.value_103                             AS suitability_rating,
    ud.value_104                             AS scholarship_rating,
    ui.value_06                              AS pc_admit_year,
    ui.value_101                             AS bcm_pqr_x,
    ui.value_102                             AS bcb_pqr_x,
    ui.value_105                             AS x_factor,
    ui.value_106                             AS bcm_pqr_x_2,
    ui.value_107                             AS bcb_pqr_x_2,
    ui.value_142                             AS acad_rating,
    ui.value_194                             AS amount_of_nb_grant,
    ui.value_195                             AS amount_to_appear_on_packaging,
    ui.value_196                             AS amount_of_merit_adjustment,
    ui.value_197                             AS calculated_amount_of_merit_per_pqr,
    ui.value_126                             AS net_partner_scholarship_display,
    ul.value_05                              AS reason_for_adjustment,
    ul.value_07                              AS adm_inst,
    ul.value_08                              AS adm_major,
    us.value_08                              AS colleague_citizenship,
    us.value_09                              AS bcm_app_status,
    us.value_11                              AS start_term,
    us.value_26                              AS bcb_citizenship,
    us.value_28                              AS pc_admit_term,
    us.value_114                             AS scholarship_page,
    ums.value_117                            AS full_tuition_flag
FROM       dbo.stu_award_year              AS ay
INNER JOIN dbo.stu_award                   AS sa   ON ay.stu_award_year_token = sa.stu_award_year_token
INNER JOIN dbo.funds                       AS f    ON f.fund_token = sa.fund_ay_token
INNER JOIN dbo.student                     AS st   ON st.student_token = ay.student_token
INNER JOIN dbo.v_stu_funds_med_strings     AS vfms ON vfms.stu_award_token = sa.stu_award_token
INNER JOIN dbo.user_decimal                AS ud   ON ay.stu_award_year_token = ud.stu_award_year_token
INNER JOIN dbo.user_int                    AS ui   ON ay.stu_award_year_token = ui.stu_award_year_token
INNER JOIN dbo.user_long                   AS ul   ON ay.stu_award_year_token = ul.stu_award_year_token
INNER JOIN dbo.user_mediumstring           AS ums  ON ay.stu_award_year_token = ums.stu_award_year_token
INNER JOIN dbo.say_fm_stu                  AS fm   ON ay.stu_award_year_token = fm.stu_award_year_token
INNER JOIN dbo.say_im_stu                  AS im   ON ay.stu_award_year_token = im.stu_award_year_token
INNER JOIN dbo.user_string                 AS us   ON ay.stu_award_year_token = us.stu_award_year_token
INNER JOIN dbo.v_stu_award_year_totals     AS ayt  ON ay.stu_award_year_token = ayt.stu_award_year_token
WHERE
    ay.award_year_token > @MinAwardYear
ORDER BY
    ay.award_year_token DESC,
    sa.created_dt DESC,
    st.alternate_id ASC,
    f.fund_short_name ASC;
