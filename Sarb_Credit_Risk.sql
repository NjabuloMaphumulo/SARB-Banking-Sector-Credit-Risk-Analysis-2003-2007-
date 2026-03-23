									"NPL Ratio by Loan Type Over Time"
				"Non-performing loans = Sub-Standard + Doubtful + Loss as % of total gross exposure"

SELECT
    period,
    loan_type,
    ROUND(
        (COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0)) * 100.0
        / NULLIF(total, 0), 2
    ) AS npl_ratio_pct,
    COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0) AS npl_r000,
    total AS gross_exposure_r000
FROM credit_risk
WHERE metric = 'gross_end_of_month'
  AND loan_type NOT IN ('offbalance_sheet_items',
                        'interbank_advances_ncds_investments_and_all_otherassets')
ORDER BY period, loan_type;
-----------------------------------------------------------------------------------------------------------------------------------------------

									"Sector-Wide NPL Trend (Monthly)"
					"Aggregate NPLs across all retail loan types — the headline credit stress indicator"

SELECT
    period,
    SUM(COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0)) AS total_npl_r000,
    SUM(total) AS total_gross_exposure_r000,
    ROUND(
        SUM(COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0)) * 100.0
        / NULLIF(SUM(total), 0), 2
    ) AS sector_npl_ratio_pct
FROM credit_risk
WHERE metric = 'gross_end_of_month'
  AND loan_type NOT IN ('offbalance_sheet_items',
                        'interbank_advances_ncds_investments_and_all_otherassets')
GROUP BY period
ORDER BY period;
-----------------------------------------------------------------------------------------------------------------------------------------------

										"Provision Coverage Ratio"
				"Are provisions keeping pace with NPLs? Below 100% signals under-provisioning."

WITH npl AS (
    SELECT
        period,
        SUM(COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0)) AS total_npl_r000
    FROM credit_risk
    WHERE metric = 'gross_end_of_month'
      AND loan_type NOT IN ('offbalance_sheet_items',
                            'interbank_advances_ncds_investments_and_all_otherassets')
    GROUP BY period
),
prov AS (
    SELECT
        period,
        SUM(COALESCE(mortgage_loans, 0) + COALESCE(instalment_sales_and_leases, 0) +
            COALESCE(credit_cards, 0) + COALESCE(other_loans_and_advances, 0) +
            COALESCE(investments_and_other_assets, 0) + COALESCE(off_balance_sheet, 0)
        ) AS total_specific_provisions_r000
    FROM provisions
    WHERE metric = 'closing_balance'
    GROUP BY period
)
SELECT
    n.period,
    n.total_npl_r000,
    p.total_specific_provisions_r000,
    ROUND(p.total_specific_provisions_r000 * 100.0 / NULLIF(n.total_npl_r000, 0), 2) AS provision_coverage_pct
FROM npl n
JOIN prov p ON n.period = p.period
ORDER BY n.period;
-----------------------------------------------------------------------------------------------------------------------------------------------

									"Write-offs vs Recoveries by Loan Type"
						"Credit loss realisation — how much was written off vs recovered each month"

SELECT
    period,
    loan_type,
    COALESCE(standard_or_current, 0) + COALESCE(special_mention, 0) +
    COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0) AS total_written_off_r000
FROM credit_risk
WHERE metric = 'written_off'
UNION ALL
SELECT
    period,
    loan_type,
    COALESCE(standard_or_current, 0) + COALESCE(special_mention, 0) +
    COALESCE(sub_standard, 0) + COALESCE(doubtful, 0) + COALESCE(loss, 0) AS total_recovered_r000
FROM credit_risk
WHERE metric = 'recovered'
ORDER BY period, loan_type;
-----------------------------------------------------------------------------------------------------------------------------------------------


												"Loan Book Growth Rate (YoY)"
							"Annual growth in gross exposure — identifies credit expansion phases"

WITH monthly AS (
    SELECT
        period,
        loan_type,
        total AS gross_exposure_r000
    FROM credit_risk
    WHERE metric = 'gross_end_of_month'
      AND loan_type NOT IN ('offbalance_sheet_items',
                            'interbank_advances_ncds_investments_and_all_otherassets')
),
with_prior AS (
    SELECT
        a.period,
        a.loan_type,
        a.gross_exposure_r000 AS current_exposure,
        b.gross_exposure_r000 AS prior_year_exposure
    FROM monthly a
    LEFT JOIN monthly b
        ON a.loan_type = b.loan_type
        AND DATE(a.period) = DATE(b.period, '+1 year')
)
SELECT
    period,
    loan_type,
    current_exposure,
    prior_year_exposure,
    ROUND((current_exposure - prior_year_exposure) * 100.0
          / NULLIF(prior_year_exposure, 0), 2) AS yoy_growth_pct
FROM with_prior
WHERE prior_year_exposure IS NOT NULL
ORDER BY period, loan_type;

"Query 6 — Sectoral Concentration of Advances"
"Which sectors hold the most credit risk — quarterly"

SELECT
    period,
    ROUND(individuals * 100.0 / NULLIF(total, 0), 2)                      AS individuals_pct,
    ROUND(finance_insurance * 100.0 / NULLIF(total, 0), 2)                 AS finance_insurance_pct,
    ROUND(real_estate_business_services * 100.0 / NULLIF(total, 0), 2)     AS real_estate_pct,
    ROUND(manufacturing * 100.0 / NULLIF(total, 0), 2)                     AS manufacturing_pct,
    ROUND(trade_and_accommodation * 100.0 / NULLIF(total, 0), 2)           AS trade_pct,
    ROUND(agriculture_forestry_fishing * 100.0 / NULLIF(total, 0), 2)      AS agriculture_pct,
    total AS total_advances_r000
FROM sectoral_exposure
WHERE metric = 'distribution_r000'
ORDER BY period;
-----------------------------------------------------------------------------------------------------------------------------------------------


												"Domestic vs Foreign Exposure Trend"

SELECT
    period,
    south_africa                                                                AS domestic_r000,
    (total - south_africa)                                                      AS foreign_r000,
    ROUND(south_africa * 100.0 / NULLIF(total, 0), 2)                          AS domestic_pct,
    ROUND((total - south_africa) * 100.0 / NULLIF(total, 0), 2)                AS foreign_pct,
    total                                                                       AS total_advances_r000
FROM geographic_exposure
WHERE metric = 'distribution_r000'
ORDER BY period;

