-- Drop old tables if exist

DROP TABLE IF EXISTS attrition;
DROP TABLE IF EXISTS cm_follow_up_dist;
DROP TABLE IF EXISTS cohort_method_analysis;
DROP TABLE IF EXISTS cohort_method_result;
DROP TABLE IF EXISTS comparison_summary;
DROP TABLE IF EXISTS covariate;
DROP TABLE IF EXISTS covariate_analysis;
DROP TABLE IF EXISTS covariate_balance;
DROP TABLE IF EXISTS database;
DROP TABLE IF EXISTS exposure_of_interest;
DROP TABLE IF EXISTS exposure_summary;
DROP TABLE IF EXISTS kaplan_meier_dist;
DROP TABLE IF EXISTS likelihood_profile;
DROP TABLE IF EXISTS negative_control_outcome;
DROP TABLE IF EXISTS outcome_of_interest;
DROP TABLE IF EXISTS preference_score_dist;
DROP TABLE IF EXISTS propensity_model;


-- Create tables

--Table attrition

CREATE TABLE attrition (
			database_id VARCHAR(255) NOT NULL,
			exposure_id INTEGER NOT NULL,
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			outcome_id INTEGER,
			analysis_id INTEGER NOT NULL,
			sequence_number INTEGER NOT NULL,
			description VARCHAR(255) NOT NULL,
			subjects INTEGER NOT NULL,
			PRIMARY KEY(database_id, exposure_id, target_id, comparator_id, outcome_id, analysis_id, sequence_number)
);

--Table cm_follow_up_dist

CREATE TABLE cm_follow_up_dist (
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			outcome_id INTEGER NOT NULL,
			analysis_id INTEGER NOT NULL,
			target_min_days NUMERIC,
			target_p10_days NUMERIC,
			target_p25_days NUMERIC,
			target_median_days NUMERIC,
			target_p75_days NUMERIC,
			target_p90_days NUMERIC,
			target_max_days NUMERIC,
			comparator_min_days NUMERIC,
			comparator_p10_days NUMERIC,
			comparator_p25_days NUMERIC,
			comparator_median_days NUMERIC,
			comparator_p75_days NUMERIC,
			comparator_p90_days NUMERIC,
			comparator_max_days NUMERIC,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(target_id, comparator_id, outcome_id, analysis_id, database_id)
);

--Table cohort_method_analysis

CREATE TABLE cohort_method_analysis (
			analysis_id INTEGER NOT NULL,
			description VARCHAR(255) NOT NULL,
			definition TEXT NOT NULL,
			PRIMARY KEY(analysis_id)
);

--Table cohort_method_result

CREATE TABLE cohort_method_result (
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			outcome_id INTEGER NOT NULL,
			analysis_id INTEGER NOT NULL,
			rr NUMERIC,
			ci_95_lb NUMERIC,
			ci_95_ub NUMERIC,
			p NUMERIC,
			tau NUMERIC,
			log_rr NUMERIC,
			se_log_rr NUMERIC,
			target_subjects INTEGER NOT NULL,
			comparator_subjects INTEGER NOT NULL,
			target_days INTEGER NOT NULL,
			comparator_days INTEGER NOT NULL,
			target_outcomes INTEGER NOT NULL,
			comparator_outcomes INTEGER NOT NULL,
			calibrated_p NUMERIC,
			calibrated_rr NUMERIC,
			calibrated_ci_95_lb NUMERIC,
			calibrated_ci_95_ub NUMERIC,
			calibrated_log_rr NUMERIC,
			calibrated_se_log_rr NUMERIC,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(target_id, comparator_id, outcome_id, analysis_id, database_id)
);

--Table comparison_summary

CREATE TABLE comparison_summary (
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			min_date INTEGER NOT NULL,
			max_date INTEGER NOT NULL,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(target_id, comparator_id, database_id)
);

--Table covariate

CREATE TABLE covariate (
			covariate_id BIGINT NOT NULL,
			covariate_name TEXT NOT NULL,
			covariate_analysis_id INTEGER NOT NULL,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(covariate_id, database_id)
);

--Table covariate_analysis

CREATE TABLE covariate_analysis (
			covariate_analysis_id INTEGER NOT NULL,
			covariate_analysis_name VARCHAR(255) NOT NULL,
			PRIMARY KEY(covariate_analysis_id)
);

--Table covariate_balance

CREATE TABLE covariate_balance (
			database_id VARCHAR(255) NOT NULL,
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			outcome_id INTEGER,
			analysis_id INTEGER NOT NULL,
			covariate_id BIGINT NOT NULL,
			target_mean_before NUMERIC,
			comparator_mean_before NUMERIC,
			std_diff_before NUMERIC,
			target_mean_after NUMERIC,
			comparator_mean_after NUMERIC,
			std_diff_after NUMERIC,
			PRIMARY KEY(database_id, target_id, comparator_id, outcome_id, analysis_id, covariate_id)
);

--Table database

CREATE TABLE database (
			database_id VARCHAR(255) NOT NULL,
			database_name VARCHAR(255) NOT NULL,
			description TEXT NOT NULL,
			vocabularyVersion VARCHAR(255),
			minObsPeriodDate DATE NOT NULL,
			maxObsPeriodDate DATE NOT NULL,
			studyPackageVersion VARCHAR(10) NOT NULL,
			is_meta_analysis INTEGER NOT NULL,
			PRIMARY KEY(database_id)
);

--Table exposure_of_interest

CREATE TABLE exposure_of_interest (
			exposure_id INTEGER NOT NULL,
			exposure_name VARCHAR(255) NOT NULL,
			definition TEXT,
			PRIMARY KEY(exposure_id)
);

--Table exposure_summary

CREATE TABLE exposure_summary (
			exposure_id INTEGER NOT NULL,
			min_date INTEGER NOT NULL,
			max_date INTEGER NOT NULL,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(exposure_id, database_id)
);

--Table kaplan_meier_dist

CREATE TABLE kaplan_meier_dist (
			time NUMERIC NOT NULL,
			target_survival NUMERIC,
			target_survival_lb NUMERIC,
			target_survival_ub NUMERIC,
			comparator_survival NUMERIC,
			comparator_survival_lb NUMERIC,
			comparator_survival_ub NUMERIC,
			target_at_risk INTEGER,
			comparator_at_risk INTEGER,
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			outcome_id INTEGER NOT NULL,
			analysis_id INTEGER NOT NULL,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(time, target_id, comparator_id, outcome_id, analysis_id, database_id)
);

--Table likelihood_profile

CREATE TABLE likelihood_profile (
			profile TEXT NOT NULL,
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			outcome_id INTEGER NOT NULL,
			analysis_id INTEGER NOT NULL,
			database_id VARCHAR(255) NOT NULL,
			PRIMARY KEY(target_id, comparator_id, outcome_id, analysis_id, database_id)
);

--Table negative_control_outcome

CREATE TABLE negative_control_outcome (
			outcome_id INTEGER NOT NULL,
			outcome_name VARCHAR(255) NOT NULL,
			PRIMARY KEY(outcome_id)
);

--Table outcome_of_interest

CREATE TABLE outcome_of_interest (
			outcome_id INTEGER NOT NULL,
			outcome_name VARCHAR(255) NOT NULL,
			definition TEXT,
			PRIMARY KEY(outcome_id)
);

--Table preference_score_dist

CREATE TABLE preference_score_dist (
			database_id VARCHAR(255) NOT NULL,
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			analysis_id INTEGER NOT NULL,
			preference_score NUMERIC NOT NULL,
			target_density NUMERIC NOT NULL,
			comparator_density NUMERIC NOT NULL,
			PRIMARY KEY(database_id, target_id, comparator_id, analysis_id, preference_score)
);

--Table propensity_model

CREATE TABLE propensity_model (
			database_id VARCHAR(255) NOT NULL,
			target_id INTEGER NOT NULL,
			comparator_id INTEGER NOT NULL,
			analysis_id INTEGER NOT NULL,
			covariate_id BIGINT NOT NULL,
			coefficient NUMERIC NOT NULL,
			PRIMARY KEY(database_id, target_id, comparator_id, analysis_id, covariate_id)
);