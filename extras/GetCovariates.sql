-- PBR's script to grab covariates with large difference in prevalance between T and C
SELECT *
FROM (
	SELECT tc.*,
		c1.covariate_id,
		c1.covariate_name,
		tc1.mean AS t_mean,
		tc2.mean AS c_mean
	FROM (
		SELECT tcc1.database_id,
			t1.cohort_id AS t_cohort_id,
			t1.target_name || '-' || t1.subgroup_name AS t_cohort_name,
			c1.cohort_id AS c_cohort_id,
			c1.target_name || '-' || c1.subgroup_name AS c_cohort_name,
			tcc1.cohort_subjects AS t_persons,
			ccc1.cohort_subjects AS c_persons
		FROM scylla.cohort t1
		INNER JOIN scylla.cohort c1
			ON t1.subgroup_id = c1.subgroup_id
				AND t1.cohort_type = 'TwS'
				AND c1.cohort_type = 'TwS'
		INNER JOIN scylla.cohort_count tcc1
			ON t1.cohort_id = tcc1.cohort_id
		INNER JOIN scylla.cohort_count ccc1
			ON c1.cohort_id = ccc1.cohort_id
				AND tcc1.database_id = ccc1.database_id
		WHERE tcc1.cohort_subjects > 140
			AND ccc1.cohort_subjects > 140
			AND t1.cohort_id <> c1.cohort_id
		) tc
	INNER JOIN scylla.covariate_value tc1
		ON tc.t_cohort_id = tc1.cohort_id
			AND tc.database_id = tc1.database_id
			AND tc1.mean > 0.5
	INNER JOIN (
		SELECT *
		FROM scylla.covariate
		WHERE time_window_id IN (1, 2, 3)
		) c1
		ON tc1.covariate_id = c1.covariate_id
	INNER JOIN scylla.covariate_value tc2
		ON tc.c_cohort_id = tc2.cohort_id
			AND tc.database_id = tc2.database_id
			AND tc1.covariate_id = tc2.covariate_id
			AND tc2.mean < 0.5
	WHERE tc1.mean - tc2.mean > 0.5
	) t1
