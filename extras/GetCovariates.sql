-- PBR's script to grab covariates with large difference in prevalance between T and C

select *
from
(

select tc.*, c1.covariate_id, c1.covariate_name, tc1.mean as t_mean, tc2.mean as c_mean
from
(
select tcc1.database_id, t1.cohort_id as t_cohort_id, t1.target_name || '-' || t1.subgroup_name as t_cohort_name, c1.cohort_id as c_cohort_id, c1.target_name || '-' || c1.subgroup_name as c_cohort_name, tcc1.cohort_subjects as t_persons, ccc1.cohort_subjects as c_persons
from scylla.cohort t1
inner join scylla.cohort c1
on t1.subgroup_id = c1.subgroup_id
and t1.cohort_type = 'TwS'
and c1.cohort_type = 'TwS'
inner join scylla.cohort_count tcc1
on t1.cohort_id = tcc1.cohort_id
inner join scylla.cohort_count ccc1
on c1.cohort_id = ccc1.cohort_id
and tcc1.database_id = ccc1.database_id
where tcc1.cohort_subjects > 140 and ccc1.cohort_subjects > 140
and t1.cohort_id <> c1.cohort_id
) tc
inner join
scylla.covariate_value tc1
on tc.t_cohort_id = tc1.cohort_id
and tc.database_id = tc1.database_id
and tc1.mean > 0.5
inner join
(select * from scylla.covariate where time_window_id in (1,2,3)) c1
on tc1.covariate_id = c1.covariate_id
inner join
scylla.covariate_value tc2
on tc.c_cohort_id = tc2.cohort_id
and tc.database_id = tc2.database_id
and tc1.covariate_id = tc2.covariate_id
and tc2.mean < 0.5
where tc1.mean - tc2.mean > 0.5
) t1
