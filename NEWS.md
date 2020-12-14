ScyllaEstimation 1.0.1
======================

Changes

1. Using newer version of CohortMethod, which always generates likelihood profiles, not only when Cyclops is able to fit a model.

2. Downgrading required version of `nloptr` to 1.2.2.1 to allow execution on R 3.5.1.


ScyllaEstimation 1.0.0
======================

Changes

1. Defining strict data model for results. Now used in database backend.

2. Implemented meta-analysis using non-normal likelihood approximation.


Bugfixes

1. Fixing issues with time-at-risk.

2. Adding missing exposure cohorts.

3. Adding negative control outcome cohorts.

4. Adding missing analysis results.


ScyllaEstimation 0.0.1
======================

Initial develop version
