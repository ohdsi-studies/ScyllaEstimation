library(testthat)

cmAnalysisListFile <- system.file("settings",
                                  "cmAnalysisList.json",
                                  package = "ScyllaEstimation")
cmAnalysisList <- CohortMethod::loadCmAnalysisList(cmAnalysisListFile)

test_that("washout", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$createStudyPopArgs$washoutPeriod == 365
    }))),
    3 * 24
  )
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$createStudyPopArgs$washoutPeriod == 0
    }))),
    1 * 24
  )
})

test_that("endDays", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$getDbCohortMethodDataArgs$covariateSettings$endDays == -1
    }))),
    2 * 24
  )
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$getDbCohortMethodDataArgs$covariateSettings$endDays == 0
    }))),
    2 * 24
  )
})

test_that("modelType", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$fitOutcomeModelArgs$modelType == "logistic"
    }))),
    2 * 24
  )
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$fitOutcomeModelArgs$modelType == "cox"
    }))),
    2 * 24
  )
})

test_that("stratified outcome model", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$fitOutcomeModelArgs$stratified == TRUE
    }))),
    2 * 24
  )
})

test_that("create PS", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$createPs == TRUE
    }))),
    3 * 24
  )
})

test_that("match on PS", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$matchOnPs == TRUE
    }))),
    2 * 24
  )
})

test_that("stratify on PS", {
  expect_equal(
    sum(unlist(lapply(cmAnalysisList, function (x) {
      x$stratifyByPs == TRUE
    }))),
    1 * 24
  )
})
