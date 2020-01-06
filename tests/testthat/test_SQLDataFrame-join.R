context("join SQLDataFrame")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
con <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
obj <- SQLDataFrame(conn = con,
                    dbtable = "state",
                    dbkey = c("region", "population"))

obj01 <- obj[1:10, 1:2]
obj02 <- obj[8:15, 2:3]

test_that("left_join works", {
    aa <- suppressMessages(left_join(obj01, obj02))
    expect_equal(dim(aa), c(10L, 3L))
    expect_equal(colnames(aa), colnames(obj))
    expect_equal(aa$size[1:7], as.character(rep(NA, 7)))
    expect_equal(ridx(aa), NULL)
})

test_that("inner_join works", {
    aa <- suppressMessages(inner_join(obj01, obj02))
    expect_equal(dim(aa), c(3L, 3L))
    expect_equal(colnames(aa), colnames(obj))
    expect_equal(ridx(aa), NULL)
})

test_that("semi_join works", {
    aa <- suppressMessages(semi_join(obj01, obj02))
    expect_equal(dim(aa), c(3L, 2L))
    expect_equal(colnames(aa), colnames(obj01))
    expect_equal(ridx(aa), NULL)
})

test_that("anti_join works", {
    aa <- suppressMessages(anti_join(obj01, obj02))
    expect_equal(dim(aa), c(7L, 2L))
    expect_equal(colnames(aa), colnames(obj01))
    expect_equal(ridx(aa), NULL)
})


