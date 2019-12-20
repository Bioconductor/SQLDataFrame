context("join SQLDataFrame")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
con <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db)
obj <- SQLDataFrame(conn = con,
                    dbtable = "state",
                    dbkey = c("region", "population"))

obj1 <- obj[1:10, 1:2]
obj2 <- obj[8:15, 2:3]

test_that("left_join works", {
    aa <- suppressMessages(left_join(obj1, obj2))
    expect_equal(dim(aa), c(10L, 3L))
    expect_equal(colnames(aa), colnames(obj))
    expect_equal(aa$size, c("small", "medium", rep(NA, 6), "large", NA))
    expect_equal(ridx(aa), NULL)
})

test_that("inner_join works", {
    aa <- suppressMessages(inner_join(obj1, obj2))
    expect_equal(dim(aa), c(3L, 3L))
    expect_equal(colnames(aa), colnames(obj))
    expect_equal(ridx(aa), NULL)
})

test_that("semi_join works", {
    aa <- suppressMessages(semi_join(obj1, obj2))
    expect_equal(dim(aa), c(3L, 2L))
    expect_equal(colnames(aa), colnames(obj_sub))
    expect_equal(ridx(aa), NULL)
})

test_that("anti_join works", {
    aa <- suppressMessages(anti_join(obj1, obj2))
    expect_equal(dim(aa), c(7L, 2L))
    expect_equal(colnames(aa), colnames(obj_sub))
    expect_equal(ridx(aa), NULL)
})


