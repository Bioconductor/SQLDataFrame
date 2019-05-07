context("join SQLDataFrame")

db <- system.file("extdata", "test.db", package = "SQLDataFrame")
db1 <- system.file("extdata", "test1.db", package = "SQLDataFrame")
obj <- SQLDataFrame(dbname = db,
                     dbtable = "state",
                     dbkey = c("region", "population"))
obj1 <- SQLDataFrame(dbname = db1,
                     dbtable = "state1",
                     dbkey = c("region", "population"))

obj_sub <- obj[1:10, 1:2]
obj1_sub <- obj1[8:15, 2:3]

obj_sub1 <- obj[c(1:2, 1:10, 9:10), 1:2]
obj1_sub1 <- obj1[c(7:10, 8:15), 2:3]

test_that("left_join works", {
    aa <- suppressMessages(left_join(obj_sub, obj1_sub))
    expect_equal(dim(aa), c(10L, 3L))
    expect_equal(colnames(aa), colnames(obj))
    expect_equal(aa$size, c(rep(NA, 7), "small", "large", "medium"))
    expect_equal(ridx(aa), NULL)

    ## join with duplicate rows
    aa <- suppressMessages(left_join(obj_sub1, obj1_sub1))
    expect_equal(dim(aa), c(14L, 3L))
    expect_equal(ridx(aa), ridx(obj_sub1))
    expect_identical(aa[1:2, ], aa[3:4, ])
    expect_identical(aa[11:12, ], aa[13:14, ])
})

test_that("inner_join works", {
    aa <- suppressMessages(inner_join(obj_sub, obj1_sub))
    expect_equal(dim(aa), c(3L, 3L))
    expect_equal(colnames(aa), colnames(obj))
    expect_equal(ridx(aa), NULL)

    ## join with duplicate rows  ## intersect? more
    aa <- suppressMessages(inner_join(obj_sub1, obj1_sub1))
    expect_equal(dim(aa), c(6L, 3L))
    expect_equal(ridx(aa), c(1L, 2L, rep(c(3L, 4L), 2)))
    expect_identical(aa[3:4, ], aa[5:6, ])
})

test_that("semi_join works", {
    aa <- suppressMessages(semi_join(obj_sub, obj1_sub))
    expect_equal(dim(aa), c(3L, 2L))
    expect_equal(colnames(aa), colnames(obj_sub))
    expect_equal(ridx(aa), NULL)

    ## join with duplicate rows
    aa <- suppressMessages(semi_join(obj_sub1, obj1_sub1))
    expect_equal(dim(aa), c(6L, 2L))
    expect_equal(colnames(aa), colnames(obj_sub1))
    expect_equal(ridx(aa), c(1L, 2L, rep(c(3L, 4L), 2)))
    expect_identical(aa[3:4, ], aa[5:6, ])
})

test_that("anti_join works", {
    aa <- suppressMessages(anti_join(obj_sub, obj1_sub))
    expect_equal(dim(aa), c(7L, 2L))
    expect_equal(colnames(aa), colnames(obj_sub))
    expect_equal(ridx(aa), NULL)

    ## join with duplicate rows
    aa <- suppressMessages(anti_join(obj_sub1, obj1_sub1))
    expect_equal(dim(aa), c(8L, 2L))
    expect_equal(colnames(aa), colnames(obj_sub1))
    expect_equal(ridx(aa), c(rep(c(1L, 2L), 2), 3L, 4L, 5L, 6L))
    expect_identical(aa[1:2, ], aa[3:4, ])
})


