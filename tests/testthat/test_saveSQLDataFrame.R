context("saveSQLDataFrame")

db <- system.file("extdata", "test.db", package = "SQLDataFrame")
obj <- SQLDataFrame(dbname = db,
                     dbtable = "state",
                     dbkey = c("region", "population"))

test_that("saveSQLDataFrame works!", {
    ## op_base
    expect_message(aa <- saveSQLDataFrame(obj))
    expect_true(validObject(aa))
    expect_s4_class(aa, "SQLDataFrame")
    expect_identical(dim(aa), c(50L, 3L))
    expect_identical(dirname(dbname(aa)), tempdir())
    expect_identical(dbtable(aa), "obj")
    expect_identical(as.data.frame(aa), as.data.frame(obj))
    
    ## op_double
    obj1 <- rbind(obj[1:10, ], obj[8:15, ])
    expect_message(aa <- saveSQLDataFrame(obj1))
    expect_true(validObject(aa))
    expect_s4_class(aa, "SQLDataFrame")
    expect_identical(dim(aa), c(18L, 3L))
    expect_identical(dirname(dbname(aa)), tempdir())
    expect_identical(dbtable(aa), "obj1")
    expect_identical(as.data.frame(aa), as.data.frame(obj1))
    
    ## op_single
    obj1 <- obj %>% mutate(p1 = population/10)
    expect_message(aa <- saveSQLDataFrame(obj1))
    expect_identical(dim(aa), c(50L, 4L))
    expect_identical(dirname(dbname(aa)), tempdir())
    expect_identical(dbtable(aa), "obj1")
    expect_identical(as.data.frame(aa), as.data.frame(obj1))
})
