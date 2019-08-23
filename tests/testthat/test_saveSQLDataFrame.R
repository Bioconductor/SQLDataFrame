context("saveSQLDataFrame")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
conn <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db)
obj <- SQLDataFrame(conn = conn,
                    dbtable = "state",
                    dbkey = c("region", "population"))

test_that("saveSQLDataFrame works!", {
    ## op_base
    expect_message(aa <- saveSQLDataFrame(obj))
    expect_true(validObject(aa))
    expect_s4_class(aa, "SQLDataFrame")
    expect_identical(dim(aa), c(50L, 3L))
    expect_identical(normalizePath(dirname(connSQLDataFrame(aa)@dbname)),
                     normalizePath(tempdir()))
    expect_identical(dbtable(aa), "obj")
    expect_identical(as.data.frame(aa), as.data.frame(obj))
    
    ## op_double
    obj1 <- rbind(obj[1:10, ], obj[8:15, ])
    expect_message(aa <- saveSQLDataFrame(obj1))
    expect_true(validObject(aa))
    expect_s4_class(aa, "SQLDataFrame")
    expect_identical(dim(aa), c(18L, 3L))
    expect_identical(normalizePath(dirname(connSQLDataFrame(aa)@dbname)),
                     normalizePath(tempdir()))
    expect_identical(dbtable(aa), "obj1")
    expect_identical(as.data.frame(aa), as.data.frame(obj1))
    
    ## op_single
    obj1 <- obj %>% mutate(p1 = population/10)
    expect_message(aa <- saveSQLDataFrame(obj1))
    expect_identical(dim(aa), c(50L, 4L))
    expect_identical(normalizePath(dirname(connSQLDataFrame(aa)@dbname)),
                     normalizePath(tempdir()))
    expect_identical(dbtable(aa), "obj1")
    expect_identical(as.data.frame(aa), as.data.frame(obj1))
})
