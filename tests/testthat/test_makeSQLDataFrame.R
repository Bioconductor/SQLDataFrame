context("makeSQLDataFrame")

mtc <- tibble::rownames_to_column(mtcars)

test_that("makeSQLDataFrame works!", {
    ## in memory data
    expect_message(obj <- makeSQLDataFrame(mtc, dbkey = "rowname"))
    expect_true(validObject(obj))
    expect_s4_class(obj, "SQLDataFrame")
    expect_identical(dim(obj), c(32L, 12L))
    expect_identical(normalizePath(dirname(connSQLDataFrame(obj)@dbname)),
                     normalizePath(tempdir()))
    expect_identical(dbtable(obj), "mtc")
    df <- as.data.frame(obj)
    exp <- as.data.frame(mtc)[order(mtc$rowname), ]
    rownames(exp) <- NULL
    expect_identical(df, exp)
    
    ## character input (csv, ... unquoted data)
    filename <- file.path(tempdir(), "mtc.csv")
    write.csv(mtc, file= filename, row.names = FALSE, quote = FALSE)
    expect_message(obj <- makeSQLDataFrame(filename, dbkey = "rowname"))
    expect_true(validObject(obj))
    expect_s4_class(obj, "SQLDataFrame")
    expect_identical(dim(obj), c(32L, 12L))
    expect_identical(normalizePath(dirname(connSQLDataFrame(obj)@dbname)),
                     normalizePath(tempdir()))
    expect_identical(dbtable(obj), "mtc")
    df <- as.data.frame(obj)
    exp <- as.data.frame(mtc)[order(mtc$rowname), ]
    rownames(exp) <- NULL
    expect_equal(df, exp)
})
