# Tests the basic functions of a SQLColumnSeed.
# library(testthat); library(SQLDataFrame); source("setup.R"); source("test_SQLColumnSeed.R")

for (tp in c("sqlite", "duckdb", "parquet")) {
    pth <- switch(tp,
                  sqlite = tf,
                  duckdb = tf1,
                  parquet = tf2)
    x <- SQLColumnSeed(pth, dbtype = tp, table = "mtcars", column="wt")
    y <- DelayedArray(x)
    
    test_that("basic methods work as expected for a SQLColumnSeed", {
        expect_s4_class(y, "SQLColumnVector")
        expect_identical(length(y), nrow(example_df))
        expect_identical(type(y), typeof(example_df$wt))
        expect_identical(as.vector(y), example_df$wt)
    })
    
    test_that("extraction methods work as expected for a SQLColumnSeed", {
        keep <- seq(1, length(y), by=2)
        expect_identical(as.vector(extract_array(y, list(keep))), example_df$wt[keep])
        
        ## Not sorted.
        keep <- sample(length(y))
        expect_identical(as.vector(extract_array(y, list(keep))), example_df$wt[keep])
        
        ## duplicates
        keep <- c(1,2,3,1,3,5,2,4,6)
        expect_identical(as.vector(extract_array(y, list(keep))), example_df$wt[keep])
    })
}

test_that("backend-specific ColumnSeed or ColumnVector constructor works", {
    ## SQLite backend
    x1 <- SQLiteColumnSeed(tf, "mtcars", "mpg")
    y1 <- SQLiteColumnVector(x1)
    expect_s4_class(x1, "SQLiteColumnSeed")
    expect_s4_class(y1, "SQLiteColumnVector")
    
    ## DuckDB backend
    x2 <- DuckDBColumnSeed(tf1, "mtcars", "mpg")
    y2 <- DuckDBColumnVector(x2)
    expect_s4_class(x2, "DuckDBColumnSeed")
    expect_s4_class(y2, "DuckDBColumnVector")
    
    ## Parquet backend
    x3 <- ParquetColumnSeed(tf2, "mpg")
    y3 <- ParquetColumnVector(x3)
    expect_s4_class(x3, "ParquetColumnSeed")
    expect_s4_class(y3, "ParquetColumnVector")
})    

