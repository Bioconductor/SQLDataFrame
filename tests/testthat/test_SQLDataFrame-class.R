context("SQLDataFrame-class")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
conn <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db)
obj <- SQLDataFrame(conn = conn, dbtable = "colData",
                    dbkey = "sampleID")

test_that("SQLDataFrame constructor argument checking",
{
    ## not valid SQLDataFrame()
    expect_error(SQLDataFrame())

    ## missing "dbtable"
    expect_error(SQLDataFrame(conn = conn, dbkey = "sampleID"))

    ## non-existing "dbtable"
    expect_error(SQLDataFrame(
        conn = conn, dbtable = "random", dbkey = "sampleID"))

    ## "row.names" does not work
    expect_error(SQLDataFrame(
        conn = conn, dbtable = "colData", dbkey = "sampleID",
        row.names = letters), "unused argument")

    ## non-matching "col.names"
    expect_warning(SQLDataFrame(
        conn = conn, dbtable = "colData", dbkey = "sampleID",
        col.names = letters))
})


test_that("SQLDataFrame constructor works",
{
    ## check slot values / accessors
    expect_true(validObject(obj))
    exp <- c("dbkey", "dbnrows", "tblData", "indexes", "dbconcatKey")
    expect_identical(exp, slotNames(obj))
    expect_identical(normalizePath(test.db), normalizePath(connSQLDataFrame(obj)@dbname))
    expect_identical("colData", dbtable(obj))
    expect_identical("sampleID", dbkey(obj))
    expect_identical(c(26L, 2L), dim(obj))
    expect_identical(list(NULL, c("Treatment", "Ages")),
                     dimnames(obj))
    expect_identical(2L, length(obj))
})

test_that("validity,SQLDataFrame works",
{
    expect_error(initialize(obj, indexes = vector("list", 3)))
    obj1 <- obj
    expect_error(dbkey(obj1) <- "Ages")
})

## utility functions
test_that("'.extract_tbl_from_SQLDataFrame' works",
{
    obj1 <- obj[1:5, 2, drop=FALSE]

    res <- .extract_tbl_from_SQLDataFrame(obj1)
    expect_true(is(res, "tbl_dbi")) 
    expect_true(is.na(nrow(res)))
    expect_identical(ncol(res), 2L)
    expect_identical(colnames(res), c("sampleID", "Ages"))

    ## always keep key column in tblData
    obj2 <- obj[, 1, drop=FALSE]
    res <- .extract_tbl_from_SQLDataFrame(obj2)
    expect_identical(ncol(res), 2L)
    expect_identical(colnames(res), c("sampleID", "Treatment"))
})

test_that("'.extract_tbl_rows_by_key' works",
{
    obj1 <- obj[, 2, drop=FALSE]
    tbl <- .extract_tbl_from_SQLDataFrame(obj1)
    res <- .extract_tbl_rows_by_key(tbl, dbkey(obj), dbconcatKey(obj), 1:5)
    nrow <- res %>% summarize(n=n()) %>% pull(n)
    expect_identical(nrow, 5L)
    expect_identical(colnames(res), c(dbkey(obj), colnames(obj)[2]))
})

## coercion
test_that("'as.data.frame' works",
{
    obj1 <- obj[, 2, drop=FALSE]
    exp <- data.frame(sampleID = letters, Ages = obj1$Ages,
                      stringsAsFactors = FALSE)
    expect_identical(exp, as.data.frame(obj1))
})

test_that("coercion to SQLDataFrame works",
{
    expect_error(as(letters, "SQLDataFrame"))

    aa <- list(a=letters, b=LETTERS)
    expect_message(obj <- as(aa, "SQLDataFrame"))
    expect_true(validObject(obj))
    expect_s4_class(obj, "SQLDataFrame")
    expect_identical(dim(obj), c(26L, 1L))
    expect_identical(dbtable(obj), "from") ## FIXME: use "aa"? 
    expect_identical(dbkey(obj), "a")
    
    aa <- list(letters, LETTERS)
    expect_error(as(aa, "SQLDataFrame"))

    da <- DelayedArray::DelayedArray(array(1:26, c(13,2)))
    expect_message(obj <- as(da, "SQLDataFrame"))
    expect_s4_class(obj, "SQLDataFrame")
    expect_identical(dim(obj), c(13L, 1L))
    expect_identical(dbtable(obj), "from") ## FIXME: use "da"? 
    expect_identical(dbkey(obj), "V1")
})
