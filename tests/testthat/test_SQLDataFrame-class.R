context("SQLDataFrame-class")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
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
})


test_that("SQLDataFrame constructor works",
{
    ## check slot values / accessors
    expect_true(validObject(obj))
    exp <- c("tblData", "keyData", "dbkey", "partitionID", "pidRle", "ridx", "dim", "dimnames")
    expect_identical(exp, slotNames(obj))
    expect_identical(normalizePath(test.db), normalizePath(connSQLDataFrame(obj)@dbname))
    expect_identical("colData", dbtable(obj))
    expect_identical("sampleID", dbkey(obj))
    expect_identical(c(26L, 2L), dim(obj))
    expect_identical(list(NULL, c("Treatment", "Ages")),
                     dimnames(obj))
    expect_identical(2L, length(obj))
})

test_that("SQLDataFrame constructor alternative arguments works!", 
{
    obj <- SQLDataFrame(conn = conn, dbtable = "colData",
                        dbkey = "sampleID")
    obj1 <- SQLDataFrame(dbname = test.db, type = "SQLite",
                         dbtable = "colData",
                         dbkey = "sampleID")
    expect_equal(obj, obj1)

    expect_message(SQLDataFrame(conn = conn, dbname = dbname,
                                dbtable = "colData",
                                dbkey = "sampleID"),
                   "These arguments are ignored: dbname")
})

## test_that("validity,SQLDataFrame works",
## {
##     expect_error(initialize(obj, indexes = vector("list", 3)))
##     obj1 <- obj
##     expect_error(dbkey(obj1) <- "Ages")
## })

## utility functions
test_that("'.update_keyData' works",
{
    new_tblData <- tblData(obj) %>% filter(Treatment == "ChIP")
    new_keyData <- .update_keyData(new_tblData, dbkey(obj))
    expect_identical(new_keyData %>% pull(rid), seq(13))
    expect_identical(colnames(new_keyData), c("rid", "sampleID"))
})

test_that("'.update_pidRle' works",
{
    new_pidRle <- .update_pidRle(keyData(obj), partitionID = NULL)
    expect_null(new_pidRle)
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
