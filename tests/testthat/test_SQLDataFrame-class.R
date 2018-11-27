context("SQLDataFrame-class")

test.db <- system.file("inst/test.db", package = "SQLDataFrame")

test_that("SQLDataFrame constructor works",
{
    ## not valid SQLDataFrame()
    expect_error(SQLDataFrame())

    ## missing "dbtable"
    ## con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    ## DBI::dbWriteTable(con, "mtcars", mtcars)
    ## obj <- SQLDataFrame(dbname = ":memory:", dbkey = "mpg")
    expect_error(SQLDataFrame(dbname = test.db, dbkey = "sampleID"))

    ## non-existing "dbtable"
    expect_error(SQLDataFrame(
        dbname = test.db, dbtable = "random", dbkey = "sampleID"))

    ## incorrect length of "row.names"
    expect_warning(SQLDataFrame(
        dbname = test.db, dbtable = "colData", dbkey = "sampleID",
        row.names = letters))

    ## non-matching "col.names"
    expect_warning(SQLDataFrame(
        dbname = test.db, dbtable = "colData", dbkey = "sampleID",
        col.names = letters))

    ## check slot values / accessors
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID",
        row.names = letters)
    expect_true(validObject(obj))
    exp <- c("dbtable", "dbkey", "dbrownames", "dbnrows", "tblData",
             "indexes")
    expect_identical(exp, slotNames(obj))
    expect_identical(test.db, dbname(obj))
    expect_identical("colDatal", dbtable(obj))
    expect_identical("sampleID", dbkey(obj))
    expect_identical(c(26L, 3L), dim(obj))
    expect_identical(
        list(letters, c("sampleID", "Treatment", "ages")),
        dimnames(obj))
    expect_identical(3L, length(obj))
})

test_that("validity,SQLDataFrame works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")
    expect_error(initialize(obj, indexes = vector("list", 3)))
})

test_that("[[,SQLDataFrame works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")

    exp <- letters
    expect_identical(exp, obj[[1]])

    exp <- rep(c("ChIP", "Input"), 13)
    expect_identical(exp, obj[[2]])

    exp <- obj@tblData %>% pull(ages)
    expect_identical(exp, obj[[3]])
    expect_identical(obj[[3]], obj[["ages"]])

    expect_error(obj[[2:3]], "attempt to extract more than one element")
})

test_that("[,SQLDataFrame works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID",
        row.names = letters)

    obj0 <- obj[]
    expect_true(validObject(obj0))
    expect_identical(obj0, obj)

    ## list_style_subsetting
    obj1 <- obj[1]
    expect_s4_class(obj1, "SQLDataFrame")
    
    ## 1-col subsetting, drop=TRUE by default
    obj2 <- obj[, 1]
    expect_false(is(obj2, "SQLDataFrame"))

    obj2 <- obj[, 1, drop=FALSE]
    expect_identical(obj1, obj2)

    ## multi-col subsetting
    obj3 <- obj[, 1:3]
    expect_identical(obj, obj3)

    obj3 <- obj[, 2:3] ## FIXME: ERROR, cidx does not include dbkey(obj)... 

    ## row&col subsetting
    obj4 <- obj[1:5, c(1,3)]
    expect_s4_class(obj4, "SQLDataFrame")
    expect_identical(dim(obj4), c(5L, 2L))
    expect_identical(rownames(obj4), rownames(obj)[1:5])
    expect_identical(colnames(obj4), colnames(obj)[c(1,3)])
})

test_that("'extractROWS,SQLDataFrame' works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID",
        row.names = letters)
    obj1 <- extractROWS(obj, 1:5)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(obj@tblData, obj1@tblData)
    expect_identical(1:5, obj1@indexes[[1]])
    expect_identical(dim(obj1), c(5L, 3L))
    expect_identical(rownames(obj)[1:5], rownames(obj1))
})

test_that("'.extract_tbl_from_SQLDataFrame' works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")
    obj1 <- obj[1:5, c(1,3)]

    ## exp <- obj@tblData %>% select(-Treatment)
    res <- .extract_tbl_from_SQLDataFrame(obj1)
    expect_true(is(res, "tbl_dbi")) 
})
