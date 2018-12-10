context("SQLDataFrame-class")

test.db <- system.file("inst/extdata/test.db", package = "SQLDataFrame")

test_that("SQLDataFrame constructor argument checking",
{
    ## not valid SQLDataFrame()
    expect_error(SQLDataFrame())

    ## missing "dbtable"
    expect_error(SQLDataFrame(dbname = test.db, dbkey = "sampleID"))

    ## non-existing "dbtable"
    expect_error(SQLDataFrame(
        dbname = test.db, dbtable = "random", dbkey = "sampleID"))

    ## "row.names" does not work
    expect_error(SQLDataFrame(
        dbname = test.db, dbtable = "colData", dbkey = "sampleID",
        row.names = letters), "unused argument")

    ## non-matching "col.names"
    expect_warning(SQLDataFrame(
        dbname = test.db, dbtable = "colData", dbkey = "sampleID",
        col.names = letters))
})


obj <- SQLDataFrame(
    dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")

test_that("SQLDataFrame constructor works",
{
    ## check slot values / accessors
    expect_true(validObject(obj))
    exp <- c("dbtable", "dbkey", "dbnrows", "tblData", "indexes",
             "includeKey")
    expect_identical(exp, slotNames(obj))
    expect_identical(test.db, dbname(obj))
    expect_identical("colDatal", dbtable(obj))
    expect_identical("sampleID", dbkey(obj))
    expect_identical(c(26L, 3L), dim(obj))
    expect_identical(list(NULL, c("sampleID", "Treatment", "ages")),
                     dimnames(obj))
    expect_identical(3L, length(obj))
})

test_that("validity,SQLDataFrame works",
{
    expect_error(initialize(obj, indexes = vector("list", 3)))
})

test_that("[[,SQLDataFrame works",
{
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
    expect_identical(NULL, obj3@indexes[[2]])
    expect_identical(TRUE, obj3@includeKey)
    
    obj3 <- obj[, 2:3]
    expect_identical(dim(obj3), c(26L, 2L))
    expect_identical(NULL, obj3@indexes[[2]])
    expect_identical(FALSE, obj3@includeKey)
    
    ## row&col subsetting
    obj4 <- obj[1:5, 2:3]
    expect_s4_class(obj4, "SQLDataFrame")
    expect_identical(dim(obj4), c(5L, 2L))
    expect_identical(colnames(obj4), colnames(obj)[2:3])
    expect_identical(list(1:5, NULL), obj4@indexes)

    ## out-of-bounds indices
    expect_error(obj[1:100, ],
                 "subscript contains out-of-bounds indices")
    expect_error(obj[, 4:5],
                 "subscript contains out-of-bounds indices")
})

test_that("'extractROWS,SQLDataFrame' works",
{
    obj1 <- extractROWS(obj, 1:5)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(obj@tblData, obj1@tblData)
    expect_identical(1:5, obj1@indexes[[1]])
    expect_identical(dim(obj1), c(5L, 3L))
})

test_that("'.extractCOLS_SQLDataFrame' works",
{
    obj1 <- .extractCOLS_SQLDataFrame(obj, 2:3)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(obj@tblData, obj1@tblData)
    expect_identical(NULL, obj1@indexes[[2]])
    expect_false(obj1@includeKey)
    expect_identical(dim(obj1), c(26L, 2L))
})

test_that("'.extract_tbl_from_SQLDataFrame' works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")
    obj1 <- obj[1:5, c(1,3)]

    res <- .extract_tbl_from_SQLDataFrame(obj1)
    expect_true(is(res, "tbl_dbi")) 
    expect_true(is.na(nrow(res)))
    expect_identical(ncol(res), 2L)
    expect_identical(colnames(res), c("sampleID", "ages"))

    ## always keep key column, @includeKey updates
    obj2 <- obj[, 2:3]
    res <- .extract_tbl_from_SQLDataFrame(obj2)
    expect_identical(ncol(res), 3L)
    expect_identical(colnames(res), c("sampleID", "Treatment", "ages"))
    expect_false(obj2@includeKey)
})

test_that("'.extract_tbl_rows_by_key' works",
{
    obj <- SQLDataFrame(
        dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")
    obj1 <- obj[, 2:3]
    tbl <- .extract_tbl_from_SQLDataFrame(obj1)
    res <- .extract_tbl_rows_by_key(tbl, dbkey(obj), 1:5)
    nrow <- res %>% summarize(n=n()) %>% pull(n)
    expect_identical(nrow, 5L)
    expect_identical(colnames(res), colnames(obj))
})

test_that("'as.data.frame' works",
{
    obj1 <- obj[, 2:3]
    exp <- data.frame(
        Treatment = obj1$Treatment, ages = obj1$ages,
        stringsAsFactors = FALSE)
    expect_identical(exp, as.data.frame(obj1))
})
