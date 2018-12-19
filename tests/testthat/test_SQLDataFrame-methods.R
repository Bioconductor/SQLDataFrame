context("SQLDataFrame-methods")

test.db <- system.file("inst/extdata/test.db", package = "SQLDataFrame")
obj <- SQLDataFrame(
    dbname = test.db, dbtable = "colDatal", dbkey = "sampleID")

## methods
test_that("[[,SQLDataFrame works",
{
    ## key values
    exp <- letters
    expect_identical(exp, obj[["sampleID"]])
    expect_identical(exp, obj$sampleID)

    exp <- rep(c("ChIP", "Input"), 13)
    expect_identical(exp, obj[[1]])

    exp <- obj@tblData %>% pull(ages)
    expect_identical(exp, obj[[2]])
    expect_identical(obj[[2]], obj[["ages"]])

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
    obj3 <- obj[, 1:2]
    expect_identical(obj, obj3)
    expect_identical(NULL, obj3@indexes[[2]])
    
    ## row&col subsetting
    obj4 <- obj[1:5, 1:2]
    expect_s4_class(obj4, "SQLDataFrame")
    expect_identical(dim(obj4), c(5L, 2L))
    expect_identical(colnames(obj4), colnames(obj))
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
    expect_identical(dim(obj1), c(5L, 2L))
})

test_that("'.extractCOLS_SQLDataFrame' works",
{
    obj1 <- .extractCOLS_SQLDataFrame(obj, 1:2)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(obj@tblData, obj1@tblData)
    expect_identical(NULL, obj1@indexes[[2]])
    expect_identical(dim(obj1), c(26L, 2L))
})
