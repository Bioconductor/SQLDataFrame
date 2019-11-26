context("SQLDataFrame-methods")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
conn <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db)
obj <- SQLDataFrame(conn = conn, dbtable = "colData",
                    dbkey = "sampleID")

## methods
test_that("[[,SQLDataFrame works",
{
    ## key values
    exp <- letters
    expect_identical(exp, obj[["sampleID"]])
    expect_identical(exp, obj$sampleID)

    exp <- rep(c("ChIP", "Input"), 13)
    expect_identical(exp, obj[[1]])

    exp <- tblData(obj) %>% pull(Ages)
    expect_identical(exp, obj[[2]])
    expect_identical(obj[[2]], obj[["Ages"]])

    expect_error(obj[[1:2]], "attempt to extract more than one element")
})

test_that("[,SQLDataFrame works",
{
    obj0 <- obj[]
    expect_true(validObject(obj0))
    expect_identical(obj0, obj)

    ## list_style_subsetting
    obj1 <- obj[1]
    expect_s4_class(obj1, "SQLDataFrame")

    obj2 <- obj[c("sampleID", "Treatment")]  ## have key column doesn't affect results.
    expect_identical(obj1, obj2)

    ## 1-col subsetting, drop=TRUE by default
    obj3 <- obj[, 1]
    expect_false(is(obj3, "SQLDataFrame"))

    ## 1-col subsetting with key column, equivalent to 1-col subsetting and drop = FALSE.
    obj3 <- obj[, "Treatment", drop = FALSE]
    obj4 <- obj[, c("sampleID", "Treatment")]
    expect_identical(obj3, obj4) 

    ## multi-col subsetting
    obj5 <- obj[, 1:2]
    expect_identical(as.data.frame(obj), as.data.frame(obj5))
    expect_identical(NULL, ridx(obj5))
    
    ## row&col subsetting
    obj6 <- obj[1:5, 1:2]
    expect_s4_class(obj6, "SQLDataFrame")
    expect_identical(dim(obj6), c(5L, 2L))
    expect_identical(colnames(obj6), colnames(obj))
    expect_identical(NULL, ridx(obj6))

    ## out-of-bounds indices
    expect_error(obj[1:100, ],
                 "subscript contains out-of-bounds indices")
    expect_error(obj[, 4:5],
                 "subscript contains out-of-bounds indices")

    ## non-sequential row index
    rowidx <- c(8,1,6,3,7)
    obj7 <- obj[rowidx, ]
    expect_identical(ridx(obj7), as.integer(rank(rowidx)))

    ## duplicate row index
    rowidx <- c(8,3,2,2,3,7)
    obj8 <- obj[rowidx,]
    expect_identical(ridx(obj8),
                     as.integer(rank(unique(rowidx))
                                [match(rowidx, unique(rowidx))]))

    ## additional subsetting
    obj9 <- obj8[2:3, ]
    exp <- as.data.frame(obj8[2:3, ])
    expect_equal(exp, as.data.frame(obj9))

    obj10 <- obj8[3, ]
    obj11 <- obj8[4, ]
    expect_equal(obj10, obj11)
})

test_that("'extractROWS,SQLDataFrame' works",
{
    obj1 <- extractROWS(obj, 1:5)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(tblData(obj1) %>% summarize(n=n()) %>% pull(n), 5L)
    expect_identical(NULL, ridx(obj1))
    expect_identical(dim(obj1), c(5L, 2L))
})

test_that("'.extractCOLS_SQLDataFrame' works",
{
    obj1 <- .extractCOLS_SQLDataFrame(obj, 1:2)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(as.data.frame(tblData(obj)), as.data.frame(tblData(obj1)))
    expect_identical(dim(obj1), c(26L, 2L))
})

test_that("select.SQLDataFrame works",
{
    obj1 <- obj %>% select(Treatment:Ages)
    expect_s4_class(obj1, "SQLDataFrame")

    expect_identical(as.data.frame(tblData(obj)), as.data.frame(tblData(obj1)))
    expect_identical(NULL, ridx(obj1))
    expect_identical(dim(obj1), c(26L, 2L))

    obj1 <- obj %>% select(Ages)
    expect_s4_class(obj1, "SQLDataFrame")
    expect_identical(colnames(obj1), "Ages")
    expect_identical(dim(obj1), c(26L, 1L))

    obj1 <- obj %>% select("sampleID")  ## key column.
    expect_s4_class(obj1, "SQLDataFrame")
    expect_identical(dim(obj1), c(26L, 0L))
})

test_that("filter.SQLDataFrame works",
{
    obj1 <- obj %>% filter(Treatment == "ChIP")
    expect_identical(dim(obj1), c(13L, 2L))
    expect_equal(ridx(obj1), NULL)
    expect_identical(keyData(obj1) %>% pull(rid), seq(13))
})

test_that("mutate.SQLDataFrame works",
{
    obj1 <- obj %>% mutate(Age1 = ifelse(Ages <= 30, "30th", "Older"))
    expect_identical(dim(obj1), c(26L, 3L))
    expect_null(ridx(obj1))

    obj1 <- obj %>% filter(Treatment == "ChIP") %>%
        mutate(Age1 = ifelse(Ages <= 30, "30th", "Older"))
    expect_identical(dim(obj1), c(13L, 3L))
})
