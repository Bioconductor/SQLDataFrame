context("rbind SQLDataFrame")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
obj <- SQLDataFrame(conn = con,
                    dbtable = "state",
                    dbkey = c("region", "population"))
obj01 <- obj[1:10, 2:3]
obj02 <- obj[8:15, 2:3]

#########
## union
#########

## same source
test_that("union SQLDataFrame with same source works!", {
    u1 <- union(obj01, obj02)
    expect_true(validObject(u1))
    expect_identical(dim(u1), c(15L, 2L))
    expect_null(ridx(u1))
    expect_warning(dbtable(u1))
})

#########
## rbind
#########

## different sources
test_that("rbind SQLDataFrame works!", {
    r1 <- rbind(obj01, obj11)
    expect_true(validObject(r1))
    expect_identical(dim(r1), c(18L, 2L))
    expect_identical(ridx(r1), match(ROWNAMES(r1), dbconcatKey(r1)))
    expect_identical(normalizePath(dirname(connSQLDataFrame(r1)@dbname)),
                     normalizePath(tempdir()))
    expect_warning(dbtable(r1))

    r2 <- rbind(r1, obj21)
    expect_true(validObject(r2))
    expect_identical(dim(r2), c(22L, 2L))
    expect_identical(ridx(r2), match(ROWNAMES(r2), dbconcatKey(r2)))
    expect_identical(connSQLDataFrame(r1)@dbname, connSQLDataFrame(r2)@dbname)

    r3 <- rbind(obj21, r1)
    expect_identical(dim(r3), dim(r2))
    expect_identical(connSQLDataFrame(r2)@dbname, connSQLDataFrame(r3)@dbname) 

    ## multiple inputs
    r4 <- rbind(obj01, obj11, obj12, obj21)
    expect_identical(dim(r4), c(26L, 2L))
    expect_identical(ridx(r4), match(ROWNAMES(r4), dbconcatKey(r4)))
})

test_that("saveSQLDataFrame after rbind preserves row index!", {
    r1 <- rbind(obj01, obj11)
    r11 <- saveSQLDataFrame(r1)
    expect_identical(ridx(r1), ridx(r11))
    expect_identical(as.data.frame(r1), as.data.frame(r11))
})

