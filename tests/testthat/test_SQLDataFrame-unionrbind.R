context("rbind SQLDataFrame")

db <- system.file("extdata", "test.db", package = "SQLDataFrame")
db1 <- system.file("extdata", "test1.db", package = "SQLDataFrame")
db2 <- system.file("extdata", "test2.db", package = "SQLDataFrame")
obj <- SQLDataFrame(dbname = db,
                     dbtable = "state",
                     dbkey = c("region", "population"))
obj1 <- SQLDataFrame(dbname = db1,
                     dbtable = "state1",
                     dbkey = c("region", "population"))
obj2 <- SQLDataFrame(dbname = db2,
                     dbtable = "state2",
                     dbkey = c("region", "population"))

obj01 <- obj[1:10, 2:3]
obj02 <- obj[8:15, 2:3]

obj11 <- obj1[8:15,2:3]
obj12 <- obj1[15:18, 2:3]

obj21 <- obj2[15:18, 2:3]

#########
## union
#########

## same source
test_that("union SQLDataFrame with same source works!", {
    u1 <- union(obj01, obj02)
    expect_true(validObject(u1))
    expect_identical(dim(u1), c(15L, 2L))
    expect_null(ridx(u1))
    expect_identical(normalizePath(dirname(dbname(u1))),
                     normalizePath(tempdir()))
    expect_warning(dbtable(u1))
})

## different sources
test_that("union SQLDataFrame with difference source works!", {
    u2 <- union(obj01, obj11)
    expect_true(validObject(u2))
    expect_identical(dim(u2), c(15L, 2L))
    expect_null(ridx(u2))
    expect_identical(normalizePath(dirname(dbname(u2))),
                     normalizePath(tempdir()))
    expect_warning(dbtable(u2))

    u3 <- union(u2, obj21)
    expect_true(validObject(u3))
    expect_identical(dim(u3), c(18L, 2L))
    expect_null(ridx(u3))
    expect_identical(dbname(u2), dbname(u3))

    u4 <- union(obj21, u2)
    expect_identical(as.data.frame(u3), as.data.frame(u4))
    expect_identical(dbname(u3), dbname(u4)) 
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
    expect_identical(normalizePath(dirname(dbname(r1))),
                     normalizePath(tempdir()))
    expect_warning(dbtable(r1))

    r2 <- rbind(r1, obj21)
    expect_true(validObject(r2))
    expect_identical(dim(r2), c(22L, 2L))
    expect_identical(ridx(r2), match(ROWNAMES(r2), dbconcatKey(r2)))
    expect_identical(dbname(r1), dbname(r2))

    r3 <- rbind(obj21, r1)
    expect_identical(dim(r3), dim(r2))
    expect_identical(dbname(r2), dbname(r3)) 

    ## multiple inputs
    r4 <- rbind(obj01, obj11, obj12, obj21)
    expect_identical(dim(r4), c(26L, 2L))
    expect_identical(ridx(r4), match(ROWNAMES(r4), dbconcatKey(r4)))
})


