context("rbind SQLDataFrame")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
con <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
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
test_that("rbindUniq SQLDataFrame works!", {
    r1 <- rbindUniq(obj01, obj02)
    expect_true(validObject(r1))
    expect_identical(dim(r1), c(15L, 2L))
    expect_warning(dbtable(r1))
    u1 <- union(obj01, obj02)
    expect_equal(as.data.frame(r1), as.data.frame(u1))

    ## multiple inputs
    obj03 <- obj[14:18, 2:3]
    r2 <- rbindUniq(obj01, obj02, obj03)
    expect_true(validObject(r2))
    expect_identical(dim(r2), c(18L, 2L))
    expect_warning(dbtable(r2))
})

