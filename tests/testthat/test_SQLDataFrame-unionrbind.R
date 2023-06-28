context("rbind SQLDataFrame")

test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
test.db1 <- system.file("extdata", "test1.db", package = "SQLDataFrame")
test.db2 <- system.file("extdata", "test2.db", package = "SQLDataFrame")
con <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db)
con1 <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db1)
con2 <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db2)

obj <- SQLDataFrame(conn = con,
                    dbtable = "state",
                    dbkey = c("region", "population"))
obj1 <- SQLDataFrame(conn = con1,
                     dbtable = "state1",
                     dbkey = c("region", "population"))
obj2 <- SQLDataFrame(conn = con2,
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
    expect_identical(normalizePath(dbcon(u1)@dbname),
                     normalizePath(dbcon(obj01)@dbname))
    expect_warning(dbtable(u1))
})

## different sources
test_that("union SQLDataFrame with difference source works!", {
    u2 <- union(obj01, obj11)
    expect_true(validObject(u2))
    expect_identical(dim(u2), c(15L, 2L))
    expect_null(ridx(u2))
    expect_identical(normalizePath(dbcon(u2)@dbname),
                     normalizePath(dbcon(obj01)@dbname))
    expect_warning(dbtable(u2))

    u3 <- union(u2, obj21)
    expect_true(validObject(u3))
    expect_identical(dim(u3), c(18L, 2L))
    expect_null(ridx(u3))
    expect_identical(dbcon(u2)@dbname, dbcon(u3)@dbname)

    u4 <- union(obj21, u2)
    expect_identical(as.data.frame(u3), as.data.frame(u4))  ## dbconcatKey sorted and reordered!
    expect_identical(dbcon(u3)@dbname, dbcon(u4)@dbname) 
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
    expect_identical(normalizePath(dbcon(r1)@dbname),
                     normalizePath(dbcon(obj01)@dbname))
    expect_warning(dbtable(r1))

    r2 <- rbind(r1, obj21)
    expect_true(validObject(r2))
    expect_identical(dim(r2), c(22L, 2L))
    expect_identical(ridx(r2), match(ROWNAMES(r2), dbconcatKey(r2)))
    expect_identical(dbcon(r1)@dbname, dbcon(r2)@dbname)
    expect_true(all.equal(as.data.frame(r2),
                          rbind(as.data.frame(r1), as.data.frame(obj21)),
                          check.attributes = FALSE))
    r3 <- rbind(obj21, r1)
    expect_identical(dim(r3), dim(r2))
    expect_identical(dbcon(r2)@dbname, dbcon(r3)@dbname) 
    expect_true(all.equal(as.data.frame(r3),
                          rbind(as.data.frame(obj21), as.data.frame(r1)),
                          check.attributes = FALSE))

    ## multiple inputs
    r4 <- rbind(obj01, obj11, obj12, obj21)
    expect_identical(dim(r4), c(26L, 2L))
    expect_identical(ridx(r4), match(ROWNAMES(r4), dbconcatKey(r4)))
    expect_true(all.equal(as.data.frame(r4),
                          rbind(as.data.frame(obj01),
                                as.data.frame(obj11),
                                as.data.frame(obj12),
                                as.data.frame(obj21)),
                          check.attributes = FALSE))
    
    ## two inputs, both from "lazy_set_op" (e.g., rbind), but with
    ## different connections. Error.
    expect_error(r5 <- rbind(r1, rbind(obj11, obj21)))
    
})

test_that("saveSQLDataFrame after rbind preserves row index!", {
    r1 <- rbind(obj21, obj11)
    r11 <- saveSQLDataFrame(r1, overwrite = TRUE)
    expect_identical(ridx(r1), ridx(r11))
    expect_identical(as.data.frame(r1), as.data.frame(r11))
})

