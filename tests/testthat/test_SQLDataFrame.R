# Tests the basic functions of a SQLiteDataFrame.
# library(testthat); library(SQLiteDataFrame); source("setup.R"); source("test-SQLiteDataFrame.R")

for (tp in c("sqlite", "duckdb", "parquet")) {
    pth <- switch(tp,
                  sqlite = tf,
                  duckdb = tf1,
                  parquet = tf2)
    x <- SQLDataFrame(pth, dbtype = tp, "mtcars")

    test_that("backend-specific constructor works", {
        if (tp == "sqlite") { 
            x1 <- SQLiteDataFrame(pth, "mtcars")
        } else if (tp == "duckdb") { 
            x1 <- DuckDBDataFrame(pth, "mtcars")
        } else if (tp == "parquet") {
            x1 <- ParquetDataFrame(pth)
        }
        expect_s4_class(x1, "SQLDataFrame")
        expect_identical(x, x1)
    })
    
    test_that("basic methods work for a SQLDataFrame", {
        expect_identical(ncol(x), ncol(example_df))
        expect_identical(nrow(x), nrow(example_df))
        expect_null(rownames(x))
        expect_identical(colnames(x), colnames(example_df))
        
        unnamed <- example_df
        rownames(unnamed) <- NULL
        expect_identical(as.data.frame(x), unnamed)
    })
    
    test_that("renaming collapses to an ordinary DFrame", {
        copy <- x
        replacements <- sprintf("COL%i", seq_len(ncol(x)))
        colnames(copy) <- replacements
        expect_s4_class(copy, "DFrame")
        expect_identical(colnames(copy), replacements)
        
        copy <- x
        replacements <- sprintf("COL%i", seq_len(nrow(x)))
        rownames(copy) <- replacements
        expect_s4_class(copy, "DFrame")
        expect_identical(rownames(copy), replacements)
        
        ## Unless the new names are the same as the old names.
        copy <- x
        rownames(copy) <- NULL
        expect_s4_class(copy, "SQLDataFrame")

        copy <- x
        colnames(copy) <- colnames(x)
        expect_s4_class(copy, "SQLDataFrame")
    })
    
    test_that("slicing by columns preserves type of a SQLDataFrame", {
        copy <- x[,1:2]
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), colnames(example_df)[1:2])
        expect_identical(ncol(copy), 2L)
        unnamed <- example_df[,1:2]
        rownames(unnamed) <- NULL
        expect_identical(as.data.frame(copy), unnamed)
        
        cn <- colnames(x)[c(4,2,3)]
        copy <- x[,cn]
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), cn)
        expect_identical(ncol(copy), 3L)
        
        keep <- startsWith(colnames(x), "d")
        copy <- x[,keep]
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), colnames(example_df)[keep])
        expect_identical(ncol(copy), sum(keep))
        
        copy <- x[,5,drop=FALSE]
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), colnames(example_df)[5])
        
        ## Respects mcols.
        x2 <- x
        mcols(x2) <- DataFrame(whee=seq_len(ncol(x)))
        copy <- x2[,3:1]
        expect_identical(mcols(copy)$whee, 3:1)
    })
    
    test_that("extraction of a column yields a SQLColumnVector", {
        col <- x[,5]
        expect_s4_class(col, "SQLColumnVector")
        expect_identical(as.vector(col), example_df[,5])
        
        nm <- colnames(example_df)[5]
        col <- x[[nm]]
        expect_s4_class(col, "SQLColumnVector")
        expect_identical(as.vector(col), example_df[[nm]])
    })
    
    test_that("slicing by rows collapses to an ordinary DFrame", {
        i <- sample(nrow(x))
        copy <- x[i,]
        expect_s4_class(copy, "DFrame")
        expect_identical(colnames(copy), colnames(example_df))
        expect_identical(as.vector(copy[[1]]), example_df[[1]][i])
    })
    
    test_that("subset assignment usually collapses to an ordinary DFrame", {
        copy <- x
        copy[,c(1,2,3)] <- copy[,c(4,5,6)]
        expect_s4_class(copy, "DFrame")
        expect_s4_class(copy[[1]], "SQLColumnVector")
        ref <- example_df
        ref[,c(1,2,3)] <- ref[,c(4,5,6)]
        rownames(ref) <- NULL
        expect_identical(as.data.frame(copy), ref)

        copy <- x
        copy[1:5,] <- copy[9:13,]
        expect_s4_class(copy, "DFrame")
        expect_s4_class(copy[[1]], "DelayedArray")
        ref <- example_df
        ref[1:5,] <- ref[9:13,]
        rownames(ref) <- NULL
        expect_identical(as.data.frame(copy), ref)
        
        copy <- x
        copy[,"foobar"] <- runif(nrow(x))
        expect_s4_class(copy, "DFrame")
        expect_identical(colnames(copy), c(colnames(x), "foobar"))
        
        copy <- x
        copy[[1]] <- copy[[3]]
        expect_s4_class(copy, "DFrame")
        expect_s4_class(copy[[1]], "SQLColumnVector")
        ref <- example_df
        ref[[1]] <- ref[[3]]
        rownames(ref) <- NULL
        expect_identical(as.data.frame(copy), ref)
        
        copy <- x
        copy$some_random_thing <- runif(nrow(x))
        expect_s4_class(copy, "DFrame")
        expect_identical(colnames(copy), c(colnames(x), "some_random_thing"))
    })
    
    test_that("no-op subset assignment returns a SQLDataFrame", {
        copy <- x
        copy[,1] <- copy[,1]
        expect_s4_class(copy, "SQLDataFrame")
        
        copy <- x
        copy[,colnames(x)[2]] <- copy[,colnames(x)[2],drop=FALSE]
        expect_s4_class(copy, "SQLDataFrame")
        
        copy <- x
        copy[[3]] <- copy[[3]]
        expect_s4_class(copy, "SQLDataFrame")
    })
    
    test_that("rbinding collapses to an ordinary DFrame", {
        copy <- rbind(x, x)
        expect_s4_class(copy, "DataFrame")
        expect_s4_class(copy[[1]], "DelayedArray")
        
        ref <- rbind(example_df, example_df)
        rownames(ref) <- NULL
        expect_identical(as.data.frame(copy), ref)
    })
    
    test_that("cbinding may or may not collapse to an ordinary DFrame", {
        ## Same path, we get another PDF.
        copy <- cbind(x, x)
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), rep(colnames(x), 2))

        ## Different DataFrame class causes collapse.
        copy <- cbind(x, example_df)
        expect_s4_class(copy, "DFrame")
        expect_identical(colnames(copy), rep(colnames(example_df), 2))

        ## We can try combining them with xxColumnVectors.
        copy <- cbind(x, foo=x[["carb"]])
        expect_s4_class(copy, "DFrame")
        expect_identical(colnames(copy), c(colnames(example_df), "foo"))
        
        copy <- cbind(x, carb=x[["carb"]]) # if the name is the same, we preserve the SDF type.
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), c(colnames(example_df), "carb"))

        copy <- cbind(carb=x[["carb"]], x) # works in the other direction, too.
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(colnames(copy), c("carb", colnames(example_df)))
        
        ## Different paths causes collapse.
        if (tp == "sqlite") { ## duckdb cannot be copied/linked with contents
            tmp <- tempfile()
            file.link(pth, tmp)
            x2 <- SQLDataFrame(tmp, tp, table="mtcars")
            copy <- cbind(x, x2)  ## cbind SDF with different path
            expect_s4_class(copy, "DFrame")

            copy <- cbind(x, carb=x2[["carb"]]) # cbind SDF with SCV with different path
            expect_s4_class(copy, "DFrame")
            expect_identical(colnames(copy), c(colnames(example_df), "carb"))
        }
    })

    test_that("cbinding carries forward any metadata", {
        x1 <- x
        mcols(x1) <- DataFrame(whee="A")
        x2 <- x
        mcols(x2) <- DataFrame(whee="B")
        
        copy <- cbind(x1, x2)
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(mcols(copy)$whee, rep(c("A", "B"), each=ncol(x)))
        
        mcols(x1) <- NULL
        copy <- cbind(x1, x2)
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(mcols(copy)$whee, rep(c(NA, "B"), each=ncol(x)))
        
        metadata(x1) <- list(a="YAY")
        metadata(x2) <- list(a="whee")
        copy <- cbind(x1, x2)
        expect_s4_class(copy, "SQLDataFrame")
        expect_identical(metadata(copy), list(a="YAY", a="whee"))
    })
    
    test_that("as.data.frame works with duplicated columns", {
        duplicates <- c(1,1,2,2,3,4,3,5)
        copy <- x[,duplicates]
        unnamed <- example_df[,duplicates]
        rownames(unnamed) <- NULL
        expect_identical(as.data.frame(copy), unnamed)
    })
}
