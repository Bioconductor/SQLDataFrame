## "union" method is defined for both S3 and S4 classes. Since
## SQLDataFrame is a S4 object, here I am using "setMethod" instead of
## "union.SQLDataFrame".

## "union" itself removes duplicates, and reorder by default using 1st column, then 2nd, ...
## SQL::union_all, I guess, no automatic ordering...

## setMethod("union", signature = "SQLDataFrame", function(x, y, ...)
## {
##     browser()
##     x1 <- .extract_tbl_from_SQLDataFrame(x)
##     y1 <- .extract_tbl_from_SQLDataFrame(y)
##     tbl.union <- dbplyr:::union.tbl_lazy(x1, y1)
##     x@tblData <- tbl.union
##     x@dbnrows <- tbl.union %>% summarize(n=n()) %>% pull(n)
##     x@indexes <- vector("list", 2)
##     return(x)
## })

## works as "rbind"... Be cautious to save the @tblData, which is
## supposed to be unique rows. So need to modify the @tblData and
## update @indexes. Call "union.tbl_lazy", reverse the automatic
## ordering, and then update @indexes.

## possible strategy:
## 1. use "union", then reverse the ordering.
## 2. use "union_all", then only extract unique lines. "SQL::unique?"

## following function works like "union" without automatic
## ordering. Could rename as just "union", and explain the strategy in
## documentation. Extend to "rbind" function by updating @indexes
## slot.
#' @export

## Since "dbplyr:::union.tbl_lazy(x,y)" only evaluates data from same source, need to rewrite, using the dbExecute(con, "ATTACH dbname AS aux")
setMethod("union", signature = c("SQLDataFrame", "SQLDataFrame"), function(x, y, ...)
{
    ## browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)

    ## make sure x and y are from same source. 
    if (!same_src(x1, y1)) {
        con <- .con_SQLDataFrame(x)
        auxName <- dplyr:::random_table_name()  ## need to record somewhere... 
        dbExecute(con, paste0("ATTACH '", dbname(y), "' AS ", auxName))
        auxSchema <- in_schema(auxName, ident(dbtable(y)))
        tbly <- tbl(con, auxSchema)
        tbly <- .extract_tbl_from_SQLDataFrame_indexes(tbly, y)
        tbly$ops$args <- list(auxSchema)
        y1 <- tbly
    }
    tbl.ua <- dbplyr:::union.tbl_lazy(x1, y1)
    x@tblData <- tbl.ua
    x@dbnrows <- tbl.ua %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    return(x)
})

.rbind_SQLDataFrame <- function(..., deparse.level = 1)
{
    ## browser()
    objects <- list(...)
    ## check consistent dbkey(), colnames(),
    keys <- lapply(objects, dbkey)
    if (length(unique(keys)) != 1)
        stop("Input SQLDataFrame objects must have identical dbkey()!")
    cnms <- lapply(objects, colnames)
    if (length(unique(cnms)) != 1 )
        stop("Input SQLDataFrame objects must have identical columns!")
    dbkey <- keys[[1]]
    cnm <- cnms[[1]]
    rnms_final <- do.call(c, lapply(objects, ROWNAMES))

    ## 1) pairwise "union" with multiple input. 
    out <- union(objects[[1]], objects[[2]])
    objects <- objects[-c(1:2)]
    repeat{
        if(length(objects) == 0) break
        out <- union(out, objects[[1]])
        objects <- objects[-1]
    }
    ## 2) extract the concat key values, match and update to @indexes[[1]].
    keyUnion <- out@tblData %>%
        mutate(concatKey = paste(!!!syms(dbkey), sep="\b")) %>%
        pull(concatKey)
    out@dbconcatKey <- keyUnion

    ## Possible enhancement: save 'idx' as separate database table. 
    idx <- match(rnms_final, keyUnion)
    out@indexes[[1]] <- idx   ## need a slot setter here? so ridx(out) <- idx
    return(out)
}
setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)
