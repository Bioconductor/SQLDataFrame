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
setMethod("union", signature = c("SQLDataFrame", "SQLDataFrame"), function(x, y, ...)
{
    ## browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)

    ## tbl.ua <- dbplyr:::union_all.tbl_lazy(x1, y1)
    ## tbl.uad <- distinct(tbl.ua)
    ## x@tblData <- tbl.uad
    ## x@dbnrows <- tbl.uad %>% summarize(n=n()) %>% pull(n)

    tbl.ua <- dbplyr:::union.tbl_lazy(x1, y1)
    x@tblData <- tbl.ua
    x@dbnrows <- tbl.ua %>% summarize(n=n()) %>% pull(n)

    x@indexes <- vector("list", 2)  ## debug: 
    return(x)
})

## rbind takes 2 arguments only, like "union", but add additional ridx(). 
.rbind_SQLDataFrame <- function(..., deparse.level = 1)
{
    ## number of input argument? do union iteratively? 
    ## temp <- union(...)
    ## 1) extract the concat key values.

    ## 2) iterative "union" with multiple input. 

    ## 3) match he concat key to union_ed output concatkey. 
    NULL
}
## setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)
