.doCompatibleFunction <- function(x, y, ..., FUN) {
    ## browser()
    tbls <- .join_union_prepare(x, y)
    tbl.out <- FUN(tbls[[1]], tbls[[2]], ...)
    dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)

    out <- BiocGenerics:::replaceSlots(x, tblData = tbl.out,
                                       dbnrows = dbnrows,
                                       indexes = vector("list", 2))
    return(out)
}

#########################
## left_join, inner_join
#########################

left_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    out <- .doCompatibleFunction(x, y, by = by, copy = copy,
                                 suffix = suffix,
                                 auto_index = auto_index,
                                 FUN = dbplyr:::left_join.tbl_lazy)
    dbrnms <- ROWNAMES(x)
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}

inner_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                    suffix = c(".x", ".y"),
                                    auto_index = FALSE, ...) 
{
    out <- .doCompatibleFunction(x, y, by = by, copy = copy,
                                 suffix = suffix,
                                 auto_index = auto_index,
                                 FUN = dbplyr:::inner_join.tbl_lazy)
    dbrnms <- intersect(ROWNAMES(x), ROWNAMES(y))
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}

#########################
## semi_join, anti_join (filtering joins)
#########################

## for "semi_join", the new @tblData$ops is "op_semi_join".
## see show_query(@tblData), "...WHERE EXISTS..."

semi_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    out <- .doCompatibleFunction(x, y, by = by, copy = copy,
                                 suffix = suffix,
                                 auto_index = auto_index,
                                 FUN = dbplyr:::semi_join.tbl_lazy)
    dbrnms <- intersect(ROWNAMES(x), ROWNAMES(y))
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}


## for "anti_join", the new @tblData$ops is still "op_semi_join"
## see show_query(@tblData), "...WHERE NOT EXISTS..."

anti_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    OUT <- .doCompatibleFunction(x, y, by = by, copy = copy,
                                 suffix = suffix,
                                 auto_index = auto_index,
                                 FUN = dbplyr:::anti_join.tbl_lazy)
    dbrnms <- setdiff(ROWNAMES(x), ROWNAMES(y))
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}

