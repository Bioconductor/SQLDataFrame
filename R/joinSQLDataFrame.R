.rownamesFun <- function (rx, ry, type)
    switch(type,
           left_join = rx,
           inner_join = intersect(rx, ry),
           semi_join = intersect(rx, ry),
           anti_join = setdiff(rx, ry)
           )

.doCompatibleFunction <- function(x, y, ..., FUN) {
    tbls <- .join_union_prepare(x, y)
    tbl.out <- FUN(tbls[[1]], tbls[[2]], ...)
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@dbconcatKey <- .rownamesFun(ROWNAMES(x), ROWNAMES(y),
                                 deparse(substitute(FUN)))
    x@indexes <- vector("list", 2)
    return(x)
}

#########################
## left_join, inner_join
#########################

left_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    .doCompatibleFunction(x, y, by = by, copy = copy, suffix = suffix,
                          auto_index = auto_index, FUN = left_join)
}

inner_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                    suffix = c(".x", ".y"),
                                    auto_index = FALSE, ...) 
{
    .doCompatibleFunction(x, y, by = by, copy = copy, suffix = suffix,
                          auto_index = auto_index, FUN = inner_join)
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
    .doCompatibleFunction(x, y, by = by, copy = copy, suffix = suffix,
                          auto_index = auto_index, FUN = semi_join)
}


## for "anti_join", the new @tblData$ops is still "op_semi_join"
## see show_query(@tblData), "...WHERE NOT EXISTS..."

anti_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    .doCompatibleFunction(x, y, by = by, copy = copy, suffix = suffix,
                          auto_index = auto_index, FUN = anti_join)
}

