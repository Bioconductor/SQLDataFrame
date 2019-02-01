left_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    ## browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- left_join(x1, y1, by = dbkey(x)) ## check identical dbkey()?
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    ## x@dbkey <- remain the same. 
    return(x)
}

inner_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                    suffix = c(".x", ".y"),
                                    auto_index = FALSE, ...) 
{
    ## browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- inner_join(x1, y1, by = dbkey(x)) ## check identical dbkey()?
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    ## x@dbkey <- remain the same. 
    return(x)
}

semi_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    ## browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- semi_join(x1, y1, by = dbkey(x)) ## check identical dbkey()?
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    ## x@dbkey <- remain the same. 
    return(x)
}

anti_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    ## browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- anti_join(x1, y1, by = dbkey(x)) ## check identical dbkey()?
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    ## x@dbkey <- remain the same. 
    return(x)
}
