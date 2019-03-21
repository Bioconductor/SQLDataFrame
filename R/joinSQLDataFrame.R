## FIXME: should we parse all sql code to construct a new database
## table as "rbind" function?

## "left_join" method now is lazy, only save the lazy tbl with $ops a
## "op_join" object. To further save the object, need to modify the
## "saveSQLDataFrame" function.  "saveSQLDataFrame" for "op_join"
## tblData, will do dbExecute(con, sql) to write a temporary database
## table, and print information about the dbname and dbtable.

## .join_prepare <- function(x, y) {
##     browser()

##     if (is(x@tblData$ops, "op_double")) {
##         con <- .con_SQLDataFrame(x)
##         x1 <- .extract_tbl_from_SQLDataFrame(x)
##         ## check if the y1$src$con@dbname already attached.
##         dbs <- dbGetQuery(con, "PRAGMA database_list")
##         aux_y <- dbs[match(dbname(y), dbs$file), "name"]
##         if (is.na(aux_y))
##             aux_y <- .attach_database_from_SQLDataFrame(con, y)
##         y1 <- .open_tbl_from_new_connection(con, aux_y, y)
##     } else if (is(y@tblData$ops, "op_double")) {
##         con <- .con_SQLDataFrame(y)
##         y1 <- .extract_tbl_from_SQLDataFrame(y)
##         dbs <- dbGetQuery(con, "PRAGMA database_list")
##         aux_x <- dbs[match(dbname(x), dbs$file), "name"]
##         if (is.na(aux_x)) {
##             aux_x <- .attach_database_from_SQLDataFrame(con, x)
##         }
##         x1 <- .open_tbl_from_new_connection(con, aux_x, x)
##     } else {
##         dbname <- tempfile(fileext = ".db")
##         con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)        
##         ## attach database into the existing connection.
##         aux_x <- .attach_database_from_SQLDataFrame(con, x)
##         x1 <- .open_tbl_from_new_connection(con, aux_x, x)
##         if (same_src(x@tblData, y@tblData)) {
##             aux_y <- aux_x
##         } else {
##             aux_y <- .attach_database_from_SQLDataFrame(con, y)
##         } 
##         y1 <- .open_tbl_from_new_connection(con, aux_y, y)
##     }
##     return(list(x1, y1))
## }

left_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    browser()
    tbls <- .join_union_prepare(x, y)
    tbl.out <- left_join(tbls[[1]], tbls[[2]], by = dbkey(x))
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@dbconcatKey <- ROWNAMES(x)  ## inner_join
    x@indexes <- vector("list", 2)
    return(x)
}

inner_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                    suffix = c(".x", ".y"),
                                    auto_index = FALSE, ...) 
{
    browser()
    tbls <- .join_union_prepare(x, y)
    tbl.out <- inner_join(tbls[[1]], tbls[[2]], by = dbkey(x))
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@dbconcatKey <- intersect(ROWNAMES(x), ROWNAMES(y))  ## inner_join
    x@indexes <- vector("list", 2)
    return(x)
}

## for "semi_join", the new @tblData$ops is "op_semi_join".
## see show_query(@tblData), "...WHERE EXISTS..."

## filtering joins: semi_join, anti_join
semi_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    browser()
    tbls <- .join_union_prepare(x, y)
    tbl.out <- semi_join(tbls[[1]], tbls[[2]], by = dbkey(x))
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@dbconcatKey <- intersect(ROWNAMES(x), ROWNAMES(y))  ## semi_join
    x@indexes <- vector("list", 2)
    return(x)
}

## for "anti_join", the new @tblData$ops is still "op_semi_join"
## see show_query(@tblData), "...WHERE NOT EXISTS..."

anti_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    browser()
    tbls <- .join_union_prepare(x, y)
    tbl.out <- anti_join(tbls[[1]], tbls[[2]], by = dbkey(x))
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@dbconcatKey <- setdiff(ROWNAMES(x), ROWNAMES(y))  ## anti_join
    x@indexes <- vector("list", 2)
    return(x)
}
