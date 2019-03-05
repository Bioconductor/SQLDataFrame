## FIXME: should we parse all sql code to construct a new database
## table as "rbind" function?

## "left_join" method now is lazy, only save the lazy tbl with $ops a
## "op_join" object. To further save the object, need to modify the
## "saveSQLDataFrame" function.  "saveSQLDataFrame" for "op_join"
## tblData, will do dbExecute(con, sql) to write a temporary database
## table, and print information about the dbname and dbtable.

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

## to be implement into "saveSQLDataFrame()" function with condition x@tblData$ops "op_join" object, 
.execute_join <- function(x, con, dbtable)
{
    ## attach the 2 tables from join to the new connection.
    sql_query <- dbplyr::db_sql_render(.con_SQLDataFrame(x), x@tblData)
    DBI::dbExecute(con, paste0("ATTACH '", .con_SQLDataFrame(x)@dbname, "' AS aux"))
    ## FIXME: need to add "on.exit(...)" to detach the database when done!!!
    ## set offline?? sqlite attach table as temporary. 
    
    ## following code based on same dbtable for the "op_join" lazy
    ## tbl.
    ## FIXME: need to test for different sources joining!!! 
    sql.paste <- gsub(paste0("FROM `", dbtable(x)[1], "`"),
                      paste0("FROM ", in_schema("aux", dbtable(x)[1])),
                      sql_query)
    sql.submit <- build_sql("CREATE TABLE ", sql(dbtable), " AS ", sql.paste)
    dbExecute(con, sql.submit)
}

inner_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                    suffix = c(".x", ".y"),
                                    auto_index = FALSE, ...) 
{
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- inner_join(x1, y1, by = dbkey(x))
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    return(x)
}

## for "semi_join", the new @tblData$ops is "op_semi_join".
## see show_query(@tblData), "...WHERE EXISTS..."

semi_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- semi_join(x1, y1, by = dbkey(x)) 
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    return(x)
}

## for "anti_join", the new @tblData$ops is still "op_semi_join"
## see show_query(@tblData), "...WHERE NOT EXISTS..."

anti_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   auto_index = FALSE, ...) 
{
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    tbl.out <- anti_join(x1, y1, by = dbkey(x))
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    return(x)
}
