#' Save SQLDataFrame object as a new database table.
#' @rdname saveSQLDataFrame
#' @description The function to save \code{SQLDataFrame} object as a
#'     database table with a supplied path to database.
#' @param x The \code{SQLDataFrame} object to be saved.
#' @param dbname A character string of the file path of to be saved
#'     database file.
#' @param dbtable A character string for the to be saved database
#'     table name. Default is the name of the input
#'     \code{SQLDataFrame}.
#' @param overwrite Whether to overwrite the \code{dbtable} if already
#'     exists. Default is FALSE.
#' @param ... other parameters passed to methods.
#' @import DBI
#' @import dbplyr
#' @rawNamespace import(dplyr, except = c("first", "rename",
#'     "setequal", "setdiff", "intersect", "union", "ident", "sql"))
#' @examples
#' dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
#' ss <- SQLDataFrame(dbname = dbname, dbtable = "state", dbkey = "state")
#' ss1 <- ss[1:10, 2:3]
#' ss1 <- saveSQLDataFrame(ss1, dbtable = "ss_subset")
#' dbname(ss1)
#' dbtable(ss1)
#' @export

saveSQLDataFrame <- function(x, dbname = tempfile(fileext = ".db"), 
                             dbtable = deparse(substitute(x)),
                             overwrite = FALSE, ...)
{
    ## browser()
    if (file.exists(dbname)) {
        dbname <- file_path_as_absolute(dbname)
        if (overwrite == FALSE)
            stop("The 'dbname' already exists! Please provide a new value ",
                 "OR change 'overwrite = TRUE'. ")
    }

    if (is(x@tblData$ops, "op_base") ) {  ## FIXME: mutate? / filter
        con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
        aux <- .attach_database(con, dbname(x))
        tblx <- .open_tbl_from_connection(con, aux, x)  ## already
                                                        ## evaluated
                                                        ## ridx here.
        sql_cmd <- dbplyr::db_sql_render(con, tblx)
    } else if (is(x@tblData$ops, "op_double") | is(x@tblData$ops, "op_mutate")) {
        con <- .con_SQLDataFrame(x)
        sql_cmd <- dbplyr::db_sql_render(con, x@tblData)
        if (!is.null(ridx(x))) {  ## applies to SQLDataFrame from "rbind"
            dbWriteTable(con, paste0(dbtable, "_ridx"), value = data.frame(ridx = ridx(x)))
        }
    }
    dbExecute(con, build_sql("CREATE TABLE ", dbtable, " AS ", sql_cmd))
    ## error if "dbtable" already exist. "Error: table aa already
    ## exists". Not likely happen here, because SQLDataFrame generated
    ## from "join" or "union" has connection to a new temporary .db
    ## file with empty contents.

    ## add unique index file with dbkey(x)
    dbplyr:::db_create_indexes.DBIConnection(con, dbtable, indexes = list(dbkey(x)), unique = TRUE)
    ## FIXME: implement "overwrite" argument here for the index file. if (found & overwrite)
    ## https://www.w3schools.com/sql/sql_create_index.asp
    ## DROP INDEX table_name.index_name;
    ## see also: dbRemoveTable()
    
    if (is(x@tblData$ops, "op_double") | is(x@tblData$ops, "op_mutate")) {
        file.copy(dbname(x), dbname, overwrite = overwrite)
    }
    msg_saveSQLDataFrame(x, dbname, dbtable)
    res <- SQLDataFrame(dbname = dbname, dbtable = dbtable, dbkey = dbkey(x))
    invisible(res)
}

msg_saveSQLDataFrame <- function(x, dbname, dbtable) {
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  ## "## Database: ", db_desc(con), "\n", 
                  "## Database: ",
                  paste("sqlite ", dbplyr:::sqlite_version(), " [", dbname, "] \n"),
                  "## Use the following command to reload into R: \n",
                  "## dat <- SQLDataFrame(\n",
                  "##   dbname = \"", dbname, "\",\n",
                  "##   dbtable = \"", dbtable, "\",\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

## ## open a new connection to the new "dbname". 
##     copy_to(con, res[[length(res)]], name = dbtable,
##             temporary = FALSE, overwrite = TRUE,  ## overwrite? 
##             unique_indexes = NULL, indexes = list(dbkey(x)),
##             analyze = TRUE, ...)

##     for (i in rev(seq_len(length(res)-1))){
##         sql_tbl_append <- dbplyr::db_sql_render(con, res[[i]])
##         ## check ROWNAMES, only append unique rows
##         dbExecute(con, build_sql("INSERT INTO ", sql(dbtable), " ", sql_tbl_append))
        
        
##     }
##     ## dbGetQuery(con, paste0("SELECT * FROM ", in_schema()))
##     ## open a new connection to write database table.
##     con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
##     ## attach the dbname of the to-be-copied "lazy tbl" to the new connection.
##     ## FIXME: possible to attach an online database?
##     auxName <- "aux"
##     DBI::dbExecute(con, paste0("ATTACH '", dbname(x), "' AS ", auxName))
    
##     ## open the to-be-copied "lazy tbl" from new connection.
##     tbl1 <- tbl(con, in_schema("aux", ident(dbtable(sdf1))))
##     ## apply all @indexes to "tbl_dbi" object (that opened from destination connection).
##     tbl1 <- .extract_tbl_from_SQLDataFrame_indexes(tbl1, sdf1) ## reorder by "key + otherCols"

##     copy_to(con, tbl1, name = dbtable,
##             temporary = FALSE, overwrite = TRUE,  ## overwrite? 
##             unique_indexes = NULL, indexes = list(dbkey(sdf1)),
##             analyze = TRUE, ...)
##     ## by default "temporary = FALSE", to physically write the table,
##     ## not only in the current connection. "indexes = dbkey(x)", to
##     ## accelerate the query lookup
##     ## (https://www.sqlite.org/queryplanner.html).
    

