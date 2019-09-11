#' Save SQLDataFrame object as a new database table.
#' @rdname saveSQLDataFrame
#' @description The function to save \code{SQLDataFrame} object as a
#'     database table with a supplied path to database. It also
#'     returns a \code{SQLDataFrame} object constructed from the
#'     user-supplied \code{dbname}, \code{dbtable}, and \code{dbkey}.
#' @param x The \code{SQLDataFrame} object to be saved.
#' @param dbname A character string of the file path of to be saved
#'     database file.
#' @param dbtable A character string for the to be saved database
#'     table name. Default is the name of the input
#'     \code{SQLDataFrame}.
#' @param overwrite Whether to overwrite the \code{dbtable} if already
#'     exists. Default is FALSE.
#' @param index Whether to create the database index. Default is TRUE.
#' @param ... other parameters passed to methods.
#' @return A \code{SQLDataFrame} object.
#' @import DBI
#' @import dbplyr
#' @rawNamespace import(dplyr, except = c("first", "rename",
#'     "setequal", "setdiff", "intersect", "union", "ident", "sql"))
#' @examples
#' test.db <- system.file("extdata/test.db", package = "SQLDataFrame")
#' conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
#' obj <- SQLDataFrame(conn = conn, dbtable = "state", dbkey = "state")
#' obj1 <- obj[1:10, 2:3]
#' obj1 <- saveSQLDataFrame(obj1, dbtable = "obj_subset")
#' connSQLDataFrame(obj1)
#' dbtable(obj1)
#' @export

saveSQLDataFrame <- function(x, dbname = tempfile(fileext = ".db"), 
                             ## outfile,
                             dbtable = deparse(substitute(x)),
                             overwrite = FALSE,
                             index = TRUE, ...)
{
    if (is(connSQLDataFrame(x), "MySQLConnection")) {
        con <- connSQLDataFrame(x)
        sql_cmd <- db_sql_render(con, .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x))
        ## dbExecute(con, build_sql(sql_cmd, " INTO OUTFILE ", outfile, con = con))
    } else { 
        if (file.exists(dbname)) {
            dbname <- file_path_as_absolute(dbname)
            if (overwrite == FALSE)
                stop("The 'dbname' already exists! Please provide a new value ",
                     "OR change 'overwrite = TRUE'. ")
        }
        if (is(tblData(x)$ops, "op_base") ) {  
            con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
            aux <- .attach_database(con, connSQLDataFrame(x)@dbname)
            tblx <- .open_tbl_from_connection(con, aux, x)  ## already
            ## evaluated
            ## ridx here.
            sql_cmd <- dbplyr::db_sql_render(con, tblx)
        } else if (is(tblData(x)$ops, "op_double") | is(tblData(x)$ops, "op_single")) { 
            con <- connSQLDataFrame(x)
            sql_cmd <- dbplyr::db_sql_render(con, tblData(x))
            if (!is.null(ridx(x))) {  ## applies to SQLDataFrame from "rbind"
                dbWriteTable(con, paste0(dbtable, "_ridx"),
                             value = data.frame(ridx = ridx(x)))
            }
        }
        dbExecute(con, build_sql("CREATE TABLE ", dbtable, " AS ", sql_cmd, con = con))
        ## error if "dbtable" already exist. "Error: table aa already
        ## exists". Not likely happen here, because SQLDataFrame generated
        ## from "join" or "union" has connection to a new temporary .db
        ## file with empty contents.
        
        ## add unique index file with dbkey(x)
        if (index)
            dbplyr:::db_create_indexes.DBIConnection(con, dbtable,
                                                     indexes = list(dbkey(x)),
                                                     unique = TRUE)
        ## FIXME: implement "overwrite" argument here for the index
        ## file. if (found & overwrite)
        ## https://www.w3schools.com/sql/sql_create_index.asp DROP INDEX
        ## table_name.index_name; see also: dbRemoveTable()
        
        if (is(tblData(x)$ops, "op_double") | is(tblData(x)$ops, "op_single")) {
            file.copy(connSQLDataFrame(x)@dbname, dbname, overwrite = overwrite)
        }
        msg_saveSQLDataFrame(x, dbname, dbtable)
        res <- SQLDataFrame(conn = con, dbtable = dbtable, dbkey = dbkey(x))
        invisible(res)
    }
}

msg_saveSQLDataFrame <- function(x, dbname, dbtable) {
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
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
