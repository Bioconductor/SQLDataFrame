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


saveSQLDataFrame <- function(x, localConn = connSQLDataFrame(x),
                             dbname = tempfile(fileext = ".db"),  ## only used for SQLiteConnection.
                             dbtable = deparse(substitute(x)),
                             overwrite = FALSE,
                             index = TRUE, ...)
{
    ## browser()
    if (is(connSQLDataFrame(x), "MySQLConnection")) {
        con <- connSQLDataFrame(x)
        if (identical(con, localConn)) {
            if (!.mysql_has_write_perm(con))
                stop("Please provide a MySQL connection ",
                     "with write permission in argument: localConn")
            tbl <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
            sql_cmd <- build_sql("CREATE TABLE ", sql(dbtable), " AS ",
                                 db_sql_render(con, tbl), con = con) 
            dbExecute(con, sql_cmd)
            ## error1 table exists: <simpleError in .local(conn,
            ## statement, ...): could not run statement: Table 'sdf1'
            ## already exists>
            
            ## error2 no write permission: <simpleError in .local(conn,
            ## statement, ...): could not run statement: INSERT, CREATE
            ## command denied to user
            ## 'genome'@'c-67-99-175-226.roswellpark.org' for table
            ## 'sdf1'>
        } else {
            ## FIXME: check if lazy table (with queries) in remote
            ## connection. If yes, create fedtable separately, and
            ## pass old queries into new fed tables. If no, do the
            ## following.
            if (is(tblData(x)$ops, "op_join"))
                stop("Saving SQLDataFrame with lazy join queries ",
                     "is not supported!")
            if (!.mysql_has_write_perm(localConn))
                stop("Please provide a MySQL connection ",
                     "with write permission in argument: localConn")
            fedtable <- dplyr:::random_table_name()  ## temporary,
                                                     ## will be
                                                     ## removed if
                                                     ## saved
                                                     ## correctly!
            tbl <- .createFedTable_and_reopen_tbl(x,
                                                  localConn,
                                                  fedtable,
                                                  remotePswd = .get_mysql_var(con))
            con <- localConn
            sql_cmd <- build_sql("CREATE TABLE ", sql(dbtable)," AS ",
                                 db_sql_render(con, tbl), con = con) 
            trycreate <- try(dbExecute(con, sql_cmd))
            if (!is(trycreate, "try-error")) {
                sql_drop <- build_sql("DROP TABLE ", sql(fedtable), con = con)
                dbExecute(con, sql_drop)
            }
        }
    } else if(is(connSQLDataFrame(x), "SQLiteConnection")) { 
        if (file.exists(dbname)) {
            dbname <- file_path_as_absolute(dbname)
            if (overwrite == FALSE)
                stop("The 'dbname' already exists! Please provide a new value ",
                     "OR change 'overwrite = TRUE'. ")
        }
        if (is(tblData(x)$ops, "op_base") ) {  
            con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
            aux <- .attach_database(con, connSQLDataFrame(x)@dbname)
            tbl <- .open_tbl_from_connection(con, aux, x)  ## already
                                                            ## evaluated
                                                            ## ridx
                                                            ## here.
        } else if (is(tblData(x)$ops, "op_double") | is(tblData(x)$ops, "op_single")) { 
            con <- connSQLDataFrame(x)
            tbl <- tblData(x)  ## Since the "*_join", "union" function
                               ## returns @indexes as NULL. Only
                               ## "rbind" will retain the
                               ## @indexes[[1]], and which be
                               ## processed as an additional file
                               ## "dbtable_ridx". So here we only use
                               ## "tblData(x)" instead of
                               ## ".extract_tbl_from_SQLDataFrame_indexes"
            if (!is.null(ridx(x))) {  ## applies to SQLDataFrame from "rbind"
                dbWriteTable(con, paste0(dbtable, "_ridx"),
                             value = data.frame(ridx = ridx(x)))
            }
        }
        sql_cmd <- build_sql("CREATE TABLE ", sql(dbtable), " AS ", db_sql_render(con, tbl), con = con)
        dbExecute(con, sql_cmd)
    }
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

    ## following condition applies to both SQLite and MySQL,
    ## operations don't. The @dbname from SQLite includes the path,
    ## MySQL doesn't. For MySQL, dbGetInfo(connSQLDataFrame(x))$dbname
    ## only include the database name.
    if (is(con, "SQLiteConnection")) {
        if(is(tblData(x)$ops, "op_double") | is(tblData(x)$ops, "op_single")) {
            file.copy(connSQLDataFrame(x)@dbname, dbname, overwrite = overwrite)
        }
    }
    .msg_saveSQLDataFrame(x, con, dbtable)
    ## currently SQLDataFrame() constructor required 'password', so disable for now.
    res <- SQLDataFrame(conn = con, dbtable = dbtable, dbkey = dbkey(x))
    invisible(res)
}

## .mysqlErrorMsg <- function(dbtable) {
##     paste0("ERROR: \n", "1. Check you connection is still valid. \n",
##            "2. Check if the table of '", dbtable,
##            "' already exists! \n",
##            "3. Make sure your provided MySQL connection ",
##            "has write permission.")
## }

.msg_saveSQLDataFrame <- function(x, con, dbtable)
{
    type <- class(con)
    switch(type,
           MySQLConnection = .msg_save_mysql(x, con, dbtable),
           SQLiteConnection = .msg_save_sqlite(x, con, dbtable)
           ) 
}

.msg_save_mysql <- function(x, con, dbtable) {
    info <- dbGetInfo(con)
    databaseLine <- paste0("mysql ", info$serverVersion, " [",
                           .mysql_info(con),
                           ":/", info$dbname, "] \n")  ## legacy format from lazy_tbl
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## dat <- SQLDataFrame(\n",
                  "##   host = '", info$host, "',\n",
                  "##   user = '", info$user, "',\n",
                  ## "##   password = \"", .get_mysql_var(con), "\",\n",
                  "##   password = '',", "   ## Only if required!", "\n",
                  "##   type = 'MySQL',\n",
                  "##   dbname = '", info$dbname, "',\n",
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

.msg_save_sqlite <- function(x, con, dbtable) {
    databaseLine <- paste0("sqlite ", dbplyr:::sqlite_version(),
                           " [", con@dbname, "] \n")
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## dat <- SQLDataFrame(\n",
                  ## FIXME: localConn was not required in saveSQLDataFrame for SQLiteDataFrame...
                  "##   dbname = '", con@dbname, "',\n",
                  "##   type = 'SQLite',\n",
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

