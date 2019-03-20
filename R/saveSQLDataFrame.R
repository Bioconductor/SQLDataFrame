#' Save SQLDataFrame object as a new database table. 
#' @description The function to save \code{SQLDataFrame} object as a
#'     database table with a supplied path to database. 
#' @param x The \code{SQLDataFrame} object to be saved.
#' @param dbname A database file path to save the \code{SQLDataFrame}
#'     object.
#' @param dbtable The name of the new database table.
# #' @param types a character vector giving variable types to use for the
# #' columns. See http://www.sqlite.org/datatype3.html for available types.
#' @param ... other parameters passed to methods.
#' @export
#' @rdname saveSQLDataFrame
#' @examples
#' dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
#' ss <- SQLDataFrame(dbname = dbname, dbtable = "state", dbkey = "state")
#' ss1 <- ss[1:10, 2:3]
#' saveSQLDataFrame(ss1, tempfile(fileext = ".db"))

saveSQLDataFrame <- function(x, dbname = tempfile(fileext = ".db"), 
                             dbtable = deparse(substitute(x)),
                             ## overwrite = FALSE, ## couldn't pass to
                             ## "copy_to" then "compute"
                             types = NULL, ...)
{
    browser()
    if (!file.exists(dbname))
        file.create(dbname) 
    dbname <- file_path_as_absolute(dbname)

    ## open the existing connection
    con <- .con_SQLDataFrame(x)
    ops <- x@tblData$ops

    ## save intermediate y's as list elements
    res <- list()
    res[[1]] <- ops$y
    chk <- ops$x$ops$x
    if(is(chk, "op_double")) {
        i <- 2
        repeat{
            res[[i]] <- chk$y
            xx <- chk$x
            chk <- chk$x$ops$x
            i <- i+1
            if (is(chk, "op_single")) {
                res[[i]] <- xx
                break        
            }
        }
    }

    ## open a new connection to the new "dbname". 
    copy_to(con, res[[length(res)]], name = dbtable,
            temporary = FALSE, overwrite = TRUE,  ## overwrite? 
            unique_indexes = NULL, indexes = list(dbkey(x)),
            analyze = TRUE, ...)

    for (i in rev(seq_len(length(res)-1))){
        sql_tbl_append <- dbplyr::db_sql_render(con, res[[i]])
        ## check ROWNAMES, only append unique rows
        dbExecute(con, build_sql("INSERT INTO ", sql(dbtable), " ", sql_tbl_append))
        
        
    }
    ## dbGetQuery(con, paste0("SELECT * FROM ", in_schema()))
    ## open a new connection to write database table.
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    ## attach the dbname of the to-be-copied "lazy tbl" to the new connection.
    ## FIXME: possible to attach an online database?
    auxName <- "aux"
    DBI::dbExecute(con, paste0("ATTACH '", dbname(x), "' AS ", auxName))
    
    ## open the to-be-copied "lazy tbl" from new connection.
    tbl1 <- tbl(con, in_schema("aux", ident(dbtable(sdf1))))
    ## apply all @indexes to "tbl_dbi" object (that opened from destination connection).
    tbl1 <- .extract_tbl_from_SQLDataFrame_indexes(tbl1, sdf1) ## reorder by "key + otherCols"

    copy_to(con, tbl1, name = dbtable,
            temporary = FALSE, overwrite = TRUE,  ## overwrite? 
            unique_indexes = NULL, indexes = list(dbkey(sdf1)),
            analyze = TRUE, ...)
    ## by default "temporary = FALSE", to physically write the table,
    ## not only in the current connection. "indexes = dbkey(x)", to
    ## accelerate the query lookup
    ## (https://www.sqlite.org/queryplanner.html).
    
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [", paste(dim(x),
                  collapse = " X "), "] \n",
                  "## Database: ", db_desc(con), "\n",
                  "## Use the following command to reload into R: \n",
                  "## dat <- SQLDataFrame(\n",
                  "##   dbname = \"", dbname, "\",\n",
                  "##   dbtable = \"", dbtable, "\",\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

