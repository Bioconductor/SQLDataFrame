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

saveSQLDataFrame <- function(x, dbname, 
                             dbtable = deparse(substitute(x)),
                             ## overwrite = FALSE, ## couldn't pass to
                             ## "copy_to" then "compute"
                             types = NULL, ...)
{
    ## browser()
    file.create(dbname) ## create if not already exists.
    dbname <- file_path_as_absolute(dbname)

    ## open a new connection to write database table.
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    ## attach the dbname of the to-be-copied "lazy tbl" to the new connection.
    ## FIXME: possible to attach an online database?
    DBI::dbExecute(con, paste0("ATTACH '", x@tblData$src$con@dbname, "' AS aux"))
    ## open the to-be-copied "lazy tbl" from new connection.
    tbl <- tbl(con, in_schema("aux", x@tblData$ops$x))
    ## apply all @indexes to "tbl_dbi" object (that opened from destination connection).
    ridx <- x@indexes[[1]]
    if (!is.null(ridx))
        tbl <- .extract_tbl_rows_by_key(tbl, dbkey(x), ridx)
    tbl <- tbl %>% select(dbkey(x), colnames(x))  ## order by "key + otherCols"

    copy_to(con, tbl, name = dbtable, temporary = FALSE,
            ## overwrite = overwrite,
            types = types, unique_indexes = NULL,
            indexes = list(dbkey(x)), analyze = TRUE, ...)
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

