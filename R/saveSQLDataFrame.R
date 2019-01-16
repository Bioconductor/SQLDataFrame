#' @param ... other parameters passed to methods.

saveSQLDataFrame <- function(x, dbname, 
                             dbtable = dplyr:::random_table_name(),
                             overwrite = FALSE,
                             types = NULL, ...)
{
    ## browser()
    dbname <- file_path_as_absolute(dbname)
    tbl <- .extract_tbl_from_SQLDataFrame(x)

    ## open a new connection, or "src_dbi", and write database table.
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    copy_to(con, tbl, name = dbtable, temporary = FALSE, overwrite = overwrite,
            types = types, unique_indexes = NULL, indexes = list(dbkey(x)),
            analyze = TRUE, ...)
    ## by default "temporary = FALSE", to physically write the table,
    ## not only in the current connection. "indexes = dbkey(x)", to
    ## accelerate the query lookup
    ## (https://www.sqlite.org/queryplanner.html).
    ## FIXME: "overwrite = TRUE" currently do not work...
    ## ?copy_to.src_sql
    ## copy_to(src_memdb(), cyl8, overwrite = T) does not work...
    
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

## random_table_name <- function(n = 10) {
##     paste0(sample(letters, n, replace = TRUE), collapse = "")

