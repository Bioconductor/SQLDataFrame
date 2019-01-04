## the newly saved SQLDataFrame must have a key column.
## output message:
## Source: table<> [?? X ncol]
## Database: sqlite 3.22.0 [/home/qian/...]
## Key(s): dbkey(x)
## Use the following code to read into R as SQLDataFrame object:
## dat <- SQLDataFrame(dbname, dbtable, dbkey)
#' @param ... other parameters passed to methods.

saveSQLDataFrame <- function(x, dbtable = dplyr:::random_table_name(),
                             overwrite = FALSE, types = NULL, ...)
{
    browser()
    tbl <- .extract_tbl_from_SQLDataFrame(x)
    ## invisible(compute(tbl, name = dbtable))
    ## compute(tbl, name = dbtable)
    copy_to(tbl$src$con, tbl, name = dbtable, temporary = FALSE, overwrite = overwrite,
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
                  "## Database: sqlite 3.22.0 \n", ## FIXME: where to
                  extract "sqlite 3.22.0"?  "## [", dbname(x), "] \n",
                  "## Use the following command to read it into R: \n",
                  "## dat <- SQLDataFrame(\n", "## dbname = \"",
                  dbname(x), "\",\n", "## dbtable = \"", dbtable,
                  "\",\n", "## dbkey = \"", dbkey(x), "\")", "\n",
                  "\n",

                  "## A new database table is saved. \n",
                  "## Database: \n",
                  "##   dbname: ", dbname(x), "\n",
                  "##   dbtable: ", dbtable, " [", paste(dim(x), collapse = " X "), "] \n",
                  "##   dbkey: ", paste(dbkey(x), collapse = " ,"), "\n", 
                  "## Use the following command to read into R: \n",
                  "##   dat <- SQLDataFrame(dbname, dbtable, dbkey)"
                  )
    message(msg)
}

## random_table_name <- function(n = 10) {
##     paste0(sample(letters, n, replace = TRUE), collapse = "")

