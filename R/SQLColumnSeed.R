#' Column of a SQL table
#'
#' Represent a column of a SQL table as a 1-dimensional
#' \linkS4class{DelayedArray}.  This allows us to use SQL data inside
#' \linkS4class{DataFrame}s without loading them into memory.
#'
#' @param path String containing a path to a SQL file.
#' @param dbtype String containing the SQL database type (case
#'     insensitive). Supported types are "SQLite", "DuckDB", and
#'     "Parquet".
#' @param table String containing the name of the table in SQL
#'     file. For Parquet, it overwrites any given value here by
#'     "my_parquet_table".
#' @param column String containing the name of the column inside the
#'     table.
#' @param length Integer containing the number of rows. If
#'     \code{NULL}, this is determined by inspecting the SQL
#'     table. This should only be supplied for efficiency purposes, to
#'     avoid a file look-up on construction.
#' @param type String specifying the type of the data. If \code{NULL},
#'     this is determined by inspecting the file. Users may specify
#'     this to avoid a look-up, or to coerce the output into a
#'     different type.
#' @param x A SQLColumnSeed object.

## #' @param index An unnamed list of subscripts as positive integer
## vectors, one vector per dimension in \code{x}. Empty and missing
## #subscripts (represented by \code{integer(0)} and \code{NULL} list
## elements, respectively) are allowed. The subscripts can contain
## duplicated indices. They cannot contain NAs or non-positive values.

#' @param ... Further arguments to be passed to the
#'     \code{SQLColumnSeed} constructor.
#' @return For \code{SQLColumnSeed}: a SQLColumnSeed. For
#'     \code{SQLColumnVector}: a SQLColumnVector.
#' @author Qian Liu
#'
#' @examples
#' # Mocking up a file:
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' con <- DBI::dbConnect(RSQLite::SQLite(), tf)
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#' DBI::dbDisconnect(con)
#' 
#' # Creating a vector:
#' SQLColumnVector(tf, dbtype = "SQLite", "mtcars", column="gear")
#'
#' # This happily lives inside DataFrames:
#' collected <- list()
#' for (x in colnames(mtcars)) {
#'     collected[[x]] <- SQLColumnVector(tf, dbtype = "SQLite", "mtcars", column=x)
#' }
#' DataFrame(collected)
#' #'
#' @aliases
#' SQLColumnSeed-class
#' dim,SQLColumnSeed-method
#' type,SQLColumnSeed-method
#' path,SQLColumnSeed-method
#' sqltable,SQLColumnSeed-method
#' extract_array,SQLColumnSeed-method
#' SQLColumnVector-class
#' DelayedArray,SQLColumnSeed-method
#'
#' @name SQLColumnSeed
NULL


#' @export
#' @import methods
setClass("SQLColumnSeed", slots=c(
                             path="character",
                             dbtype="character",
                             table="character",
                             column="character",
                             length="integer",
                             type="character"
                         ))

#' @export
#' @rdname SQLColumnSeed
#' @importFrom DelayedArray type
SQLColumnSeed <- function(path, dbtype, table, column, length=NULL, type=NULL) {
    if (is.null(dbtype))
        stop("Please specify the SQL database type: sqlite, duckdb, parquet.")
    dbtype <- match.arg(tolower(dbtype), c("sqlite", "duckdb", "parquet"))
    if (dbtype == "parquet") {
        table <- "my_parquet_table"
    }
    if (is.null(length) || is.null(type)) {
        con <- acquireConn(path, dbtype)
        if (is.null(type)) {
            out <- DBI::dbGetQuery(con, paste0("SELECT ", column, " FROM ", table, " LIMIT 1"))
            type <- typeof(out[,column])
        }
        if (is.null(length)) {
            length <- as.integer(DBI::dbGetQuery(con, paste0("SELECT COUNT(", column, ") FROM ", table))[,1])
        }
    }
    new("SQLColumnSeed", path=path, dbtype = dbtype, table=table, column=column, length=length, type=type)
}

#' @export
setMethod("dim", "SQLColumnSeed", function(x) x@length)

#' @export
#' @importFrom DelayedArray type
setMethod("type", "SQLColumnSeed", function(x) x@type)

#' @export
#' @importFrom BiocGenerics path
setMethod("path", "SQLColumnSeed", function(object) object@path)

#' @export
setGeneric("sqltable", signature = "x", function(x)
    standardGeneric("sqltable"))

#' @export
setMethod("sqltable", "SQLColumnSeed", function(x) x@table)

#' @export
setGeneric("dbtype", signature = "x", function(x)
    standardGeneric("dbtype"))

#' @export
setMethod("dbtype", "SQLColumnSeed", function(x) x@dbtype)

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "SQLColumnSeed", function(x, index) {
    con <- acquireConn(path(x), dbtype(x))
    i <- index[[1]]
    if (is.null(i)) {
        res <- DBI::dbGetQuery(con, paste0("SELECT ", x@column, " FROM ", sqltable(x)))
    } else {
        DBI::dbWriteTable(con, "tmp_indices", data.frame(indices=i), temporary=TRUE, overwrite=TRUE)
        res <- DBI::dbGetQuery(con, sprintf("SELECT x.%s,x.dotrow FROM (SELECT %s, ROW_NUMBER () OVER (ORDER BY 1) AS dotrow FROM %s) x INNER JOIN tmp_indices ON tmp_indices.indices = x.dotrow",
                                            x@column, x@column, sqltable(x))
                               )
        res <- res[match(i, res$dotrow), x@column, drop=FALSE]
    }  
    array(res[,x@column])
})

#' @export
setClass("SQLColumnVector", contains="DelayedArray", slots=c(seed="SQLColumnSeed"))

#' @export
#' @importFrom DelayedArray DelayedArray
setMethod("DelayedArray", "SQLColumnSeed", function(seed) new("SQLColumnVector", seed=seed))

#' @export
#' @rdname SQLColumnSeed
SQLColumnVector <- function(x, ...) {
    if (!is(x, "SQLColumnSeed")) {
        x <- SQLColumnSeed(x, ...)
    }
    new("SQLColumnVector", seed=x)
}

