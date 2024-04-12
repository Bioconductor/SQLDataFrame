#' SQL extensions
#'
#' Extensions of SQLDataFrame, SQLColumnVector, SQLColumnSeed with
#' different SQL backends. Currently supporting SQLite and DuckDB,
#' with which the definition coding can be followed for added
#' extension of other SQL backends.
#'
#' @param path String containing a path to a SQL file.
#' @param table String containing the name of the table in SQL file.
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
#'
#' @examples
#' ## Mocking up a file:
#'
#' ### SQLite
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' con <- DBI::dbConnect(RSQLite::SQLite(), tf)
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#' DBI::dbDisconnect(con)
#'
#' ### DuckDB
#' tf1 <- tempfile()
#' on.exit(unlist(tf1))
#' con <- DBI::dbConnect(duckdb::duckdb(), tf1)
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#' DBI::dbDisconnect(con)
#'
#' ## Constructor of xxColumnSeed and xxColumnVector
#'
#' sd <- SQLiteColumnSeed(tf, "mtcars", "gear")
#' scv <- SQLiteColumnVector(sd)
#' scv1 <- SQLiteColumnVector(tf, "mtcars", "gear")
#' identical(scv, scv1)
#' 
#' DuckDBColumnSeed(tf1, "mtcars", "mpg")
#' DuckDBColumnVector(tf1, "mtcars", "mpg")
#'
#' ## Constructor of xxDataFrame
#'
#' SQLiteDataFrame(tf, "mtcars")
#' DuckDBDataFrame(tf1, "mtcars")
#' 
#' @rdname SQL_extensions
#' @aliases
#' SQLiteDataFrame-class
#' DuckDBDataFrame-class
#' SQLiteColumnSeed-class
#' SQLiteColumnVector-class
#' DuckDBColumnSeed-class
#' DuckDBColumnVector-class
#' DelayedArray,SQLiteColumnSeed-method
#' DelayedArray,DuckDBColumnSeed-method

###############
## SQLite
###############

#' @export
#'
setClass("SQLiteColumnSeed", contains = "SQLColumnSeed")

#' @export
SQLiteColumnSeed <- function(path, table, column, length = NULL, type = NULL) {
    dbtype <- "SQLite"
    sd <- SQLColumnSeed(path = path, dbtype = dbtype, table = table, column = column,
                        length = length, type = type)
    new("SQLiteColumnSeed", path=path(sd), dbtype = dbtype(sd), table=sqltable(sd),
        column=sd@column, length=dim(sd), type=type(sd))
}

#' @export
setClass("SQLiteColumnVector", contains = "DelayedArray", slots = c(seed = "SQLiteColumnSeed"))

#' @export
setMethod("DelayedArray", "SQLiteColumnSeed", function(seed) new("SQLiteColumnVector", seed=seed))

#' @export
SQLiteColumnVector <- function(x, ...) {
    if (!is(x, "SQLiteColumnSeed")) {
        x <- SQLiteColumnSeed(x, ...)
    }
    new("SQLiteColumnVector", seed=x)
}

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "SQLiteColumnSeed", function(x, index) {
    callNextMethod()
})

#' @export
setClass("SQLiteDataFrame", contains = "SQLDataFrame")

#' @export
SQLiteDataFrame <- function(path, table=NULL, columns=NULL, nrows=NULL) {
    dbtype <- "SQLite"
    SQLDataFrame(path=path, dbtype = dbtype, table = table,
                 columns=columns, nrows=nrows)
}

###############
## DuckDB
###############

#' @export
#'
setClass("DuckDBColumnSeed", contains = "SQLColumnSeed")

#' @export
DuckDBColumnSeed <- function(path, table, column, length = NULL, type = NULL) {
    dbtype <- "DuckDB"
    sd <- SQLColumnSeed(path = path, dbtype = dbtype, table = table, column = column,
                        length = length, type = type)
    new("DuckDBColumnSeed", path=path(sd), dbtype = dbtype(sd), table=sqltable(sd),
        column=sd@column, length=dim(sd), type=type(sd))

}

#' @export
setClass("DuckDBColumnVector", contains = "DelayedArray", slots = c(seed = "DuckDBColumnSeed"))

#' @export
setMethod("DelayedArray", "DuckDBColumnSeed", function(seed) new("DuckDBColumnVector", seed=seed))

#' @export
DuckDBColumnVector <- function(x, ...) {
    if (!is(x, "DuckDBColumnSeed")) {
        x <- DuckDBColumnSeed(x, ...)
    }
    new("DuckDBColumnVector", seed=x)
}

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "DuckDBColumnSeed", function(x, index) {
    callNextMethod()
})

#' @export
setClass("DuckDBDataFrame", contains = "SQLDataFrame")

#' @export
DuckDBDataFrame <- function(path, table=NULL, columns=NULL, nrows=NULL) {
    dbtype <- "DuckDB"
    SQLDataFrame(path=path, dbtype = dbtype, table = table,
                 columns=columns, nrows=nrows)
}

###############
## Parquet
###############

#' @export
#'
setClass("ParquetColumnSeed", contains = "SQLColumnSeed")

#' @export
ParquetColumnSeed <- function(path, column, length = NULL, type = NULL) {
    dbtype <- "Parquet"
    table <- "my_parquet_table"
    sd <- SQLColumnSeed(path = path, dbtype = dbtype, table = table, column = column,
                        length = length, type = type)
    new("ParquetColumnSeed", path=path(sd), dbtype = dbtype(sd), table=sqltable(sd),
        column=sd@column, length=dim(sd), type=type(sd))

}

#' @export
setClass("ParquetColumnVector", contains = "DelayedArray", slots = c(seed = "ParquetColumnSeed"))

#' @export
setMethod("DelayedArray", "ParquetColumnSeed", function(seed) new("ParquetColumnVector", seed=seed))

#' @export
ParquetColumnVector <- function(x, ...) {
    if (!is(x, "ParquetColumnSeed")) {
        x <- ParquetColumnSeed(x, ...)
    }
    new("ParquetColumnVector", seed=x)
}

#' @export
#' @importFrom DelayedArray extract_array
setMethod("extract_array", "ParquetColumnSeed", function(x, index) {
    callNextMethod()
})

#' @export
setClass("ParquetDataFrame", contains = "SQLDataFrame")

#' @export
ParquetDataFrame <- function(path, columns=NULL, nrows=NULL) {
    dbtype <- "Parquet"
    table <- "my_parquet_table"
    SQLDataFrame(path=path, dbtype = dbtype, table = table,
                 columns=columns, nrows=nrows)
}

