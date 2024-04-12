#' SQL-backed DataFrame
#'
#' Create a SQL-backed \linkS4class{DataFrame}, where the data are
#' kept on disk until requested. Direct extension classes are
#' \code{SQLiteDataFrame}, \code{DuckDBDataFrame}, and
#' \code{ParquetDataFrame}.
#'
#' @param path String containing a path to a SQL file.
#' @param dbtype String containing the SQL database type (case
#'     insensitive). Supported types are "SQLite", "DuckDB", and
#'     "Parquet".
#' @param table String containing the name of SQL table.
#' @param columns Character vector containing the names of columns in
#'     a SQL table. If \code{NULL}, this is determined from
#'     \code{path}.
#' @param nrows Integer scalar specifying the number of rows in a SQL
#'     table. If \code{NULL}, this is determined from \code{path}.
#' @return A SQLDataFrame where each column is a
#'     \linkS4class{SQLColumnVector}.
#'
#' @details The SQLDataFrame is essentially just a
#'     \linkS4class{DataFrame} of \linkS4class{SQLColumnVector}
#'     objects. It is primarily useful for indicating that the
#'     in-memory representation is consistent with the underlying
#'     SQL file (e.g., no delayed filter/mutate operations have
#'     been applied, no data has been added from other files). Thus,
#'     users can specialize code paths for a SQLDataFrame to
#'     operate directly on the underlying SQL table.
#'
#' In that vein, operations on a SQLDataFrame may return another
#' SQLDataFrame if the operation does not introduce inconsistencies
#' with the file-backed data. For example, slicing or combining by
#' column will return a SQLDataFrame as the contents of the retained
#' columns are unchanged. In other cases, the SQLDataFrame will
#' collapse to a regular \linkS4class{DFrame} of
#' \linkS4class{SQLColumnVector} objects before applying the
#' operation; these are still file-backed but lack the guarantee of
#' file consistency.
#'
#' @author Qian Liu
#' @examples
#'
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
#' on.exit(unlink(tf1))
#' con <- DBI::dbConnect(duckdb::duckdb(), tf1)
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#' DBI::dbDisconnect(con)
#'
#' ### Parquet
#' tf2 <- tempfile()
#' on.exit(unlink(tf2))
#' arrow::write_dataset(mtcars, tf2, format = "parquet")
#' dir(tf2)
#'  
#' ## Creating a SQLite-backed data frame:
#'
#' df <- SQLDataFrame(tf, dbtype = "SQLite", table = "mtcars")
#' df1 <- SQLiteDataFrame(tf, "mtcars")
#' identical(df, df1)
#' 
#' ## DuckDB-backed data frame:
#' df2 <- SQLDataFrame(tf1, dbtype = "duckdb", table = "mtcars")
#' df3 <- DuckDBDataFrame(tf1, "mtcars")
#' identical(df2, df3)
#'
#' ## Parquet-backed data frame: (opened through a duckdb connection)
#' df4 <- SQLDataFrame(tf2, dbtype = "parquet") ## does not request "table"
#' df5 <- ParquetDataFrame(tf2)  ## does not request "table"
#' identical(df4, df5)
#' 
#' ## Extraction yields a SQLiteColumnVector:
#' df$carb
#'
#' ## Some operations preserve the SQLDataFrame:
#' df[,1:5]
#' combined <- cbind(df, df)
#' class(combined)
#'
#' ## ... but most operations collapse to a regular DFrame:
#' df[1:5,]
#' combined2 <- cbind(df, some_new_name=df[,1])
#' class(combined2)
#'
#' df1 <- df
#' rownames(df1) <- paste0("row", seq_len(nrow(df1)))
#' class(df1)
#'
#' df2 <- df
#' colnames(df2) <- letters[1:ncol(df2)]
#' class(df2)
#'
#' df3 <- df
#' df3$carb <- mtcars$carb
#' class(df3)
#' 
#' ## Utility functions
#' path(df)
#' dbtype(df)
#' sqltable(df)
#' dim(df)
#' names(df)
#'
#' as.data.frame(df)
#' 
#' @rdname SQLDataFrame
#' @aliases
#' SQLDataFrame-class
#'
#' nrow,SQLDataFrame-method
#' ncol,SQLDataFrame-method
#' length,SQLDataFrame-method
#' path,SQLDataFrame-method
#'
#' rownames,SQLDataFrame-method
#' names,SQLDataFrame-method
#' rownames<-,SQLDataFrame-method
#' names<-,SQLDataFrame-method
#'
#' extractROWS,SQLDataFrame,ANY-method
#' extractCOLS,SQLDataFrame-method
#' [[,SQLDataFrame-method
#'
#' replaceROWS,SQLDataFrame-method
#' replaceCOLS,SQLDataFrame-method
#' normalizeSingleBracketReplacementValue,SQLDataFrame-method
#' [[<-,SQLDataFrame-method
#'
#' cbind,SQLDataFrame-method
#' cbind.SQLDataFrame
#'
#' as.data.frame,SQLDataFrame-method
#' coerce,SQLDataFrame,DFrame-method
#'
#' @export
SQLDataFrame <- function(path, dbtype=NULL, table=NULL, columns=NULL, nrows=NULL) {
    if (is.null(dbtype))
        stop("Please specify the SQL database type: sqlite, duckdb, parquet.")
    dbtype <- match.arg(tolower(dbtype), c("sqlite", "duckdb", "parquet"))
    con <- acquireConn(path, dbtype)
    if (dbtype == "parquet") {
        table <- "my_parquet_table"
    } else if (is.null(table)) {
        tbls <- DBI::dbListTables(con)
        stop("Please specify a table name. \n Available tables are: ",
             paste(tbls, collapse=", "))
    }
    if (is.null(columns) || is.null(nrows)) {
        if (is.null(columns)) {
            columns <- DBI::dbListFields(con, table)
        }
        if (is.null(nrows)) {
            nrows <- as.integer(DBI::dbGetQuery(con, paste0("SELECT COUNT(", columns[1], ") FROM ", table))[,1])
        }
    } 
    new("SQLDataFrame", path=path, dbtype = dbtype, table = table, columns=columns, nrows=nrows)
}

#' @export
setClass("SQLDataFrame", contains="DataFrame",
         slots=c(path="character", dbtype="character", table="character", columns="character", nrows="integer"))

#' @export
setMethod("nrow", "SQLDataFrame", function(x) x@nrows)

#' @export
setMethod("length", "SQLDataFrame", function(x) length(x@columns))

#' @export
setMethod("path", "SQLDataFrame", function(object) object@path)

#' @export
setMethod("sqltable", "SQLDataFrame", function(x) x@table)

#' @export
setMethod("dbtype", "SQLDataFrame", function(x) x@dbtype)

#' @export
setMethod("rownames", "SQLDataFrame", function(x) NULL)

#' @export
setMethod("names", "SQLDataFrame", function(x) x@columns)

#' @export
setReplaceMethod("rownames", "SQLDataFrame", function(x, value) {
    if (!is.null(value)) {
        x <- .collapse_to_df(x)
        rownames(x) <- value
    }
    x
})

#' @export
setReplaceMethod("names", "SQLDataFrame", function(x, value) {
    if (!identical(value, names(x))) {
        x <- .collapse_to_df(x)
        names(x) <- value
    }
    x
})

#' @export
#' @importFrom S4Vectors extractROWS
setMethod("extractROWS", "SQLDataFrame", function(x, i) {
    if (!missing(i)) {
        collapsed <- .collapse_to_df(x)
        extractROWS(collapsed, i)
    } else {
        x
    }
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors extractCOLS normalizeSingleBracketSubscript
setMethod("extractCOLS", "SQLDataFrame", function(x, i) {
    if (!missing(i)) {
        xstub <- setNames(seq_along(x), names(x))
        i <- normalizeSingleBracketSubscript(i, xstub)
        x@columns <- names(x)[i]
        x@elementMetadata <- extractROWS(x@elementMetadata, i)
    }
    x
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[", "SQLDataFrame", function(x, i, j, ...) {
    if (!missing(j)) {
        stop("list-style indexing of a SQLDataFrame with non-missing 'j' is not supported")
    }

    if (missing(i) || length(i) != 1L) {
        stop("expected a length-1 'i' for list-style indexing of a SQLDataFrame")
    }

    i <- normalizeDoubleBracketSubscript(i, x)
    SQLColumnVector(path(x), dbtype(x),  sqltable(x), column=names(x)[i])
})

#' @export
#' @importFrom S4Vectors replaceROWS
setMethod("replaceROWS", "SQLDataFrame", function(x, i, value) {
    x <- .collapse_to_df(x)
    replaceROWS(x, i, value)
})

#' @export
#' @importFrom S4Vectors normalizeSingleBracketReplacementValue
setMethod("normalizeSingleBracketReplacementValue", "SQLDataFrame", function(value, x) {
    if (is(value, "SQLColumnVector")) {
        return(new("SQLDataFrame", path=path(value@seed),
                   dbtype = dbtype(value@seed),
                   table = sqltable(value@seed),
                   columns=value@seed@column,
                   nrows=length(value)))
    }
    callNextMethod()
})

#' @export
#' @importFrom stats setNames
#' @importFrom S4Vectors replaceCOLS normalizeSingleBracketSubscript
setMethod("replaceCOLS", "SQLDataFrame", function(x, i, value) {
    xstub <- setNames(seq_along(x), names(x))
    i2 <- normalizeSingleBracketSubscript(i, xstub, allow.NAs=TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "SQLDataFrame")) {
            if (path(x) == path(value) &&
                dbtype(x) == dbtype(value) &&
                sqltable(x) == sqltable(value) &&
                identical(names(x)[i2], names(value))) {
                return(x)
            }
        }
    }
    # In theory, it is tempting to return a SQLDataFrame; the problem is
    # that assignment will change the mapping of column names to their
    # contents, so it is no longer a pure representation of a SQLDataFrame.
    x <- .collapse_to_df(x)
    replaceCOLS(x, i, value)
})

#' @export
#' @importFrom S4Vectors normalizeDoubleBracketSubscript
setMethod("[[<-", "SQLDataFrame", function(x, i, j, ..., value) {
    i2 <- normalizeDoubleBracketSubscript(i, x, allow.nomatch=TRUE)
    if (length(i2) == 1L && !is.na(i2)) {
        if (is(value, "SQLColumnVector")) {
            if (path(x) == path(value@seed) &&
                dbtype(x) == dbtype(value@seed) &&
                sqltable(x) == sqltable(value@seed) &&
                names(x)[i2] == value@seed@column) {
                return(x)
            }
        }
    }
    x <- .collapse_to_df(x)
    x[[i]] <- value
    x
})

#' @export
#' @importFrom S4Vectors mcols make_zero_col_DFrame combineRows
cbind.SQLDataFrame <- function(..., deparse.level=1) {
    preserved <- TRUE
    all_columns <- character(0)
    objects <- list(...)
    xpath <- NULL
    xtable <- NULL
    xdbtype <- NULL
    
    for (i in seq_along(objects)) {
        obj <- objects[[i]]
        if (is(obj, "SQLDataFrame")) {
            if (is.null(xpath)) {
                xpath <- path(obj)
                xtable <- sqltable(obj)
                xdbtype <- dbtype(obj)
            } else if (path(obj) != xpath ||
                       dbtype(obj) != xdbtype ||
                       sqltable(obj) != xtable) {
                preserved <- FALSE
                break
            } 
            all_columns <- c(all_columns, names(obj))

        } else if (is(obj, "SQLColumnVector")) {
            if (is.null(xpath)) {
                xpath <- path(obj@seed)
                xtable <- sqltable(obj@seed) 
                xdbtype <- dbtype(obj@seed)
            } else if (path(obj@seed) != xpath ||
                       dbtype(obj@seed) != xdbtype ||
                       sqltable(obj@seed) != xtable ||
                       !identical(names(objects)[i], obj@seed@column)) {
                preserved <- FALSE
                break
            } 
            all_columns <- c(all_columns, obj@seed@column)

        } else {
            preserved <- FALSE
            break
        }
    }

    if (!preserved) {
        for (i in seq_along(objects)) {
            obj <- objects[[i]]
            if (is(obj, "SQLDataFrame")) {
                objects[[i]] <- .collapse_to_df(obj)
            }
        }
        do.call(cbind, objects)

    } else {
        all_mcols <- list()
        has_mcols <- FALSE
        all_metadata <- list()

        for (i in seq_along(objects)) {
            obj <- objects[[i]]

            mc <- NULL
            md <- list()
            if (is(obj, "DataFrame")) {
                mc <- mcols(obj, use.names=FALSE)
                md <- metadata(obj)
                if (is.null(mc)) {
                    mc <- make_zero_col_DFrame(length(obj))
                } else {
                    has_mcols <- TRUE
                }
            } else {
                mc <- make_zero_col_DFrame(1)
            }

            all_mcols[[i]] <- mc
            all_metadata[[i]] <- md
        }

        if (has_mcols) {
            all_mcols <- do.call(combineRows, all_mcols)
        } else {
            all_mcols <- NULL
        }

        new("SQLDataFrame", 
            path=xpath,
            dbtype=xdbtype,
            table=xtable,
            columns=all_columns,
            nrows=NROW(objects[[1]]),
            elementMetadata=all_mcols,
            metadata=do.call(c, all_metadata)
        )
    }
}

#' @export
#' @importFrom S4Vectors bindCOLS
setMethod("cbind", "SQLDataFrame", cbind.SQLDataFrame)

#' @importFrom S4Vectors make_zero_col_DFrame mcols mcols<- metadata metadata<-
.collapse_to_df <- function(x) {
    df <- make_zero_col_DFrame(x@nrows)
    for (i in seq_along(names(x))) {
        df[[as.character(i)]] <- SQLColumnVector(path(x), dbtype(x), sqltable(x), column=names(x)[i])
    }
    colnames(df) <- names(x)
    mcols(df) <- mcols(x, use.names=FALSE)
    metadata(df) <- metadata(x)
    df
}

#' @export
setMethod("as.data.frame", "SQLDataFrame", function(x, row.names = NULL, optional = FALSE, ...) {
    con <- acquireConn(path(x), dbtype(x))
    ucol <- unique(x@columns)    
    tab <- DBI::dbGetQuery(con, paste0("SELECT ", paste(ucol, collapse=","), " FROM ", sqltable(x)))
    output <- as.data.frame(tab, row.names=row.names, optional=optional, ...)
    output <- output[,match(x@columns,colnames(output)),drop=FALSE]
    output
})

#' @export
setAs("SQLDataFrame", "DFrame", function(from) .collapse_to_df(from))

