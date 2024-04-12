persistent <- new.env()
persistent$handles <- list()

#' Acquire the SQL file connection 
#'
#' Acquire a (possibly cached) SQL file connection given it's path.
#' 
#' @param path String containing a path to a SQL file.
#' @param dbtype String containing the SQL database type (case
#'     insensitive). Supported types are "SQLite", "DuckDB", and
#'     "Parquet".
#' @return For \code{acquireConn}, a DBIConnection with backends of
#'     SQLite, DuckDB, or Parquet, which are identical to that
#'     returned by \code{DBI::dbConnect(RSQLite::SQLite(), path)} for
#'     SQLite tables, or \code{DBI::dbConnect(duckdb::duckdb(), path)}
#'     for DuckDB table, or a duckdb connection with a virtual parquet
#'     table that was opened as arrow dataset and then registered in a
#'     duckdb connection.
#'
#' For \code{releaseConn}, any existing DBIConnection for the
#' \code{path} is disconnected and cleared from cache, and \code{NULL}
#' is invisibly returned. If \code{path=NULL}, all cached connections
#' are removed.
#' @author Qian Liu
#' @details \code{acquireConn} will cache the DBIConnection object in
#'     the current R session to avoid repeated initialization. This
#'     improves efficiency for repeated calls, e.g., when creating a
#'     \linkS4class{DataFrame} with multiple columns from the same SQL
#'     table. The cached DBIConnection for any given \code{path} can
#'     be deleted by calling \code{releaseConn} for the same
#'     \code{path}.
#'
#' @examples
#'
#' ###########
#' ## SQLite
#' ###########
#' 
#' ## Mocking up a file
#' tf <- tempfile()
#' on.exit(unlink(tf))
#' con <- DBI::dbConnect(RSQLite::SQLite(), tf)
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#' DBI::dbDisconnect(con)
#'
#' ## Acquire or release connection
#' con <- acquireConn(tf, dbtype = "SQLite")
#' acquireConn(tf, dbtype = "SQLite") # just re-uses the cache
#' releaseConn(tf) # clears the cache
#'
#' ###########
#' ## DuckDB
#' ###########
#' 
#' tf1 <- tempfile()
#' on.exit(unlink(tf1))
#' con <- DBI::dbConnect(duckdb::duckdb(), tf1)
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#' DBI::dbDisconnect(con)
#' con <- acquireConn(tf1, dbtype = "DuckDB")
#' releaseConn(tf1)
#'
#' ############
#' ## Parquet
#' ############
#'
#' tf2 <- tempfile()
#' on.exit(unlink(tf2))
#' arrow::write_dataset(mtcars, tf2, format = "parquet")
#' dir(tf2)
#' con <- acquireConn(tf2, dbtype = "Parquet")
#' releaseConn(tf2)
#'
#' @importFrom arrow open_dataset write_dataset
#' @import dplyr
#' @export
#' @rdname acquireConn
#' @importFrom utils tail

acquireConn <- function(path, dbtype = NULL) {
    ## browser()
    if (is.null(dbtype))
        stop("Please specify the SQL database type: sqlite, duckdb, parquet.")
    dbtype <- tolower(dbtype)
    
    ## Here we set up an LRU cache for the SQLite connection. 
    ## This avoids the initialization time when querying lots of columns.
    nhandles <- length(persistent$handles)

    i <- which(names(persistent$handles) == path)
    if (length(i)) {
        output <- persistent$handles[[i]]
        if (i < nhandles) {
            persistent$handles <- persistent$handles[c(seq_len(i-1L), seq(i+1L, nhandles), i)]
            ## moving to the back
        }
        return(output)
    }

    ## Pulled this value from most recent cache
    limit <- 100
    if (nhandles >= limit) {
        persistent$handles <- tail(persistent$handles, limit - 1L)
    }

    if (dbtype %in% c("sqlite", "duckdb")) {
        drv <- switch(dbtype,
                      "sqlite" = RSQLite::SQLite(), 
                      "duckdb"= duckdb::duckdb())        
        output <- DBI::dbConnect(drv, path)
    } else if (dbtype == "parquet") {
        output <- DBI::dbConnect(duckdb::duckdb())
        ds <- arrow::open_dataset(path)
        duckdb::duckdb_register(output, "my_parquet_table", ds)
        ## open parquet as a virtual table in duckdb connection
        ## ref: https://www.richpauloo.com/blog/parquet/
    }
    persistent$handles[[path]] <- output
    output
}

#' @export
#' @rdname acquireConn
releaseConn <- function(path) {
    if (is.null(path)) {
        persistent$handles <- list()
    } else {
        i <- which(names(persistent$handles) == path)
        if (length(i)) {
            con <- persistent$handles[[i]]
            if (grepl(".parquet", path)) {
                duckdb::duckdb_unregister(con, "my_parquet_table")
            }
            DBI::dbDisconnect(con)  
            persistent$handles <- persistent$handles[-i]
        }
    }
    invisible(NULL)
}
