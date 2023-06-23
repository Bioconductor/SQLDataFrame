.union_SQLDataFrame <- function(x, y, localConn, ...)
{
    type <- class(dbcon(x))
    switch(type,
           SQLiteConnection = .union_SQLDataFrame_sqlite(x, y),
           MySQLConnection = .union_SQLDataFrame_mysql(x, y, localConn))
}

.union_SQLDataFrame_mysql <- function(x, y,
                                      localConn) ## "localConn" only
                                                 ## used when both X
                                                 ## and Y are
                                                 ## MySQLConnection
                                                 ## without write
                                                 ## permission.
{
    out <- .doCompatibleFunction(x, y, copy = FALSE,
                                 FUN = dbplyr:::union.tbl_lazy,
                                 localConn = localConn)
    ## dbplyr:::union.tbl_lazy for MySQL will preserve the original
    ## order, but when "saveSQLDataFrame", the data will be sorted by
    ## the key columns and reordered in the saved database table.
    rnms <- unique(c(ROWNAMES(x), ROWNAMES(y)))
    BiocGenerics:::replaceSlots(out, dbconcatKey = rnms)
}

.union_SQLDataFrame_sqlite <- function(x, y)
{
    out <- .doCompatibleFunction(x, y, copy = FALSE,
                                 FUN = dbplyr:::union.tbl_lazy,
                                 localConn = localConn)
    ## dbplyr:::union.tbl_lazy will reorder the records (SQLite). need
    ## to update the !dbconcatKey to be consistent with the newly
    ## generated @tblData.
    rnms <- unique(c(ROWNAMES(x), ROWNAMES(y)))

    tt <- as.data.frame(do.call(rbind, strsplit(rnms, split = ":")),
                        stringsAsFactors = FALSE)
    cls <- tblData(out) %>% head %>% select(dbkey(x)) %>%
        as.data.frame() %>% vapply(class, character(1))
    for (i in seq_len(length(tt))) class(tt[,i]) <- unname(cls)[i]
    od <- do.call(order, tt)
    dbrnms <- rnms[od]
    
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}

#' Union of \code{SQLDataFrame} objects
#' @name union
#' @aliases union union,SQLDataFrame,SQLDataFrame-method
#' @rdname unionSQLDataFrame
#' @description Performs union operations on \code{SQLDataFrame}
#'     objects.
#' @param x A \code{SQLDataFrame} object.
#' @param y A \code{SQLDataFrame} object.
#' @param localConn A MySQL connection with write permission. Will be
#'     used only when \code{union}-ing two SQLDataFrame objects from
#'     different MySQL connections (to different MySQL databases), and
#'     neither has write permission. The situation is rare and
#'     operation is expensive. See Details for suggestions.
#' @param ... Other arguments passed on to methods.
#' @details The \code{union} function supports aggregation of
#'     SQLDataFrame objects from same or different connection (e.g.,
#'     cross databases), either with or without write permission.
#'
#'     SQLite database tables are supported by SQLDataFrame package,
#'     in the same/cross-database aggregation, and saving.
#'
#'     For MySQL databases, There are different situations:
#'
#'     When the input SQLDataFrame objects connects to same remote
#'     MySQL database without write permission (e.g., ensembl), the
#'     functions work like \code{dbplyr} with the lazy operations and
#'     a \code{DataFrame} interface. Note that the unioned
#'     SQLDataFrame can not be saved using \code{saveSQLDataFrame}.
#'
#'     When the input SQLDataFrame objects connects to different MySQL
#'     databases, and neither has write permission, the \code{union}
#'     function is supported but will be quite expensive. To avoid
#'     this situation, a more efficient way is to save the database
#'     table in local MySQL server (with write permission) using
#'     \code{saveSQLDataFrame}, and then call the \code{union}
#'     function again.
#'
#'     More frequent situation will be the \code{union} operation on
#'     two SQLDataFrame objects, of which at least one has write
#'     permission. Then the cross-database aggregation through
#'     SQLDataFrame package will be supported by generating federated
#'     table from the non-writable connection in the writable
#'     connection. Look for MySQL database manual for more details.
#'
#'     NOTE also, that the \code{union} operation on SQLDataFrame
#'     objects will perform differently on database table from SQLite
#'     or MySQL. For SQLite tables, the \code{union} will sort the
#'     data using the key columns, and write the sorted data into a
#'     provided SQLite dbname when \code{saveSQLDataFrame} was
#'     called. For MySQL tables, \code{union} preserves the orders,
#'     but will be sorted with key columns and saved into a user
#'     provided MySQL database (with write permission) when
#'     \code{saveSQLdataFrame} was called.
#' 
#' @return A \code{SQLDataFrame} object.
#' @export
#' @examples
#' test.db1 <- system.file("extdata/test.db", package = "SQLDataFrame")
#' test.db2 <- system.file("extdata/test1.db", package = "SQLDataFrame")
#' con1 <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db1)
#' con2 <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db2)
#' obj1 <- SQLDataFrame(conn = con1,
#'                      dbtable = "state",
#'                      dbkey = c("region", "population"))
#' obj2 <- SQLDataFrame(conn = con2,
#'                      dbtable = "state1",
#'                      dbkey = c("region", "population"))
#' obj1_sub <- obj1[1:10, 2:3]
#' obj2_sub <- obj2[8:15,2:3]
#'
#' ## union
#' res_union <- union(obj1_sub, obj2_sub)  ## sorted
#' dim(res_union)
#'

setMethod("union", signature = c("SQLDataFrame", "SQLDataFrame"), .union_SQLDataFrame)

