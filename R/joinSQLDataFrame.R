#########################
## left_join, inner_join
#########################

#' join \code{SQLDataFrame} together
#' @name left_join
#' @rdname joinSQLDataFrame
#' @description *_join functions for \code{SQLDataFrame} objects. Will
#'     preserve the duplicate rows for the input argument `x`.
#' @aliases left_join left_join,SQLDataFrame-method
#' @param x \code{SQLDataFrame} objects to join.
#' @param y \code{SQLDataFrame} objects to join.
#' @param by A character vector of variables to join by.  If ‘NULL’,
#'     the default, ‘*_join()’ will do a natural join, using all
#'     variables with common names across the two tables. See
#'     \code{?dplyr::join} for details.
#' @param suffix A character vector of length 2 specify the suffixes
#'     to be added if there are non-joined duplicate variables in ‘x’
#'     and ‘y’. Default values are ".x" and ".y".See
#'     \code{?dplyr::join} for details.
#' @param localConn A MySQL connection with write permission. Will be
#'     used only when \code{join}-ing two SQLDataFrame objects from
#'     different MySQL connections (to different MySQL databases), and
#'     neither has write permission. The situation is rare and should
#'     be avoided. See Details.
#' @param ... Other arguments passed on to \code{*_join} methods. 
#' @details The \code{*_join} functions support aggregation of
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
#'     a \code{DataFrame} interface. Note that the aggregated
#'     SQLDataFrame can not be saved using \code{saveSQLDataFrame}.
#'
#'     When the input SQLDataFrame objects connects to different MySQL
#'     databases, and neither has write permission, the \code{*_join}
#'     functions are supported but will be quite time consuming. To
#'     avoid this situation, a more efficient way is to save the
#'     database table in local MySQL server using
#'     \code{saveSQLDataFrame}, and then call the \code{*_join}
#'     functions again.
#'
#'     More frequent situation will be the \code{*_join} operation on
#'     two SQLDataFrame objects, of which at least one has write
#'     permission. Then the cross-database aggregation through
#'     SQLDataFrame package will be supported by generating federated
#'     table from the non-writable connection in the writable
#'     connection. Look for MySQL database manual for more details.
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
#'
#' obj1_sub <- obj1[1:10, 1:2]
#' obj2_sub <- obj2[8:15, 2:3]
#'
#' left_join(obj1_sub, obj2_sub)
#' inner_join(obj1_sub, obj2_sub)
#' semi_join(obj1_sub, obj2_sub)
#' anti_join(obj1_sub, obj2_sub)

left_join.SQLDataFrame <- function(x, y, by = NULL,
                                   suffix = c(".x", ".y"),
                                   localConn,
                                   ...) 
{
    out <- .doCompatibleFunction(x, y, by = by, copy = FALSE,
                                 suffix = suffix,
                                 auto_index = FALSE,
                                 FUN = dbplyr:::left_join.tbl_lazy,
                                 localConn = localConn)
    if (!identical(dbkey(x), dbkey(y))) {
        dbkey(out) <- c(dbkey(x), dbkey(y))
    } else {
        dbrnms <- unique(ROWNAMES(x))
        ind <- match(ROWNAMES(x), dbrnms)
        ind <- ind[!is.na(ind)]
        ridx <- NULL
        if (!identical(ind, seq_len(nrow(x)))) {
            ridx <- ind
        }
        out <- BiocGenerics:::replaceSlots(
                           out, dbconcatKey = dbrnms,
                           indexes = list(ridx, NULL))
    }
    out
}

#' @name inner_join
#' @rdname joinSQLDataFrame
#' @aliases inner_join inner_join,SQLDataFrame-method
#' @export
inner_join.SQLDataFrame <- function(x, y, by = NULL,
                                    suffix = c(".x", ".y"),
                                    localConn,...) 
{
    out <- .doCompatibleFunction(x, y, by = by, copy = FALSE,
                                 suffix = suffix,
                                 auto_index = FALSE,
                                 FUN = dbplyr:::inner_join.tbl_lazy,
                                 localConn = localConn)
    if (!identical(dbkey(x), dbkey(y))) {
        dbkey(out) <- c(dbkey(x), dbkey(y))
    } else {
        dbrnms <- intersect(ROWNAMES(x), ROWNAMES(y))
        ind <- match(ROWNAMES(x), dbrnms)
        ind <- ind[!is.na(ind)]
        ridx <- NULL
        if (!identical(ind, normalizeRowIndex(out))) {
            ridx <- ind
        }
        out <- BiocGenerics:::replaceSlots(
                                  out, dbconcatKey = dbrnms,
                                  indexes = list(ridx, NULL))
    }
    out
}

#########################
## semi_join, anti_join (filtering joins)
#########################

## for "semi_join", the new tblData()$ops is "op_semi_join".
## see show_query(tblData()), "...WHERE EXISTS..."
## semi_join is similar to `inner_join`, but doesn't add new columns.

#' @name semi_join
#' @rdname joinSQLDataFrame
#' @aliases semi_join semi_join,SQLDataFrame-method
#' @export
semi_join.SQLDataFrame <- function(x, y, by = NULL,
                                   suffix = c(".x", ".y"),
                                   localConn, ...) 
{
        out <- .doCompatibleFunction(x, y, by = by, copy = FALSE,
                                     suffix = suffix,
                                     auto_index = FALSE,
                                     FUN = dbplyr:::semi_join.tbl_lazy,
                                     localConn = localConn)
    if (!identical(dbkey(x), dbkey(y))) {
        dbkey(out) <- c(dbkey(x), dbkey(y))
    } else {        
        dbrnms <- intersect(ROWNAMES(x), ROWNAMES(y))
        ind <- match(ROWNAMES(x), dbrnms)
        ind <- ind[!is.na(ind)]
        ridx <- NULL
        if (!identical(ind, normalizeRowIndex(out))) {
            ridx <- ind
        }
        out <- BiocGenerics:::replaceSlots(
                                  out, dbconcatKey = dbrnms,
                                  indexes = list(ridx, NULL))
    }
    out
}

## for "anti_join", the new tblData()$ops is still "op_semi_join"
## see show_query(tblData()), "...WHERE NOT EXISTS..."

#' @name anti_join
#' @rdname joinSQLDataFrame
#' @aliases anti_join anti_join,SQLDataFrame-method
#' @export
anti_join.SQLDataFrame <- function(x, y, by = NULL,
                                   suffix = c(".x", ".y"),
                                   localConn,...) 
{
    out <- .doCompatibleFunction(x, y, copy = FALSE,
                                 FUN = dbplyr:::anti_join.tbl_lazy,
                                 localConn = localConn)
    if (!identical(dbkey(x), dbkey(y))) {
        dbkey(out) <- c(dbkey(x), dbkey(y))
    } else {
        dbrnms <- setdiff(ROWNAMES(x), ROWNAMES(y))
        ind <- match(ROWNAMES(x), dbrnms)
        ind <- ind[!is.na(ind)]
        ridx <- NULL
        if (!identical(ind, normalizeRowIndex(out))) {
            ridx <- ind
        }
        out <- BiocGenerics:::replaceSlots(
                           out, dbconcatKey = dbrnms,
                           indexes = list(ridx, NULL))
    }
    out
}

