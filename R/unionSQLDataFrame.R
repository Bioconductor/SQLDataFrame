.union_SQLDataFrame <- function(x, y, copy = FALSE, ...)
{
    conx <- connSQLDataFrame(x)
    if (is(conx, "BigQueryConnection")) {
        .doCompatibleFunction(x = x, y = y, ..., 
                              FUN = dbplyr:::union_all.tbl_lazy)
    } else {
        .doCompatibleFunction(x = x, y = y, ..., 
                              FUN = dbplyr:::union.tbl_lazy)
    }
}

#' Union of \code{SQLDataFrame} objects
#' @name union
#' @aliases union union,SQLDataFrame,SQLDataFrame-method
#' @rdname unionSQLDataFrame
#' @description Performs union operations on \code{SQLDataFrame}
#'     objects.
#' @param x A \code{SQLDataFrame} object.
#' @param y A \code{SQLDataFrame} object.
#' @param copy Only kept for S3 generic/method consistency. Used as
#'     "copy = FALSE" internally and not modifiable.
#' @param ... Other arguments passed on to methods.
#' @details The \code{union} function supports aggregation of
#'     SQLDataFrame objects from same connections.
#'
#'     For SQLite and MySQL connections, \code{union} drops all
#'     duplicate and return only distinct rows.
#'
#'     For BigQuery connections, \code{union} only drops duplicate
#'     rows inside the to-be-joined SQLDataFrame objects, but
#'     preserves the overlapping rows between the input SQLDataFrame
#'     objects.
#' @return A \code{SQLDataFrame} object.
#' @export
#' @examples
#' test.db <- system.file("extdata/test.db", package = "SQLDataFrame")
#' con <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
#' obj <- SQLDataFrame(conn = con,
#'                     dbtable = "state",
#'                     dbkey = c("region", "population"))
#' obj_sub1 <- obj[1:10, 2:3]
#' obj_sub2 <- obj[8:15, 2:3]
#'
#' ## union
#' res_union <- union(obj_sub1, obj_sub1)  ## sorted
#' res_union
#' dim(res_union)

setMethod("union", signature = c("SQLDataFrame", "SQLDataFrame"), .union_SQLDataFrame)
