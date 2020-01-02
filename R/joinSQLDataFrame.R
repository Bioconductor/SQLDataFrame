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
#'     the default for \code{left_join} and \code{inner_join},
#'     ‘*_join()’ will do a natural join, using all variables with
#'     common names across the two tables. For the filtering joins
#'     \code{semi_join} ad \code{anti_join}, by default, it uses
#'     \code{dbkey(x)} as common variables for joining. See
#'     \code{?dplyr::join} for more details.
#' @param copy Only kept for S3 generic/method consistency. Used as
#'     "copy = FALSE" internally and not modifiable.
#' @param suffix A character vector of length 2 specify the suffixes
#'     to be added if there are non-joined duplicate variables in ‘x’
#'     and ‘y’. Default values are ".x" and ".y".See
#'     \code{?dplyr::join} for details.
#' @param ... Other arguments passed on to \code{*_join}
#'     methods. \code{localConn} can be passed here for one MySQL
#'     connection with write permission. Will be used only when
#'     \code{join}-ing two SQLDataFrame objects from different MySQL
#'     connections (to different MySQL databases), and neither has
#'     write permission. The situation is rare and should be
#'     avoided. See Details.
#' @return A \code{SQLDataFrame} object.
#' @export
#' @examples
#' test.db <- system.file("extdata", "test.db", package = "SQLDataFrame")
#' con <- DBI::dbConnect(dbDriver("SQLite"), dbname = test.db)
#' obj <- SQLDataFrame(conn = con,
#'                     dbtable = "state",
#'                     dbkey = c("region", "population"))
#' obj1 <- obj[1:10, 1:2]
#' obj2 <- obj[8:15, 2:3]
#' left_join(obj1, obj2)
#' inner_join(obj1, obj2)
#' semi_join(obj1, obj2)
#' anti_join(obj1, obj2)

left_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c("_x", "_y"))
{
    .doCompatibleFunction(x = x, y = y, by = by, copy = FALSE,
                          suffix = suffix, FUN = dplyr::left_join)
}

#' @name inner_join
#' @rdname joinSQLDataFrame
#' @aliases inner_join inner_join,SQLDataFrame-method
#' @export
inner_join.SQLDataFrame <- function(x, y, by = NULL, copy = FALSE,
                                   suffix = c("_x", "_y"))
{
    .doCompatibleFunction(x = x, y = y, by = by, copy = FALSE,
                          suffix = suffix, FUN = dplyr::inner_join)
}

#########################
## semi_join, anti_join (filtering joins)
#########################
## semi_join is similar to `inner_join`, but doesn't add new columns.

#' @name semi_join
#' @rdname joinSQLDataFrame
#' @aliases semi_join semi_join,SQLDataFrame-method
#' @export

semi_join.SQLDataFrame <- function(x, y, by = dbkey(x), copy = FALSE,
                                   suffix = c("_x", "_y"),
                                   ...) 
{
    .doCompatibleFunction(x = x, y = y, by = by, copy = FALSE,
                          suffix = suffix, FUN = dplyr::semi_join)
}

#' @name anti_join
#' @rdname joinSQLDataFrame
#' @aliases anti_join anti_join,SQLDataFrame-method
#' @export

anti_join.SQLDataFrame <- function(x, y, by = dbkey(x), copy = FALSE,
                                   suffix = c(".x", ".y"),
                                   ...) 
{
    .doCompatibleFunction(x = x, y = y, by = by, copy = FALSE, 
                          suffix = suffix, FUN = dplyr::anti_join)
}

