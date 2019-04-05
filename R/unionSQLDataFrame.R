.union_SQLDataFrame <- function(x, y, copy = FALSE)
{
    ## browser()
    out <- .doCompatibleFunction(x, y, copy = copy,
                                 FUN = dbplyr:::union.tbl_lazy)  ## dbplyr:::union.tbl_lazy
    ## @dbconcatKey
    rnms <- unique(c(ROWNAMES(x), ROWNAMES(y)))
    tt <- as.data.frame(do.call(rbind, strsplit(rnms, split = "\b")),
                        stringsAsFactors = FALSE)
    cls <- out@tblData %>% head %>% select(dbkey(x)) %>% as.data.frame() %>% sapply(class)
    for (i in seq_len(length(tt))) class(tt[,i]) <- unname(cls)[i]
    od <- do.call(order, tt)
    dbrnms <- rnms[od]
    
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}

#' Union of \code{SQLDataFrame} objects
#' @name rbind
#' @rdname unionSQLDataFrame
#' @aliases union union,SQLDataFrame-method
#' @description Performs union operations on \code{SQLDataFrame}
#'     objects.
#' @param x A \code{SQLDataFrame} object.
#' @param y A \code{SQLDataFrame} object.
#' @return A \code{SQLDataFrame} object.
#' @export
#' @examples
#' obj1 <- SQLDataFrame(dbname = "inst/extdata/test.db",
#'                      dbtable = "state",
#'                      dbkey = c("region", "population"))
#' obj2 <- SQLDataFrame(dbname = "inst/extdata/test1.db",
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

