.union_multi_SQLDataFrame <- function(..., deparse.level = 1)
{
    objects <- list(...)
    ## check consistency of dbkey(), colnames()
    keys <- lapply(objects, dbkey)
    if (length(unique(keys)) != 1)
        stop("Input SQLDataFrame objects must have identical dbkey()!")
    cnms <- lapply(objects, colnames)
    if (length(unique(cnms)) != 1 )
        stop("Input SQLDataFrame objects must have identical columns!")
    pids <- lapply(objects, pid)
    if (length(unique(pids)) != 1)
        stop("Input SQLDataFrame objects must have identical partitionID if exists!")

    new_dbkey <- keys[[1]]
    new_pid <- unique(pids)[[1]]
    
    ## 1. union(x, y) recursively for all tblData()
    ## pairwise "union" with multiple input.

    if (is(connSQLDataFrame(objects[[1]]), "BigQueryConnection")) {
        stop("Union is not supported for the SQLDataFrame with BigQueryConenction")
        ## FUN <- dbplyr:::union_all.tbl_lazy
    } else {
        FUN <- dbplyr:::union.tbl_lazy
    }
    new_tblData <- FUN(tblData(objects[[1]]), tblData(objects[[2]]))
    objects_rep <- objects[-seq_len(2)]
    repeat{
        if(length(objects_rep) == 0) break
        new_tblData <- FUN(new_tblData, tblData(objects_rep[[1]]))
        objects_rep <- objects_rep[-1]
    }

    ## @keyData
    new_keyData <- .update_keyData(new_tblData, new_dbkey, new_pid)
    ## @pidRle
    new_pidRle <- .update_pidRle(new_keyData, new_pid)
    ## @dim, @dimnames
    new_nr <- new_keyData %>% ungroup %>% summarize(n = n())
    new_nr <- collectm(new_nr) %>% pull(n) %>% as.integer
    new_nc <- ncol(new_tblData) - length(new_dbkey)
    new_cns <- setdiff(colnames(new_tblData), new_dbkey)
    ## for "filter", no need to honor the existing ridx(x), so will
    ## reset as NULL.
    BiocGenerics:::replaceSlots(objects[[1]], tblData = new_tblData,
                                keyData = new_keyData,
                                pidRle = new_pidRle,
                                dim = c(new_nr, new_nc),
                                dimnames = list(NULL, new_cns),
                                ridx = NULL)
}


setGeneric("rbindUniq", signature = "...", function(..., deparse.level = 1)
    standardGeneric("rbindUniq"))

#' unique rbind of \code{SQLDataFrame} objects
#' @name rbindUniq
#' @rdname rbindSQLDataFrame
#' @aliases rbindUniq rbindUniq,SQLDataFrame-method
#' @description Performs multiple union on \code{SQLDataFrame}
#'     objects. The result will be sorted by the \code{dbkey} columns.
#' @param ... One or more \code{SQLDataFrame} objects. These can be
#'     given as named arguments.
#' @param deparse.level See ‘?base::cbind’ for a description of this
#'     argument.
#' @details \code{rbindUnique} supports aggregation of SQLDataFrame
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
#' res_rbind <- rbindUniq(obj_sub1, obj_sub2)  ## sorted
#' res_rbind
#' dim(res_rbind)

setMethod("rbindUniq", signature = "SQLDataFrame", .union_multi_SQLDataFrame)
