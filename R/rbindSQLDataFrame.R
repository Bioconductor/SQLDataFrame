.rbind_SQLDataFrame <- function(..., deparse.level = 1)
{
    objects <- list(...)
    ## check consistency of dbkey(), colnames()
    keys <- lapply(objects, dbkey)
    if (length(unique(keys)) != 1)
        stop("Input SQLDataFrame objects must have identical dbkey()!")
    cnms <- lapply(objects, colnames)
    if (length(unique(cnms)) != 1 )
        stop("Input SQLDataFrame objects must have identical columns!")
    dbkey <- keys[[1]]
    cnm <- cnms[[1]]
    rnms_final <- do.call(c, lapply(objects, ROWNAMES))

    ## pairwise "union" with multiple input. 
    out <- union(objects[[1]], objects[[2]])
    objects <- objects[-seq_len(2)]
    repeat{
        if(length(objects) == 0) break
        out <- union(out, objects[[1]])
        objects <- objects[-1]
    }

    idx <- match(rnms_final, out@dbconcatKey)
    out@indexes[[1]] <- idx
    return(out)
}

#' rbind of \code{SQLDataFrame} objects
#' @name rbind
#' @rdname rbindSQLDataFrame
#' @aliases rbind rbind,SQLDataFrame-method
#' @description Performs rbind on \code{SQLDataFrame} objects.
#' @param ... One or more \code{SQLDataFrame} objects. These can be
#'     given as named arguments.
#' @param deparse.level See ‘?base::cbind’ for a description of this
#'     argument.
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
#' ## rbind
#' res_rbind <- rbind(obj1_sub, obj2_sub)
#' res_rbind
#' dim(res_rbind)

setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)
