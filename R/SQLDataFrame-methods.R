###---------------------------
### Basic methods
###--------------------------- 

#' SQLDataFrame methods
#' @name SQLDataFrame-methods
#' @description \code{head, tail}: Retrieve the first / last n rows of
#'     the \code{SQLDataFrame} object. See \code{?S4Vectors::head} for
#'     more details.
#' @param x An \code{SQLDataFrame} object.
#' @param n Number of rows.
#' @rdname SQLDataFrame-methods
#' @aliases head head,SQLDataFrame-method
#' @return \code{head, tail}: An \code{SQLDataFrame} object with
#'     certain rows.
#' @export
#' 
setMethod("head", "SQLDataFrame", function(x, n=6L)
{
    stopifnot(length(n) == 1L)
    n <- if (n < 0L) 
             max(nrow(x) + n, 0L)
         else min(n, nrow(x))
    x[seq_len(n), , drop = FALSE]
})

#' @rdname SQLDataFrame-methods
#' @aliases tail tail,SQLDataFrame-method
#' @export
#' 
## mostly copied from "tail,DataTable"
setMethod("tail", "SQLDataFrame", function(x, n=6L)
{
    stopifnot(length(n) == 1L)
    nrx <- nrow(x)
    n <- if (n < 0L) 
             max(nrx + n, 0L)
         else min(n, nrx)
    sel <- as.integer(seq.int(to = nrx, length.out = n))
    ans <- x[sel, , drop = FALSE]
    ans    
})

#' @description \code{dim, dimnames, length, names}: Retrieve the
#'     dimension, dimension names, number of columns and colnames of
#'     SQLDataFrame object.
#' @rdname SQLDataFrame-methods
#' @aliases dim dim,SQLDataFrame-method
#' @return \code{dim}: interger vector
#' @export
#' @examples
#' 
#' ##################
#' ## basic methods
#' ##################
#' 
#' dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
#' obj <- SQLDataFrame(dbname = dbname, dbtable = "state", dbkey = "state")
#' dim(obj)
#' dimnames(obj)
#' length(obj)
#' names(obj)

setMethod("dim", "SQLDataFrame", function(x)
{
    nr <- length(normalizeRowIndex(x))
    nc <- length(colnames(x))
    return(c(nr, nc))
})

#' @rdname SQLDataFrame-methods
#' @aliases dimnames dimnames,SQLDataFrame-method
#' @return \code{dimnames}: A list of character vectors.
#' @export

setMethod("dimnames", "SQLDataFrame", function(x)
{
    cns <- colnames(x@tblData)[-.wheredbkey(x)]
    cidx <- x@indexes[[2]]
    if (!is.null(cidx))
        cns <- cns[cidx]
    return(list(NULL, cns))
})

#' @rdname SQLDataFrame-methods
#' @aliases length length,SQLDataFrame-method
#' @return \code{length}: An integer
#' @export

setMethod("length", "SQLDataFrame", function(x) ncol(x) )

#' @rdname SQLDataFrame-methods
#' @aliases names length,SQLDataFrame-method
#' @return \code{names}: A character vector
#' @export

setMethod("names", "SQLDataFrame", function(x) colnames(x))
## used inside "[[, normalizeDoubleBracketSubscript(i, x)" 


###--------------------
### "[,SQLDataFrame"
###-------------------- 
.extractROWS_SQLDataFrame <- function(x, i)
{
    i <- normalizeSingleBracketSubscript(i, x)
    ridx <- x@indexes[[1]]
    if (is.null(ridx)) {
        if (! identical(i, seq_len(x@dbnrows)))
            x@indexes[[1]] <- i
    } else {
        x@indexes[[1]] <- x@indexes[[1]][i]
    }
    return(x)
}
setMethod("extractROWS", "SQLDataFrame", .extractROWS_SQLDataFrame)

#' @importFrom stats setNames
.extractCOLS_SQLDataFrame <- function(x, j)
{
    xstub <- setNames(seq_along(x), names(x))
    j <- normalizeSingleBracketSubscript(j, xstub)
    cidx <- x@indexes[[2]]
    if (is.null(cidx)) {
        if (!identical(j, seq_along(colnames(x))))
            x@indexes[[2]] <- j
    } else {
            x@indexes[[2]] <- x@indexes[[2]][j]
    }
    return(x)
}

#' @description \code{[i, j]} supports subsetting by \code{i} (for
#'     row) and \code{j} (for column) and respects ‘drop=FALSE’.
#' @rdname SQLDataFrame-methods
#' @param i Row subscript. Could be numeric / character / logical
#'     values, a named list of key values, and \code{SQLDataFrame},
#'     \code{data.frame}, \code{tibble} objects.
#' @param j Column subscript.
#' @param drop Whether to drop with reduced dimension. Default is
#'     TRUE.
#' @return A \code{SQLDataFrame} object or vector with realized column
#'     values (with single column subsetting and default
#'     \code{drop=TRUE}. )
#' @importFrom tibble tibble 
#' @export
#' @examples
#'
#' obj1 <- SQLDataFrame(dbname = dbname, dbtable = "state",
#'                      dbkey = c("region", "population"))

#' ###############
#' ## subsetting
#' ###############
#'
#' obj[1]
#' obj["region"]
#' obj$region
#' obj[]
#' obj[,]
#' obj[NULL, ]
#' obj[, NULL]
#'
#' ## by numeric / logical / character vectors
#' obj[1:5, 2:3]
#' obj[c(TRUE, FALSE), c(TRUE, FALSE)]
#' obj[c("Alabama", "South Dakota"), ]
#' obj1[c("South\b3615.0", "West\b3559.0"), ]
#' ### Remeber to add `.0` trailing for numeric values. If not sure,
#' ### check `ROWNAMES()`.
#'
#' ## by SQLDataFrame
#' obj_sub <- obj[sample(10), ]
#' obj[obj_sub, ]
#'
#' ## by a named list of key column values (or equivalently data.frame /
#' ## tibble)
#' obj[data.frame(state = c("Colorado", "Arizona")), ]
#' obj[tibble::tibble(state = c("Colorado", "Arizona")), ]
#' obj[list(state = c("Colorado", "Arizona")), ]
#' obj1[list(region = c("South", "West"),
#'           population = c("3615.0", "365.0")), ]
#' ### remember to add the '.0' trailing for numeric values. If not sure,
#' ### check `ROWNAMES()`.


setMethod("[", "SQLDataFrame", function(x, i, j, ..., drop = TRUE)
{
    if (!isTRUEorFALSE(drop)) 
        stop("'drop' must be TRUE or FALSE")
    if (length(list(...)) > 0L) 
        warning("parameters in '...' not supported")
    list_style_subsetting <- (nargs() - !missing(drop)) < 3L
    if (list_style_subsetting || !missing(j)) {
        if (list_style_subsetting) {
            if (!missing(drop)) 
                warning("'drop' argument ignored by list-style subsetting")
            if (missing(i)) 
                return(x)
            j <- i
        }
        if (!is(j, "IntegerRanges")) {
            if (is.character(j) && length(j) == 1 && j %in% dbkey(x)) {
                res <- .extract_tbl_from_SQLDataFrame(x) %>% select(j) %>% pull()
                if (!drop)
                    warning("'drop' argument ignored by subsetting only key columns")
                return(res)
            } else {
                x <- .extractCOLS_SQLDataFrame(x, j)
            }
        }
        if (list_style_subsetting) 
            return(x)
    }
    if (!missing(i)) { 
        x <- extractROWS(x, i)
    }
    if (missing(drop)) 
        drop <- nrow(x) & ncol(x) == 1L ## if nrow(x)==0, return the
                                        ## SQLDataFrame with 0 rows
                                        ## and 1 column(s)
    if (drop) {
        if (ncol(x) == 1L) 
            return(x[[1L]])
        if (nrow(x) == 1L) 
            return(as(x, "list"))
    }
    x  
})

#' @rdname SQLDataFrame-methods
#' @importFrom methods is as callNextMethod
#' @export
setMethod("[", signature = c("SQLDataFrame", "SQLDataFrame", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    if (!identical(dbkey(x), dbkey(i)))
        stop("The dbkey() must be same between \"", deparse(substitute(x)),
             "\" and \"", deparse(substitute(i)), "\".", "\n")
    i <- ROWNAMES(i)
    callNextMethod()
})

#' @rdname SQLDataFrame-methods
#' @export
setMethod("[", signature = c("SQLDataFrame", "list", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    if (!identical(dbkey(x), union(dbkey(x), names(i))))
        stop("Please use: \"", paste(dbkey(x), collapse=", "),
             "\" as the query list name(s).")
    i <- do.call(paste, c(i[dbkey(x)], sep="\b"))
    callNextMethod()
})

###--------------------
### "[[,SQLDataFrame" (do realization for single column only)
###--------------------

#' @rdname SQLDataFrame-methods
#' @export
setMethod("[[", "SQLDataFrame", function(x, i, j, ...)
{
    dotArgs <- list(...)
    if (length(dotArgs) > 0L) 
        dotArgs <- dotArgs[names(dotArgs) != "exact"]
    if (!missing(j) || length(dotArgs) > 0L) 
        stop("incorrect number of subscripts")
    ## extracting key col value 
    if (is.character(i) && length(i) == 1 && i %in% dbkey(x)) {
        res <- .extract_tbl_from_SQLDataFrame(x) %>% select(i) %>% pull()
        return(res)
    }
    i2 <- normalizeDoubleBracketSubscript(
        i, x,
        exact = TRUE,  ## default
        allow.NA = TRUE,
        allow.nomatch = TRUE)
    ## "allow.NA" and "allow.nomatch" is consistent with
    ## selectMethod("getListElement", "list") <- "simpleList"
    if (is.na(i2))
        return(NULL)
    tblData <- .extract_tbl_from_SQLDataFrame(x) %>% select(- !!dbkey(x))
    res <- tblData %>% pull(i2)
    return(res)
})

#' @rdname SQLDataFrame-methods
#' @param name column name to be extracted by \code{$}.
#' @export
setMethod("$", "SQLDataFrame", function(x, name) x[[name]] )

#####################
### filter & mutate
#####################

#' @description Use \code{filter()} to choose rows/cases where
#'     conditions are true.
#' @rdname SQLDataFrame-methods
#' @aliases filter filter,SQLDataFrame-method
#' @param .data A \code{SQLDataFrame} object.
#' @param ... additional arguments to be passed.
#' \itemize{
#' \item{\code{filter()}: }{Logical predicates defined in terms of the
#'     variables in ‘.data’. Multiple conditions are combined with
#'     ‘&’. Only rows where the condition evaluates to ‘TRUE’ are
#'     kept. See \code{?dplyr::filter} for more details.}
#' \item{\code{mutate()}: }{Name-value pairs of expressions, each with
#'     length 1 or the same length as the number of rows in the group
#'     (if using ‘group_by()’) or in the entire input (if not using
#'     groups). The name of each argument will be the name of a new
#'     variable, and the value will be its corresponding value. Use a
#'     ‘NULL’ value in ‘mutate’ to drop a variable.  New variables
#'     overwrite existing variables of the same name.}}
#' @return \code{filter}: A \code{SQLDataFrame} object with subset
#'     rows of the input SQLDataFrame object matching conditions.
#' @export
#' @examples
#' 
#' ###################
#' ## filter & mutate 
#' ###################
#'
#' library(dplyr)
#' obj %>% filter(region == "West" & size == "medium")
#' obj1 %>% filter(region == "West" & population > 10000)
#' 
#' obj %>% mutate(p1 = population / 10)
#' obj %>% mutate(s1 = size)

filter.SQLDataFrame <- function(.data, ...)
{
    tbl <- .extract_tbl_from_SQLDataFrame(.data)
    temp <- dplyr::filter(tbl, ...)

    rnms <- temp %>%
        transmute(concat = paste(!!!syms(dbkey(.data)), sep = "\b")) %>%
        pull(concat)
    idx <- match(rnms, ROWNAMES(.data))

    if (!identical(idx, normalizeRowIndex(.data))) {
        if (!is.null(ridx(.data))) {
            .data@indexes[[1]] <- ridx(.data)[idx]
        } else {
            .data@indexes[[1]] <- idx
        }
    }
    return(.data)
}

#' @description \code{mutate()} adds new columns and preserves
#'     existing ones; It also preserves the number of rows of the
#'     input. New variables overwrite existing variables of the same
#'     name.
#' @rdname SQLDataFrame-methods
#' @aliases mutate mutate,SQLDataFrame-methods
#' @return \code{mutate}: A SQLDataFrame object.
#' @export
#' 
mutate.SQLDataFrame <- function(.data, ...)
{
    if (is(.data@tblData$ops, "op_double") | is(.data@tblData$ops, "op_mutate")) {
        con <- .con_SQLDataFrame(.data)
        tbl <- .data@tblData
    } else {
        dbname <- tempfile(fileext = ".db")
        con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
        aux <- .attach_database(con, dbname(.data))
        auxSchema <- in_schema(aux, ident(dbtable(.data)))
        tbl <- tbl(con, auxSchema)
    }
    tbl_out <- dplyr::mutate(tbl, ...)
        
    BiocGenerics:::replaceSlots(.data, tblData = tbl_out)
}

