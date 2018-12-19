###---------------------------
### "head/tail,SQLDataFrame"
###--------------------------- 

## mostly copied from "head,DataTable"
#' @export
setMethod("head", "SQLDataFrame", function(x, n=6L)
{
    stopifnot(length(n) == 1L)
    n <- if (n < 0L) 
             max(nrow(x) + n, 0L)
         else min(n, nrow(x))
    x[seq_len(n), , drop = FALSE]
})

## mostly copied from "tail,DataTable"
#' @export
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

.extractCOLS_SQLDataFrame <- function(x, j)
{
    ## browser()
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

setMethod("[", "SQLDataFrame", function(x, i, j, ..., drop = TRUE)
{
    ## browser()
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
        if (!is(j, "IntegerRanges"))
            x <- .extractCOLS_SQLDataFrame(x, j)
        if (list_style_subsetting) 
            return(x)
    }
    if (!missing(i))
        x <- extractROWS(x, i)
    if (missing(drop)) 
        drop <- ncol(x) == 1L
    if (drop) {
        if (ncol(x) == 1L) 
            return(x[[1L]])
        if (nrow(x) == 1L) 
            return(as(x, "list"))
    }
    x  
})

###--------------------
### "[[,SQLDataFrame" (do realization for single column only)
###--------------------
setMethod("[[", "SQLDataFrame", function(x, i, j, ...)
{
    ## browser()
    dotArgs <- list(...)
    if (length(dotArgs) > 0L) 
        dotArgs <- dotArgs[names(dotArgs) != "exact"]
    if (!missing(j) || length(dotArgs) > 0L) 
        stop("incorrect number of subscripts")
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

setMethod("$", "SQLDataFrame", function(x, name) x[[name]] )
