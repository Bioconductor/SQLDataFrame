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
    ## browser()
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
        if (!is(j, "IntegerRanges"))  ## FEATURE: keyword "select(col1, col2, ...)"
            x <- .extractCOLS_SQLDataFrame(x, j)
        if (list_style_subsetting) 
            return(x)
    }
    if (!missing(i)) { 
        x <- extractROWS(x, i)
    }
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

setMethod("[", signature = c("SQLDataFrame", "SQLDataFrame", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    if (!identical(dbkey(x), dbkey(i)))
        stop("The dbkey() must be same between \"", deparse(substitute(x)),
             "\" and \"", deparse(substitute(i)), "\".", "\n")
    i <- ROWNAMES(i)
    callNextMethod()
})

## setMethod("[", signature = c("SQLDataFrame", "sql", "ANY"),
##           function(x, i, j, ..., drop = TRUE)
## {
##     browser()
##     tbl <- .extract_tbl_from_SQLDataFrame(x)
##     tbl %>% sql(i)
##     ## You can also use pass raw SQL if you want a more sophisticated query
##     ## src %>% tbl(sql("SELECT * FROM mtcars WHERE cyl = 8"))
##     tbl$src %>% tbl(sql)
## })

setMethod("[", signature = c("SQLDataFrame", "list", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    ## browser()
    if (!identical(dbkey(x), union(dbkey(x), names(i))))
        stop("Please use: \"", paste(dbkey(x), collapse=", "),
             "\" as the query list name(s).")
    i <- do.call(paste, c(i[dbkey(x)], sep="\b"))
    callNextMethod()
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

setMethod("$", "SQLDataFrame", function(x, name) x[[name]] )


###--------------------
### "ROWNAMES,SQLDataFrame" 
###--------------------
## for primary key, so that row subsetting with character vector could
## work. Pass to "NSBS,character", then
## "normalizeSingleBracketSubscript"
#' @export
## #' @examples
## #' b <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "colData", dbkey = "sampleID") 
## #' ROWNAMES(b)
## #' ROWNAMES(b[c(TRUE, FALSE), ])
#' b[letters[10:15], ]
## setMethod("ROWNAMES", "SQLDataFrame", function(x)
## {
##     ## browser()
##     ## keys <- x@tblData %>%
##     ##     transmute(concat = paste(!!!syms(dbkey(x)), sep = "\b")) %>%
##     ##     pull(concat)
##     ## FIXME: remember to add ".0" with numeric columns. (dbl, numeric, int, fct, character...)
##     tbl <- .extract_tbl_from_SQLDataFrame(x)
##     keys <- tbl %>% select(dbkey(x)) %>% collect() %>% 
##         transmute(concat = paste(!!!syms(dbkey(x)), sep = "\b")) %>%
##         pull(concat)
##     ridx <- ridx(x)
##     if (!is.null(ridx)) {
##         i <- match(ridx, sort(unique(ridx)))
##         keys <- keys[i]
##     }
##     return(keys)
## })
## setMethod("ROWNAMES", "SQLDataFrame", function(x) concatKey(x))

## #' @param ... Logical predicates defined in terms of the variables in
## #'     ‘.data’. Multiple conditions are combined with ‘&’. Only rows
## #'     where the condition evaluates to ‘TRUE’ are kept.
## #' @export S3mehod??
## setMethod("filter", signature = "SQLDataFrame", function(.data, ...)
## {
filter.SQLDataFrame <- function(.data, ...)
{
    ## browser()
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
## want something like this to work:
### `dplyr` functions: 
## ss1[filter(col1 == "", col2 != "", col3 %in% c(...)), ] pass directly to: 
## ss1@tblData %>% filter(col1 == "", col2 != "", col3 %in% c(...))

### raw sql commands:
## ss1[sql("SELECT * FROM table WHERE col1 == "", col2 != "", ...")]

mutate.SQLDataFrame <- function(.data, ...)
{
    browser()
    ## FIXME: refer to ".join_union_prepare", check if already a con. "op_mutate" or "op_double", if yes, open that temp connection. Otherwise, open a new connection.
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
