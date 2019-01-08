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
        if (!is(j, "IntegerRanges"))  ## FEATURE: keyword "filter(col1, col2, ...)"
            x <- .extractCOLS_SQLDataFrame(x, j)
        if (list_style_subsetting) 
            return(x)
    }
    if (!missing(i)) {  ## FEATURE: list(key1 = .., key2 = .., ...)
        if (is.list(i)) {
            if (!identical(dbkey(x), union(dbkey(x), names(i))))
                stop("Please use: \"", paste(dbkey(x), collapse=", "),
                     "\" as the query list name(s).")
            i <- do.call(paste, c(i[dbkey(x)], sep="\b"))
        }
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
setMethod("ROWNAMES", "SQLDataFrame", function(x)
{
    ## browser()
    ## keys <- x@tblData %>%
    ##     transmute(concat = paste(!!!syms(dbkey(x)), sep = "\b")) %>%
    ##     pull(concat)
    ## FIXME: remember to add ".0" with numeric columns. (dbl, numeric, int, fct, character...)
    keys <- x@tblData %>% select(dbkey(x)) %>% collect() %>%
        transmute(concat = paste(!!!syms(dbkey(x)), sep = "\b")) %>%
        pull(concat)
    ridx <- x@indexes[[1]]
    if (!is.null(ridx))
        keys <- keys[ridx]
    return(keys)
})

## ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db",
##                     dbtable = "state",
##                     dbkey = c("region", "population"))
## ss2 <- ss1[1:9, ]
## ROWNAMES(ss2)
## ss2[c("South\b3615.0", "West\b365.0"), ]  ## transmute(paste) %>% pull()
## ss2[c("South\b3615", "West\b365"), ]   ## collect(dbkey(x)) %>% transmute(paste) %>% pull()
### row subsetting with list object works, checks dbkey(), but doesn't matter with ordering. 
## ss1[list(population = c("3615", "365", "4981"), region = c("South", "West", "South")), ]
## ss2[list(region = c("South", "West", "West"), population = c("3615", "365", "2280")), ]
## ss1[list(region="South", population = "3615", other = "random"), ]
## Error in ss1[list(region = "South", population = "3615", other = "random"),  : 
##   Please use: "region, population" as the query list name(s).

###
## column data type for tibble & data.frame
###
## Q: when to add ".0" when concatenation. 
## ss1@tblData %>% transmute(new = paste(region, population, sep = "_"))  ## chr, dbl => chr, adding ".0"
## ss1@tblData %>% collect() %>% transmute(new = paste(region, population, sep = "_"))  ## chr, dbl => chr,no adding of ".0" after realization!!!

## con1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":MEMORY:")
## dbWriteTable(con1, "mtcars", mtcars)
## tbl(con1, "mtcars") %>% transmute(new = paste(mpg, cyl, sep="_"))  ## "tbl_dbi", added ".0" for "dbl"
## mtcars %>% transmute(new = paste(mpg, cyl, sep = "_"))  ## "tbl_df", "tbl", "data.frame"... no adding of ".0" for "dbl" columns...

## dbkey <- c("mpg", "cyl")
## tbl(con1, "mtcars") %>% transmute(new = paste(!!!syms(dbkey), sep="_")) ## efficient, but hard to cast specific column into integer. 
## tbl(con1, "mtcars") %>% select(dbkey) %>% collect() %>% transmute(new = paste(!!!syms(dbkey), sep="_")) ## less efficient, but works. 
