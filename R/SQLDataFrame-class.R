#' @importFrom methods setOldClass
#' 
setOldClass("tbl_dbi")
.SQLDataFrame <- setClass(
    "SQLDataFrame",
    slots = c(
        dbtable = "character",
        dbkey = "character",
        dbnrows = "integer",
        dbrownames = "character_OR_NULL",
        tblData = "tbl_dbi",
        indexes = "list",
        includeKey = "logical"
        ## elementType = "character",
        ## elementMetadata = "DataTable_OR_NULL",
        ## metadata = "list"
    )
)

###
### Constructor
###
#' @importFrom tools file_path_as_absolute
#' @import dbplyr
#' 
SQLDataFrame <- function(dbname = character(0),  ## cannot be ":memory:"
                         dbtable = character(0), ## could be NULL if
                                                 ## only 1 table
                                                 ## inside the
                                                 ## database.
                         dbkey = character(0),
                         row.names = NULL, ## by default, read in all
                                           ## rows
                         col.names = NULL ## used to specify certain
                                          ## columns to read
                         ){
    ## browser()
    dbname <- tools::file_path_as_absolute(dbname)
    ## error if file does not exist!
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)

    if (missing(dbtable)) {
        tbls <- DBI::dbListTables(con)
        if (length(tbls) == 1) {
            dbtable <- tbls
        } else {
            stop("Please specify the \"dbtable\" argument, ",
                 "which must be one of: \"",
                 paste(tbls, collapse = ", "), "\"")
        }
    }
    tbl <- con %>% tbl(dbtable)   ## ERROR if "dbtable" does not exist!
    dbnrows <- tbl %>% summarize(n = n()) %>% pull(n)
    ## col.names
    cns <- colnames(tbl)
    if (is.null(col.names)) {
        col.names <- cns
        cidx <- NULL
    } else {
        idx <- col.names %in% cns
        wmsg <- paste0(
            "The \"col.names\" of \"",
            paste(col.names[!idx], collapse = ", "),
            "\" does not exist!")
        if (!any(idx)) {
            warning(
                wmsg, " Will use \"col.names = colnames(dbtable)\"",
                " as default.")
            col.names <- cns
            cidx <- NULL
        } else {
            warning(wmsg, " Only \"",
                    paste(col.names[idx], collapse = ", "),
                    "\" will be used.")
            col.names <- col.names[idx]
            cidx <- match(col.names, cns)
        }
    }
    ## row.names
    if (!is.null(row.names)) {
        if (length(row.names) != dbnrows) {
            warning("the length of \"row.names\" is not consistent",
                    " with the dimensions of the database table. \n",
                    "  Will use \"NULL\" as default.")
            row.names <- NULL
        }
    }
    ## DBI::dbDisconnect(con)
    .SQLDataFrame(
        dbtable = dbtable,
        dbkey = dbkey,
        dbnrows = dbnrows,
        dbrownames = row.names,
        tblData = tbl,
        indexes = list(NULL, cidx),  ## unnamed, for row & col indexes. 
        includeKey = TRUE
    )
}

## FIXME: use of "dbConnect" has limits??

## now the "[,DataFrame" methods depends on `extractROWS,DataFrame`,
## should define first. which calls `extractROWS,listData(DF)`. How
## to save listData? save the whole tbl.db? or in columns?
## "show,DataFrame" calls `lapply()`.

.available_tbls <- function(x)
{
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = x)
    tbls <- DBI::dbListTables(con)
    return(tbls)
}

.validity_SQLDataFrame <- function(object)
{
    ## dbtable match
    tbls <- .available_tbls(dbname(object))
    if (! dbtable(object) %in% tbls)
        stop('"dbtable" must be one of :', tbls)    
    ## @indexes length
    idx <- object@indexes
    if (length(idx) != 2)
        stop("The indexes for \"SQLDataFrame\" should have \"length == 2\"")
}

setValidity("SQLDataFrame", .validity_SQLDataFrame)

###-------------
## accessor
###-------------

#' @exportMethod dim nrow ncol length colnames
setMethod("nrow", "SQLDataFrame", function(x)
{
    ridx <- x@indexes[[1]]
    if (is.null(ridx)) {
        nr <- x@dbnrows
    } else {
        nr <- length(ridx)
    }
    return(nr)
})
setMethod("ncol", "SQLDataFrame", function(x)
{
    cidx <- x@indexes[[2]]
    if (is.null(cidx)) {
        nc <- length(x@tblData$ops$vars)
    } else {
        nc <- length(cidx)
    }
    nc <- nc - !x@includeKey
    return(nc)
})
setMethod("dim", "SQLDataFrame", function(x) c(nrow(x), ncol(x)) )
setMethod("length", "SQLDataFrame", function(x) ncol(x) )
setMethod("colnames", "SQLDataFrame", function(x)
{
    cns <- colnames(x@tblData)
    cidx <- x@indexes[[2]]
    if (!x@includeKey) {
        if (is.null(cidx))
            cidx <- seq_len(ncol(x@tblData))
        cidx <- cidx[cidx != wheredbkey(x)]
    }
    if (!is.null(cidx))
        cns <- cns[cidx]
    return(cns)
})
setMethod("names", "SQLDataFrame", function(x) colnames(x))
## used inside "[[, normalizeDoubleBracketSubscript(i, x)" 
setMethod("rownames", "SQLDataFrame", function(x)
{
    rns <- x@dbrownames
    ridx <- x@indexes[[1]]
    if (!is.null(ridx))
        rns <- rns[ridx]
    return(rns)
})
setMethod("dimnames", "SQLDataFrame", function(x)
{
    list(rownames(x), colnames(x))
})

setGeneric("dbname", signature = "x", function(x)
    standardGeneric("dbname"))

#' @rdname SQLDataFrame-class
#' @aliases dbname dbname,SQLDataFrame
#' @description the \code{dbname} slot getter and setter for
#'     \code{SQLDataFrame} object.
#' @export
setMethod("dbname", "SQLDataFrame", function(x)
{
    x@tblData$src$con@dbname
})

setGeneric("dbtable", signature = "x", function(x)
    standardGeneric("dbtable"))

#' @rdname SQLDataFrame-class
#' @aliases dbtable dbtable,SQLDataFrame
#' @description the \code{dbtable} slot getter and setter for
#'     \code{SQLDataFrame} object.
#' @export
setMethod("dbtable", "SQLDataFrame", function(x) x@dbtable)

setGeneric("dbkey", signature = "x", function(x)
    standardGeneric("dbkey"))

#' @rdname SQLDataFrame-class
#' @aliases key key,SQLDataFrame
#' @description the \code{key} slot getter and setter for
#'     \code{SQLDataFrame} object.
#' @export
setMethod("dbkey", "SQLDataFrame", function(x) x@dbkey )

###--------------------
### "[,SQLDataFrame"
###-------------------- 
## both input & output are SQLDataFrame, by adding ridx into @indexes[[1]]
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
    xstub <- setNames(seq_along(x), names(x))
    j <- normalizeSingleBracketSubscript(j, xstub)
    if (!wheredbkey(x) %in% j) {
        j <- sort(c(j, wheredbkey(x)))
        x@includeKey <- FALSE
    }
    cidx <- x@indexes[[2]]
    if (is.null(cidx)) {
        if (!identical(j, seq_along(colnames(x@tblData))))
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
    tblData <- .extract_tbl_from_SQLDataFrame(x)
    res <- tblData %>% pull(i2)
    return(res)
})

setMethod("$", "SQLDataFrame", function(x, name) x[[name]] )
###--------------
### show method
###--------------

#' @importFrom lazyeval interp
#' @import S4Vectors

.extract_tbl_rows_by_key <- function(x, key, i)
{
    ## always require a dbkey()
    keys <- pull(x, grep(key, colnames(x)))
    expr <- lazyeval::interp(quote(x %in% y), x = as.name(key),
                             y = keys[i])
    out <- x %>% filter(expr)
    return(out)
}

.extract_tbl_from_SQLDataFrame <- function(x)
{
    ridx <- x@indexes[[1]]
    cidx <- x@indexes[[2]]
    tbl <- x@tblData
    if (!is.null(ridx))
        tbl <- .extract_tbl_rows_by_key(tbl, dbkey(x), ridx)
    if (!is.null(cidx))
        tbl <- tbl %>% select(cidx)
    return(tbl)
}

.printROWS <- function(x, index){
    tbl <- .extract_tbl_from_SQLDataFrame(x)
    i <- normalizeSingleBracketSubscript(index, x)
    tbl <- .extract_tbl_rows_by_key(tbl, dbkey(x), i)
    out.tbl <- tbl %>% collect()
    if (!x@includeKey)
        out.tbl <- out.tbl %>% select(-wheredbkey(x))
    out.key <- tbl %>% pull(wheredbkey(x))
    out.other <- as.matrix(format(
        as.data.frame(lapply(out.tbl, showAsCell), optional = TRUE)))
    out <- unname(cbind(out.key, rep("|", length(i)), out.other))
    return(out)
}

#' @export
setMethod("show", "SQLDataFrame", function (object) 
{
    ## browser()
    nhead <- get_showHeadLines()
    ntail <- get_showTailLines()
    nr <- nrow(object)
    nc <- ncol(object)
    cat(class(object), " with ", nr, ifelse(nr == 1, " row and ", 
        " rows and "), nc, ifelse(nc == 1, " column\n", " columns\n"), 
        sep = "")
    if (nr > 0 && nc > 0) {  ## FIXME, if nc==0, still print key column. 
        nms <- rownames(object)
        if (nr <= (nhead + ntail + 1L)) {
            out <- .printROWS(object, seq_len(nr))
            if (!is.null(nms)) 
                rownames(out) <- nms
        }
        else {
            out <- rbind(.printROWS(object, seq_len(nhead)),
                         rep.int("...", nc+2),
                         .printROWS(object, tail(seq_len(nr), ntail)))
            rownames(out) <- S4Vectors:::.rownames(nms, nr, nhead, ntail)
        }
        classinfo <- matrix(unlist(lapply(
            as.data.frame(head(object@tblData %>% select(colnames(object)))),
            function(x)
            { paste0("<", classNameForDisplay(x)[1], ">") }),
            use.names = FALSE), nrow = 1,
            dimnames = list("", colnames(object)))
        keyclass <- paste0("<", classNameForDisplay(b@tblData %>% pull(wheredbkey(b))), ">")
        classinfo_key <- matrix(c(keyclass, "|"), nrow = 1, dimnames = list("", c("dbkey", "")))
        out <- rbind(cbind(classinfo_key, classinfo), out)
        print(out, quote = FALSE, right = TRUE)
    }
})


###--------------
### realization? (as.data.frame(x), as(x, "DataFrame"): use %>% collect() )
###--------------
