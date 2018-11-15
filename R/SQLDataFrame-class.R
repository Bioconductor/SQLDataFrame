#' @importFrom methods setOldClass
#' 
setOldClass("tbl_dbi")
.SQLDataFrame <- setClass(
    "SQLDataFrame",
    slots = c(
        ## dbname = "character",
        dbtable = "character",
        dbkey = "character",
        rownames = "character_OR_NULL",
        colnames = "character_OR_NULL",
        nrows = "integer",
        tblData = "tbl_dbi" ##,
        ## elementType = "character",
        ## elementMetadata = "DataTable_OR_NULL",
        ## metadata = "list"
    )
)

###
### Constructor
###
#' @importFrom tools file_path_as_absolute
#' 
SQLDataFrame <- function(dbname = character(0),
                         dbtable = character(0),
                         dbkey = character(0),
                         row.names = NULL,
                         col.names = NULL ##, 
                         ## check.names = TRUE
                         ){
    ## browser()
    ## checks
    dbname <- tools::file_path_as_absolute(dbname)  ## error if file
                                                    ## does not exist!
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    tbl <- con %>% tbl(dbtable)   ## ERROR if "dbtable" does not exist!
    ## keys <- pull(tbl, grep(key, colnames(tbl))) ## save key values,
                                                 ## to avoid
                                                 ## inconvenience in
                                                 ## %>% language.
    nrows <- tbl %>% summarize(n = n()) %>% pull(n)
    cns <- colnames(tbl)
    if (is.null(col.names)) {
        col.names <- cns
    } else {
        idx <- col.names %in% cns
        if (!all(idx)) {
            warning("These ", sum(!idx), " columns (",
                    paste(col.names[!idx], collapse = ", "),
                    ") does not exist and would be removed!")
            col.names <- col.names[idx]
        }
    }
    ## DBI::dbDisconnect(con)
    .SQLDataFrame(
        ## dbname = dbname,
        dbtable = dbtable,
        dbkey = dbkey,
        nrows = nrows,
        tblData = tbl,
        rownames = row.names,
        colnames = col.names
    )
}

## FIXME: use of "dbConnect" has limits??

## now the "[,DataFrame" methods depends on `extractROWS,DataFrame`,
## should define first. which calls `extractROWS,listData(DF)`. How
## to save listData? save the whole tbl.db? or in columns?
## "show,DataFrame" calls `lapply()`.

## ?? easiest way to do is to save individual columns from the tbl_dbi
## into the "listData()", so that "dim", "ncol" works, and
## "extractROWS()" works, so that "show" method works...



### no need to define "show" method, as long as "[" works, show method works. 

## setMethod("show", signature = "SQLDataFrame", function(object)
## {
##     con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname(object))
##     tbl <- con %>% tbl(dbtable(object))
## })

.validity_dbtable <- function(object)
{
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname(object))
    tbls <- DBI::dbListTables(con)
    if (! dbtable(object) %in% tbls)
        stop('"dbtable" must be one of :', tbls)    
}

setValidity("SQLDataFrame", .validity_dbtable)

###-------------
## accessor
###-------------

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

#' @exportMethod dim nrow ncol length colnames
setMethod("dim", "SQLDataFrame", function(x) c(x@nrows, length(a@tblData$ops$vars)) )
setMethod("nrow", "SQLDataFrame", function(x) dim(x)[1L] )
setMethod("ncol", "SQLDataFrame", function(x) dim(x)[2L] )
setMethod("length", "SQLDataFrame", function(x) ncol(x) )
setMethod("colnames", "SQLDataFrame", function(x) colnames(x@tblData) )

###--------------------
### "[,SQLDataFrame"
###-------------------- 

#' @importFrom lazyeval interp
.extractROWS_SQLDataFrame <- function(x, i)
{
    ## browser()
    i <- normalizeSingleBracketSubscript(
        i, x, exact = FALSE, allow.NAs = TRUE, as.NSBS = FALSE)
    rownames <- rownames(x)[i]
    if (!is.null(rownames))
        rownames <- make.unique(rownames)
    keys <- pull(x@tblData, grep(dbkey(x), colnames(x@tblData)))
    expr <- lazyeval::interp(quote(x %in% y), x = as.name(dbkey(x)), y = keys[i])
    filter(x@tblData, expr)
}
## FIXME: now returns "tbl_dbi" object, should we return
## "SQLDataFrame" ? So that we need to save extra slots for column and
## row indexes as lazy index for subsetting. 

setMethod("extractROWS", "SQLDataFrame", .extractROWS_SQLDataFrame)

setMethod("[", "SQLDataFrame", function(x, i, j, ...)
{
    
})

setMethod("[[", "SQLDataFrame", function(x, i, j, ...)
{
    ## "dotArgs" etc... are copied from "[[,DataTable"
    dotArgs <- list(...)
    if (length(dotArgs) > 0L) 
        dotArgs <- dotArgs[names(dotArgs) != "exact"]
    if (!missing(j) || length(dotArgs) > 0L) 
        stop("incorrect number of subscripts")
    x@tblData %>% select(colnames(x@tblData)[i])
    ## FIXME: need to return a `SQLDataFrame` object, instead of
    ## `tbl_dbi`. So need the "show,SQLDataFrame" to work first.
})

###--------------
### show method
###--------------

## 1. only print "character" value of each column
printROWS <- function(x, index){
    out.db <- .extractROWS_SQLDataFrame(x, index)
    out.tbl <- out.db %>% collect()
    out <- as.matrix(format(as.data.frame(lapply(out.tbl, showAsCell), optional = TRUE)))
    ## could add unname(as.matrix()) to remove column names here. 
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
    if (nr > 0 && nc > 0) {
        nms <- rownames(object)
        if (nr <= (nhead + ntail + 1L)) {
            out <- printROWS(object, seq_len(nr))
            if (!is.null(nms)) 
                rownames(out) <- nms
        }
        else {
            out <- rbind(printROWS(object, seq_len(nhead)),
                         rep.int("...", nc),
                         printROWS(object, tail(seq_len(nr), ntail)))
            rownames(out) <- S4Vectors:::.rownames(nms, nr, nhead, ntail)
        }
        classinfo <- matrix(unlist(lapply(as.data.frame(head(object@tblData)), function(x)
        { paste0("<", classNameForDisplay(x)[1], ">") }),
        use.names = FALSE), nrow = 1,
        dimnames = list("", colnames(object)))

        out <- rbind(classinfo, out)
        print(out, quote = FALSE, right = TRUE)
    }
})


###--------------
### realization? (as.data.frame(x), as(x, "DataFrame"): use %>% collect() )
###--------------
