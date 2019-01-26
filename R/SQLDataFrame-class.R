#' SQLDataFrame class
#' @name SQLDataFrame
#' @exportClass SQLDataFrame
#' @importFrom methods setOldClass
#' @aliases SQLDataFrame-class
#' @description NULL
setOldClass("tbl_dbi")
.SQLDataFrame <- setClass(
    "SQLDataFrame",
    slots = c(
        ## dbtable = "character",
        dbkey = "character",
        dbnrows = "integer",
        ## dbrownames = "character_OR_NULL",
        tblData = "tbl_dbi",
        indexes = "list"
        ## includeKey = "logical"
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
                         ## row.names = NULL, ## by default, read in all rows
                         col.names = NULL ## used to specify certain
                                          ## columns to read
                         ){
    ## browser()
    dbname <- tools::file_path_as_absolute(dbname)
    ## error if file does not exist!
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    ## src <- src_dbi(con, auto_disconnect = TRUE)
    ## on.exit(DBI::dbDisconnect(con))
    
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
    .SQLDataFrame(
        ## dbtable = dbtable,
        dbkey = dbkey,
        dbnrows = dbnrows,
        tblData = tbl,
        indexes = list(NULL, cidx)  ## unnamed, for row & col indexes. 
        ## includeKey = TRUE
    )
}

## FIXME: use of "dbConnect" has limits??

## now the "[,DataFrame" methods depends on `extractROWS,DataFrame`,
## should define first. which calls `extractROWS,listData(DF)`. How
## to save listData? save the whole tbl.db? or in columns?
## "show,DataFrame" calls `lapply()`.

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

#' @exportMethod dim length names dimnames
setMethod("dim", "SQLDataFrame", function(x)
{
    nr <- length(normalizeRowIndex(x))
    nc <- length(colnames(x))
    return(c(nr, nc))
})

setMethod("length", "SQLDataFrame", function(x) ncol(x) )
setMethod("names", "SQLDataFrame", function(x) colnames(x))
## used inside "[[, normalizeDoubleBracketSubscript(i, x)" 

setMethod("dimnames", "SQLDataFrame", function(x)
{
    ## colnames
    cns <- colnames(x@tblData)[-.wheredbkey(x)]
    cidx <- x@indexes[[2]]
    if (!is.null(cidx))
        cns <- cns[cidx]
    return(list(NULL, cns))
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
#' @export
## setMethod("dbtable", "SQLDataFrame", function(x) x@dbtable)
setMethod("dbtable", "SQLDataFrame", function(x)
{
    as.character(x@tblData$ops$x)
})
setGeneric("dbkey", signature = "x", function(x)
    standardGeneric("dbkey"))

#' @rdname SQLDataFrame-class
#' @aliases dbkey dbkey,SQLDataFrame
#' @export
setMethod("dbkey", "SQLDataFrame", function(x) x@dbkey )

###--------------
### show method
###--------------

## input "tbl_dbi" and output "tbl_dbi". 
#' @importFrom lazyeval interp
#' @import S4Vectors

## filter() makes sure it returns table with unique rows, no duplicate rows allowed...
.extract_tbl_rows_by_key <- function(x, key, i)
{
    ## browser()
    ## always require a dbkey(), and accommodate with multiple key columns. 
    i <- sort(unique(i))
    if (length(key) == 1) {
        keys <- pull(x, grep(key, colnames(x)))
        ## expr <- lazyeval::interp(quote(x %in% y), x = as.name(key), y = keys[i])
        out <- x %>% filter(!!as.name(key) %in% keys[i])
        ## out <- x %>% filter_(paste(key, "%in%", "c(", paste(shQuote(keys[i]), collapse=","), ")"))
        ## works, keep for now. 
    } else {
        x <- x %>% mutate(concatKey = paste(!!!syms(key), sep="\b"))
        keys <- x %>% pull(concatKey)
        out <- x %>% filter(concatKey %in% keys[i]) %>% select(-concatKey)
        ## returns "tbl_dbi" object, no realization. 

        ## keys <- x %>% transmute(concatKey = paste(!!!syms(key), sep="\b")) %>% pull(concatKey)
        ## qry <- paste("SELECT * FROM ", x$ops$x,
        ##              " WHERE (", paste(key, collapse = " || '\b' || ") ,
        ##              ") IN ($1)")
        ## out <- DBI::dbGetQuery(x$src$con, qry, param = list(keys[i]))
        ## works, but "dbGetQuery" returns the result of a query as a data frame.
    }
    return(out)
}

## Nothing special, just queried the ridx, and ordered tbl by "key+otherCols"
.extract_tbl_from_SQLDataFrame <- function(x)
{
    ridx <- ridx(x)
    tbl <- x@tblData
    if (!is.null(ridx))
        tbl <- .extract_tbl_rows_by_key(tbl, dbkey(x), ridx)
    tbl <- tbl %>% select(dbkey(x), colnames(x))
    return(tbl)
}

## .printROWS realize all ridx(x), so be careful here to only use small x.
.printROWS <- function(x, index){
    ## browser()
    tbl <- .extract_tbl_from_SQLDataFrame(x)  ## already ordered by
                                              ## "key + otherCols".
    ## i <- normalizeSingleBracketSubscript(index, x)  ## checks out-of-bound subscripts.
    ## tbl <- .extract_tbl_rows_by_key(tbl, dbkey(x), i)
    out.tbl <- tbl %>% collect()
    ridx <- normalizeRowIndex(x)
    i <- match(index, sort(unique(ridx))) 
    
    out <- as.matrix(unname(cbind(
        out.tbl[i, seq_along(dbkey(x))],
        rep("|", length(i)),
        out.tbl[i, -seq_along(dbkey(x))])))
    return(out)
}

#' @rdname SQLDataFrame-class
#' @aliases show show,SQLDataFrame-methods
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
    if (nr > 0 && nc > 0) {  ## FIXME, if nc==0, still print key column?
        ## nms <- rownames(object)  ## currently, sdf does not support rownames().
        if (nr <= (nhead + ntail + 1L)) {
            out <- .printROWS(object, normalizeRowIndex(object))
            ## if (!is.null(nms)) 
            ##     rownames(out) <- nms
        }
        else {
            sdf.head <- object[seq_len(nhead), ]
            sdf.tail <- object[tail(seq_len(nrow(object)), ntail), ]
            out <- rbind(
                .printROWS(sdf.head, ridx(sdf.head)),
                c(rep.int("...", length(dbkey(object))),".", rep.int("...", nc)),
                .printROWS(sdf.tail, ridx(sdf.tail)))
            ## rownames(out) <- S4Vectors:::.rownames(nms, nr, nhead, ntail)
        }
        classinfoFun <- function(tbl, colnames) {
            matrix(unlist(lapply(
            as.data.frame(head(tbl %>% select(colnames))),
            function(x)
            { paste0("<", classNameForDisplay(x)[1], ">") }),
            use.names = FALSE), nrow = 1,
            dimnames = list("", colnames))}
        classinfo_key <- classinfoFun(object@tblData, dbkey(object))
        classinfo_key <- matrix(
            c(classinfo_key, "|"), nrow = 1,
            dimnames = list("", c(dbkey(object), "|")))
        classinfo_other <- classinfoFun(object@tblData, colnames(object))
        out <- rbind(cbind(classinfo_key, classinfo_other), out)
        print(out, quote = FALSE, right = TRUE)
    }
})

###--------------
### coercion
###--------------

#' @rdname SQLDataFrame-class
#' @aliases coerce,SQLDataFrame,data.frame-method
#' @export
setMethod("as.data.frame", "SQLDataFrame",
          function(x, row.names = NULL, optional = FALSE, ...)
{
    tbl <- .extract_tbl_from_SQLDataFrame(x)
    out.tbl <- tbl %>% collect()
    as.data.frame(out.tbl)
    ## as.data.frame(.printROWS(aa, aa@indexes[[1]]))...
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,SQLDataFrame,DataFrame-method
#' @description the coercion method between \code{SQLDataFrame} and
#'     \code{DataFrame} objects.
#' @param from the \code{SQLDataFrame} object to be coerced.
#' @export
#' 
setAs("SQLDataFrame", "DataFrame", function(from)
{
    as(as.data.frame(from), "DataFrame")
})

## #' @name coerce
## #' @rdname SQLDataFrame-class
## #' @aliases coerce,ANY,SQLDataFrame-method
## #' @export
## setAs("ANY", "SQLDataFrame", function(from){
##     df <- as(from, "DataFrame")
##     as(df, "SQLDataFrame")
## })
