#' SQLDataFrame class
#' @name SQLDataFrame
#' @aliases class:SQLDataFrame SQLDataFrame-class
#' @rdname SQLDataFrame-class
#' @description NULL
#' @exportClass SQLDataFrame
#' @importFrom methods setOldClass new
## add other connections. 
setOldClass(c("tbl_SQLiteConnection", "tbl_dbi", "tbl_sql", "tbl_lazy", "tbl"))
.SQLDataFrame <- setClass(
    "SQLDataFrame",
    slots = c(
        dbkey = "character",
        dbnrows = "integer",  
        tblData = "tbl_dbi",
        indexes = "list",
        dbconcatKey = "character" 
    )
)

### Constructor
#' @rdname SQLDataFrame-class
#' @description \code{SQLDataFrame} constructor, slot getters, show
#'     method and coercion methods to \code{DataFrame} and
#'     \code{data.frame} objects.
#' @param dbname A character string for the database file path.
#' @param dbtable A character string for the table name in that
#'     database. If not provided and there is only one table
#'     available, it will be read in by default.
#' @param dbkey A character vector for the name of key columns that
#'     could uniquely identify each row of the database table.
#' @param col.names A character vector specifying the column names you
#'     want to read into the \code{SQLDataFrame}.
#' @return A \code{SQLDataFrame} object.
#' @export
#' @importFrom tools file_path_as_absolute
#' @importFrom RSQLite SQLite
#' @import dbplyr
#' @examples
#' 
#' ## constructor
#' dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
#' obj <- SQLDataFrame(dbname = dbname, dbtable = "state",
#'                     dbkey = "state")
#' obj
#' obj1 <- SQLDataFrame(dbname = dbname, dbtable = "state",
#'                      dbkey = c("region", "population"))
#' obj1
#'
#' ## slot accessors
#' dbname(obj)
#' dbtable(obj)
#' dbkey(obj)
#' dbkey(obj1)
#'
#' dbconcatKey(obj)
#' dbconcatKey(obj1)
#'
#' ## ROWNAMES
#' ROWNAMES(obj[sample(10, 5), ])
#' ROWNAMES(obj1[sample(10, 5), ])
#'
#' ## coercion
#' as.data.frame(obj)
#' as(obj, "DataFrame")
#' 
#' 
#' ## dbkey replacement
#' dbkey(obj) <- c("region", "population")
#' obj

SQLDataFrame <- function(dbname = character(0),  ## cannot be ":memory:"
                         dbtable = character(0), ## could be NULL if
                                                 ## only 1 table
                                                 ## inside the
                                                 ## database.
                         dbkey = character(0),
                         col.names = NULL
                         ){
    dbname <- tools::file_path_as_absolute(dbname)
    ## error if file does not exist!
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    ## src <- src_dbi(con, auto_disconnect = TRUE)
    ## on.exit(DBI::dbDisconnect(con))
    tbls <- DBI::dbListTables(con)
    
    if (missing(dbtable)) {
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

    ## ridx
    ridx <- NULL
    ridxTableName <- paste0(dbtable, "_ridx")
    if (ridxTableName %in% tbls) {
        ridx <- dbReadTable(con, ridxTableName)$ridx
    }
    
    ## concatKey
    ## keyDF <- DataFrame(select(tbl, dbkey))  ## attemp for DF format
    concatKey <- tbl %>%
        mutate(concatKey = paste(!!!syms(dbkey), sep="\b")) %>%
        pull(concatKey)

    .SQLDataFrame(
        dbkey = dbkey,
        dbnrows = dbnrows,
        tblData = tbl,
        indexes = list(ridx, cidx),
        dbconcatKey = concatKey
    )
}

## FIXME: use of "dbConnect" has limits??

## now the "[,DataFrame" methods depends on `extractROWS,DataFrame`,
## should define first. which calls `extractROWS,listData(DF)`. How
## to save listData? save the whole tbl.db? or in columns?
## "show,DataFrame" calls `lapply()`.

.validity_SQLDataFrame <- function(object)
{
    idx <- object@indexes
    if (length(idx) != 2) {
        stop("The indexes for \"SQLDataFrame\" should have \"length == 2\"")
    }
    if (any(duplicated(dbconcatKey(object)))) {
        stop("The 'dbkey' column of SQLDataFrame '", dbkey(object),
             "' must have unique values!")
    }
}
setValidity("SQLDataFrame", .validity_SQLDataFrame)

###-------------
## accessor
###-------------

setGeneric("dbname", signature = "x", function(x)
    standardGeneric("dbname"))

#' @rdname SQLDataFrame-class
#' @aliases dbname dbname,SQLDataFrame-method
#' @export

setMethod("dbname", "SQLDataFrame", function(x)
{
    x@tblData$src$con@dbname
})

setGeneric("dbtable", signature = "x", function(x)
    standardGeneric("dbtable"))

#' @rdname SQLDataFrame-class
#' @aliases dbtable dbtable,SQLDataFrame-method
#' @export

setMethod("dbtable", "SQLDataFrame", function(x)
{
    op <- x@tblData$ops
    if (! is(op, "op_double")) {
        out1 <- op$x
        repeat {
            if (is(out1, "op_double")) {
                return(message(.msg_dbtable))
            } else if (is.ident(out1)) break
            out1 <- out1$x
        }
           return(as.character(out1))
    } else {
        warning(.msg_dbtable)
    }
})
.msg_dbtable <- paste0("## not available for SQLDataFrame with lazy queries ",
                       "of 'union', 'join', or 'rbind'. \n",
                       "## call 'saveSQLDataFrame()' to save the data as ",
                       "database table and call 'dbtable()' again! \n")

setGeneric("dbkey", signature = "x", function(x)
    standardGeneric("dbkey"))

#' @rdname SQLDataFrame-class
#' @aliases dbkey dbkey,SQLDataFrame-method
#' @export
setMethod("dbkey", "SQLDataFrame", function(x) x@dbkey )

setGeneric(
    "dbkey<-",
    function(x, value) standardGeneric("dbkey<-"),
    signature="x")

## Works like constructing a new SQLDF and calculate the dbconcatKey
## which is must.
#' @name "dbkey<-"
#' @rdname SQLDataFrame-class
#' @aliases dbkey<- dbkey<-,SQLDataFrame-method
#' @param value The column name to be used as \code{dbkey(x)}
#' @rawNamespace import(BiocGenerics, except=c("combine"))
#' @export
setReplaceMethod( "dbkey", "SQLDataFrame", function(x, value) {
    concatKey <- x@tblData %>%
        mutate(concatKey = paste(!!!syms(value), sep="\b")) %>%
        pull(concatKey)
    BiocGenerics:::replaceSlots(x, dbkey = value,
                                dbconcatKey = concatKey,
                                check=TRUE)
})

setGeneric("dbconcatKey", signature = "x", function(x)
    standardGeneric("dbconcatKey"))

#' @rdname SQLDataFrame-class
#' @aliases dbconcatKey dbconcatKey,SQLDataFrame-method
#' @export
setMethod("dbconcatKey", "SQLDataFrame", function(x)
{
    x@dbconcatKey
    ## do.call(paste, c(as.list(x@keyDF), sep="\b"))
})

#' @rdname SQLDataFrame-class
#' @aliases ROWNAMES ROWNAMES,SQLDataFrame-method
#' @export
setMethod("ROWNAMES", "SQLDataFrame", function(x)
{
    ridx <- ridx(x)
    res <- dbconcatKey(x)
    if (!is.null(ridx))
        res <- dbconcatKey(x)[ridx]
    return(res)
})

###--------------
### show method
###--------------

## input "tbl_dbi" and output "tbl_dbi".

## filter() makes sure it returns table with unique rows, no duplicate rows allowed...
.extract_tbl_rows_by_key <- function(x, key, concatKey, i)  ## "concatKey" must correspond to "x"
{
    ## always require a dbkey(), and accommodate with multiple key columns. 
    i <- sort(unique(i))
    if (length(key) == 1) {
        out <- x %>% filter(!!sym(key) %in% !!(concatKey[i]))
    } else {
        x <- x %>% mutate(concatKeys = paste(!!!syms(key), sep="\b"))
        ## FIXME: possible to remove the ".0" trailing after numeric values?
        ## see: https://github.com/tidyverse/dplyr/issues/3230 (deliberate...)
        ### keys <- x %>% pull(concatKeys)
        out <- x %>% filter(concatKeys %in% !!(concatKey[i])) %>% select(-concatKeys)
        ## returns "tbl_dbi" object, no realization. 
    }
    return(out)
}

## Nothing special, just queried the ridx, and ordered tbl by "key+otherCols"
.extract_tbl_from_SQLDataFrame <- function(x, collect = FALSE)
{
    ridx <- ridx(x)
    tbl <- x@tblData
    if (!is.null(ridx))
        tbl <- .extract_tbl_rows_by_key(tbl, dbkey(x), dbconcatKey(x), ridx)
    if (collect)
        tbl <- collect(tbl)
    tbl.out <- tbl %>% select(dbkey(x), colnames(x))
    return(tbl.out)
}

## .printROWS realize all ridx(x), so be careful here to only use small x.
.printROWS <- function(x, index){
    tbl <- .extract_tbl_from_SQLDataFrame(x, collect = TRUE)
    ## already ordered by "key + otherCols".
    out.tbl <- collect(tbl)
    ridx <- normalizeRowIndex(x)
    i <- match(index, sort(unique(ridx))) 
    
    out <- as.matrix(unname(cbind(
        out.tbl[i, seq_along(dbkey(x))],
        rep("|", length(i)),
        out.tbl[i, -seq_along(dbkey(x))])))
    return(out)
}

#' @rdname SQLDataFrame-class
#' @param object An \code{SQLDataFrame} object.
#' @importFrom lazyeval interp
#' @import S4Vectors
#' @export

setMethod("show", "SQLDataFrame", function (object) 
{
    nhead <- get_showHeadLines()
    ntail <- get_showTailLines()
    nr <- nrow(object)
    nc <- ncol(object)
    cat(class(object), " with ", nr, ifelse(nr == 1, " row and ", 
        " rows and "), nc, ifelse(nc == 1, " column\n", " columns\n"), 
        sep = "")
    if (nr > 0 && nc > 0) {  ## FIXME, if nc==0, still print key column?
        if (nr <= (nhead + ntail + 1L)) {
            out <- .printROWS(object, normalizeRowIndex(object))
        }
        else {
            sdf.head <- object[seq_len(nhead), , drop=FALSE]
            sdf.tail <- object[tail(seq_len(nrow(object)), ntail), , drop=FALSE]
            out <- rbind(
                .printROWS(sdf.head, ridx(sdf.head)),
                c(rep.int("...", length(dbkey(object))),".", rep.int("...", nc)),
                .printROWS(sdf.tail, ridx(sdf.tail)))
        }
        classinfoFun <- function(tbl, colnames) {
            matrix(unlist(lapply(
            as.data.frame(head(tbl %>% select(colnames))),  ## added a layer on lazy tbl. 
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
#' @name coerce
#' @aliases coerce,SQLDataFrame,data.frame-method
#' @export

setAs("SQLDataFrame", "data.frame", function(from)
{
    as.data.frame(from, optional = TRUE)
})

## refer to: getAnywhere(as.data.frame.tbl_sql) defined in dbplyr
.as.data.frame.SQLDataFrame <- function(x, row.names = NULL,
                                        optional = NULL, ...)
{
    tbl <- .extract_tbl_from_SQLDataFrame(x)
    ridx <- normalizeRowIndex(x)
    i <- match(ridx, sort(unique(ridx))) 
    as.data.frame(tbl, row.names = row.names, optional = optional)[i, ]
}

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases as.data.frame,SQLDataFrame-method
#' @param x An \code{SQLDataFrame} object
#' @param row.names \code{NULL} or a character vector giving the row
#'     names for the data frame. Only including this argument for the
#'     \code{as.data.frame} generic. Does not apply to
#'     \code{SQLDataFrame}. See \code{base::as.data.frame} for
#'     details.
#' @param optional logical. If \code{TRUE}, setting row names and
#'     converting column names is optional. Only including this
#'     argument for the \code{as.data.frame} generic. Does not apply
#'     to \code{SQLDataFrame}. See \code{base::as.data.frame} for
#'     details.
#' @param ... additional arguments to be passed.
#' @export
setMethod("as.data.frame", signature = "SQLDataFrame", .as.data.frame.SQLDataFrame)

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,SQLDataFrame,DataFrame-method
#' @export
#' 
setAs("SQLDataFrame", "DataFrame", function(from)
{
    as(as.data.frame(from), "DataFrame")
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,data.frame,SQLDataFrame-method
#' @export
#' 
setAs("data.frame", "SQLDataFrame", function(from)
{
    ## check if unique for columns (from left to right)
    ## browser()
    for (i in seq_len(ncol(from))) {
        ifunique <- !any(duplicated(from[,i]))
        if (ifunique) break
    }
    dbkey <- colnames(from)[i]
    msg <- paste0("## Using the '", dbkey, "' as 'dbkey' column. \n",
                  "## Otherwise, construct a \"SQLDataFrame\" by: \n",
                  "## \"makeSQLDataFrame(filename, dbkey = \"\")\" \n")
    message(msg)
    makeSQLDataFrame(from, dbkey = colnames(from)[i])
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,DataFrame,SQLDataFrame-method
#' @export
#' 
setAs("DataFrame", "SQLDataFrame", function(from)
{
    as(as.data.frame(from), "SQLDataFrame")
})

#' @name coerce
#' @rdname SQLDataFrame-class
#' @aliases coerce,ANY,SQLDataFrame-method
#' @export
#' 
setAs("ANY", "SQLDataFrame", function(from)
{
    from <- as.data.frame(from)
    if (ncol(from) == 1)
        stop("There should be more than 1 columns available ",
             "to construct a \"SQLDataFrame\" object")
    if (anyDuplicated(tolower(colnames(from))))
        stop("Please use distinct colnames (case-unsensitive)!")
    as(as.data.frame(from), "SQLDataFrame")
})

