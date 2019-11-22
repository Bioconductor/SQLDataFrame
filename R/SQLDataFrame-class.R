#' SQLDataFrame class
#' @name SQLDataFrame
#' @aliases class:SQLDataFrame SQLDataFrame-class
#' @rdname SQLDataFrame-class
#' @description NULL
#' @exportClass SQLDataFrame
#' @importFrom methods setOldClass new
#' @importFrom S4Vectors Rle runLength runValue
## add other connections. 
setOldClass(c("tbl_MySQLConnection", "tbl_SQLiteConnection",
              "tbl_BigQueryConnection",
              "tbl_dbi", "tbl_sql", "tbl_lazy", "tbl"))
setClassUnion("Rle_or_null", c("Rle", "NULL"))
setClassUnion("character_or_null", c("character", "NULL"))
setClassUnion("integer_or_null", c("integer", "NULL"))
.SQLDataFrame <- setClass(
    "SQLDataFrame",
    slots = c(
        tblData = "tbl_dbi", ## "lazy_tbl" from original sql database table.
        keyData = "tbl_dbi", ## "lazy_tbl", rid, [pid], key1, key2, ...
        dbkey = "character",  ## colnames(keyData) - rid [-pid]
        ## dbnrows = "integer", ## maps to original data row
        ## Dimension. ## FIXME: REMOVE?
        dim = "integer",  ## calculate each time with row subsetting:
                          ## '[', filter, etc
        dimnames = "list",
        partitionID = "character_or_null",  ## for "tbl_BigQueryConnection"
        pidRle = "Rle_or_null", ## saves the "partionID + rid" for
                                ## very large bigQuery tables, update
                                ## with operations.
        ridx = "integer_or_null"  ## saves the non-sequential numeric
                                  ## indexs from '['
                                  ## subsetting. e.g., sample(10)
    )
)

### Constructor
#' @rdname SQLDataFrame-class
#' @description \code{SQLDataFrame} constructor, slot getters, show
#'     method and coercion methods to \code{DataFrame} and
#'     \code{data.frame} objects.
#' @param conn a valid \code{DBIConnection} from \code{SQLite},
#'     \code{MySQL} or \code{BigQuery}. If provided, arguments of
#'     `user`, `host`, `dbname`, `password` will be ignored.
#' @param host host name for MySQL database or project name for
#'     BigQuery.
#' @param user user name for SQL database.
#' @param dbname database name for SQL connection.
#' @param password password for SQL database connection.
#' @param billing the Google Cloud project name with authorized
#'     billing information.
#' @param type The SQL database type, supports "SQLite", "MySQL" and
#'     "BigQuery".
#' @param dbtable A character string for the table name in that
#'     database. If not provided and there is only one table
#'     available, it will be read in by default.
#' @param dbkey A character vector for the name of key columns that
#'     could uniquely identify each row of the database table. Will be
#'     ignored for \code{BigQueryConnection}.
#' @param partitionID A character for the column name of very large
#'     BigQuery tables to be partitioned on before assigning row
#'     ids. Takes NULL by default.
#' @param col.names A character vector specifying the column names you
#'     want to read into the \code{SQLDataFrame}.
#' @return A \code{SQLDataFrame} object.
#' @export
#' @importFrom tools file_path_as_absolute
#' @import RSQLite
#' @import dbplyr
#' @examples
#' 
#' ## SQLDataFrame construction
#' dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
#' conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = dbname)
#' obj <- SQLDataFrame(conn = conn, dbtable = "state",
#'                     dbkey = "state")
#' obj1 <- SQLDataFrame(conn = conn, dbtable = "state",
#'                      dbkey = c("region", "population"))
#' obj1
#'
#' ### construction from database credentials
#' obj2 <- SQLDataFrame(dbname = dbname, type = "SQLite",
#'                      dbtable = "state", dbkey = "state")
#' all.equal(obj, obj2)  ## [1] TRUE
#' 
#' ## slot accessors
#' connSQLDataFrame(obj)
#' dbtable(obj)
#' dbkey(obj)
#' dbkey(obj1)
#'
#' dbconcatKey(obj)
#' dbconcatKey(obj1)
#'
#' ## slot accessors (for internal use only)
#' tblData(obj)
#' dbnrows(obj)
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
#'
#' ## construction from MySQL 
#' \dontrun{
#' mysqlConn <- DBI::dbConnect(dbDriver("MySQL"),
#'                             host = "",
#'                             user = "",
#'                             password = "",  ## required if need further
#'                                             ## aggregation operations such as
#'                                             ## join, union, rbind, etc. 
#'                             dbname = "")
#' sdf <- SQLDataFrame(conn = mysqlConn, dbtable = "", dbkey = "")
#'
#' ## Or pass credentials directly into constructor
#' objensb <- SQLDataFrame(user = "genome",
#'                         host = "genome-mysql.soe.ucsc.edu",
#'                         dbname = "xenTro9",
#'                         type = "MySQL",
#'                         dbtable = "xenoRefGene",
#'                         dbkey = c("name", "txStart"))
#' 
#' }
#' 
#' ## construction from BigQuery
#' \dontrun{
#' con <- DBI::dbConnect(bigquery(),  ## equivalent dbDriver("bigquery")
#'                       project = "bigquery-public-data",
#'                       dataset = "human_variant_annotation",
#'                       billing = "")  ## your project name that linked to
#'                                      ## Google Cloud with billing information.
#' sdf <- SQLDataFrame(conn = con, dbtable = "ncbi_clinvar_hg38_20180701")
#'
#' ## Or pass credentials directly into constructor
#' sdf1 <- SQLDataFrame(host = "bigquery-public-data",
#'                      dbname = "human_variant_annotation",
#'                      billing = "",
#'                      type = "BigQuery",
#'                      dbtable = "ncbi_clinvar_hg38_20180701")
#'
#' }

SQLDataFrame <- function(conn,
                         host, user, dbname,
                         password = NULL, ## required for certain MySQL connection.
                         billing = character(0),  ## BigQuery connection.
                         type = c("SQLite", "MySQL", "BigQuery"),
                         dbtable = character(0), ## could be NULL if
                                                 ## only 1 table exists!
                         dbkey = character(0),
                         partitionID = NULL,
                         col.names = NULL
                         ){

    type <- match.arg(type)
    if (missing(conn)) {
        ## FIXME: Error in .local(drv, ...) : Cannot allocate a new
        ## connection: 16 connections already opened
        conn <- switch(type,
                       MySQL = DBI::dbConnect(dbDriver("MySQL"),
                                              host = host,
                                              user = user,
                                              password = password,
                                              dbname = dbname),
                       SQLite = DBI::dbConnect(dbDriver("SQLite"),
                                               dbname = dbname),
                       BigQuery = DBI::dbConnect(dbDriver("bigquery"),
                                                 project = host,
                                                 dataset = dbname,
                                                 billing = billing)
                       )
    } else {
        ifcred <- c(host = !missing(host), user = !missing(user),
                    dbname = !missing(dbname), password = !missing(password),
                    billing = !missing(billing))
        if (any(ifcred))
            message("These arguments are ignored: ",
                    paste(names(ifcred[ifcred]), collapse = ", "))
    }
    ## check dbtable
    tbls <- DBI::dbListTables(conn)
    if (missing(dbtable) || !dbExistsTable(conn, dbtable)) {
        if (length(tbls) == 1) {
            dbtable <- tbls
            message(paste0("Using the only table: '", tbls,
                           "' that is available in the connection"))
        } else {
            stop("Please specify the 'dbtable' argument, ",
                 "which must be one of: '",
                 paste(tbls, collapse = ", "), "'")
        }
    }
    ## check dbkey
    if (is(conn, "BigQueryConnection")) {
        if (!missing(dbkey))
            cat("The argument: '", dbkey, "' is ignored for BigQueryConnection.")        
    } else {
        flds <- dbListFields(conn, dbtable)
        if (!all(dbkey %in% flds)){
            stop("Please specify the 'dbkey' argument, ",
                 "which must be one of: '",
                 paste(flds, collapse = ", "), "'")
        }
    }
    
    if (is(conn, "MySQLConnection")) {
        .set_mysql_var(conn, password)
    }        
    
    ## construction
    ## FIXME: ROW_NUMBER() only works here for "BigQueryConnection", not for
    ### SQLiteConnection, and MySQLConnection. in ".update_keyData"
    ### check for connection type, if "MySQLConnection", realize keyData...

    ## @tblData
    tblData <- conn %>% tbl(dbtable)   ## ERROR if "dbtable" does not exist!
    ## @keyData
    keyData <- .update_keyData(tblData, dbkey, partitionID)
    ## @pidRle
    pidRle <- .update_pidRle(keyData, partitionID)
    ## @dim, @dimnames
    nr <- keyData %>% ungroup %>% summarize(n = n()) %>% pull(n) %>% as.integer
    cns <- setdiff(colnames(tblData), dbkey)
    nc <- length(cns)
    ## @ridx 
    ridx <- NULL  ## NULL for initial construction
    
    .SQLDataFrame(
        tblData = tblData,
        keyData = keyData, 
        dbkey = dbkey,
        dim = c(nr, nc),
        dimnames = list(NULL, cns),
        partitionID = partitionID,
        pidRle = pidRle,
        ridx = ridx
    )
}

## .validity_SQLDataFrame <- function(object)
## {
##     idx <- object@indexes
##     if (length(idx) != 2) {
##         stop("The indexes for SQLDataFrame should have 'length == 2'")
##     }
##     if (any(duplicated(dbconcatKey(object)))) {
##         stop("The 'dbkey' column of SQLDataFrame '", dbkey(object),
##              "' must have unique values!")
##     }
## }
## setValidity("SQLDataFrame", .validity_SQLDataFrame)

###-------------
## accessor
###-------------

setGeneric("tblData", signature = "x", function(x)
    standardGeneric("tblData"))

#' @rdname SQLDataFrame-class
#' @aliases tblData tblData,SQLDataFrame-method
#' @export

setMethod("tblData", "SQLDataFrame", function(x)
{
    x@tblData
})

setGeneric("keyData", signature = "x", function(x)
    standardGeneric("keyData"))

#' @rdname SQLDataFrame-class
#' @aliases keyData keyData,SQLDataFrame-method
#' @export

setMethod("keyData", "SQLDataFrame", function(x)
{
    x@keyData
})

setGeneric("dbtable", signature = "x", function(x)
    standardGeneric("dbtable"))

#' @rdname SQLDataFrame-class
#' @aliases dbtable dbtable,SQLDataFrame-method
#' @export

setMethod("dbtable", "SQLDataFrame", function(x)
{
    op <- tblData(x)$ops
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
    ## browser()
    if (!all(value %in% colnames(tblData(x))))
        stop("Please choose 'dbkey' from the following: ",
             paste(colnames(tblData(x)), collapse = ", "))
    new_keyData <- .update_keyData(tblData(x), value, pid(x))
    new_pidRle <- .update_pidRle(new_keyData, pid(x))
    cns <- setdiff(colnames(tblData(x)), value)
    nc <- length(cns)
    BiocGenerics:::replaceSlots(x, dbkey = value,
                                keyData = new_keyData,
                                pidRle = new_pidRle,
                                dim = c(nrow(x), nc),
                                dimnames = list(NULL, cns),
                                check=TRUE)
})

.update_keyData <- function(tblData, dbkey, partitionID = NULL)
{
    ## here assuming the dbkey are all in colnames(tblData), no checking.
    if (!is.null(partitionID)) { 
        keyData <- tblData %>% group_by(!!!syms(partitionID)) %>%
            mutate(rid = row_number(!!sym(dbkey[dbkey!=partitionID][1]))) %>%
            select(!!!syms(partitionID), rid, !!!syms(dbkey))
    } else { 
        keyData <- tblData %>% 
            mutate(rid = row_number(!!sym(dbkey[1]))) %>% 
            select(rid, dbkey)
    }
    keyData
}

.update_pidRle <- function(keyData, partitionID = NULL)
{
    pidRle <- NULL
    if (!is.null(partitionID)) {
        pidprep <- keyData %>% summarize(n=n()) %>%
            arrange(!!!syms(partitionID)) %>% as.data.frame()
        pidRle <- Rle(values = pidprep[,1], lengths = pidprep[,2])    
    }
    pidRle
}

setGeneric("ridx", signature = "x", function(x)
    standardGeneric("ridx"))

#' @rdname SQLDataFrame-class
#' @aliases ridx ridx,SQLDataFrame-method
#' @export
setMethod("ridx", "SQLDataFrame", function(x)
{
    x@ridx
})

setGeneric("pid", signature = "x", function(x)
    standardGeneric("pid"))

#' @rdname SQLDataFrame-class
#' @aliases pid pid,SQLDataFrame-method
#' @export
setMethod("pid", "SQLDataFrame", function(x)
{
    x@partitionID
})

setGeneric("pidRle", signature = "x", function(x)
    standardGeneric("pidRle"))

#' @rdname SQLDataFrame-class
#' @aliases pidRle pidRle,SQLDataFrame-method
#' @export
setMethod("pidRle", "SQLDataFrame", function(x)
{
    x@pidRle
})

###--------------
### show method
###--------------

## .printROWS is a utility function for printing smaller dataset,
## mainly used in the show method, so index is mostly in length of
## "get_showHeadLines()". BE CAREFUL when using "index=NULL" to print
## all rows.
## IMPORTANT: always order the tblData by dbkey(x) before printing.

#' @importFrom rlang parse_expr
.printROWS <- function(x, index = NULL, colClass = FALSE)
{
    ## default as print all rows to avoid unnecessary expensive
    ## operations. Be cautious of printing big dataset.
    if (is.null(index)) {  ## similar to as.data.frame
        res_tblData <- tblData(x) %>% arrange(!!!syms(dbkey(x)))
        out_tbl <- collect(res_tblData)  ## collectm <- memoise(collect)
        if (!is.null(ridx(x))) out_tbl <- out_tbl[ridx(x), ]
    } else {
        ## add @ridx here. new_index <- ridx(x)[index] is to get the
        ## corresponding row indexes in "@tblData". But after
        ## extracting corresponding rows, in the end will print:
        ## out_tbl[rank(new_index), ]
        if (!is.null(ridx(x))) {
            index <- ridx(x)[index]
        }
        if (!is.null(pidRle(x))) {
            res_keys <- .keyidx(index, pidRle(x))
            res_keyData <- keyData(x) %>% filter(rlang::parse_expr(.filtexp(res_keys, pid(x))))
        } else {
            res_keyData <- keyData(x) %in% filter(rid %in% index)
        }
        res_tblData <- semi_join(tblData(x), res_keyData) %>% arrange(!!!syms(dbkey(x)))
        out_tbl <- collect(res_tblData)[rank(index),]
    }
    res <- as.matrix(unname(out_tbl))
    res_nrow <- ifelse(is.null(index), nrow(x), length(index))
    seps <- matrix("|", nrow = res_nrow, ncol = 1)
    ## get the colClass for "out_tbl"
    if (colClass) {  
        colClasses <- matrix(unlist(lapply(as.data.frame(out_tbl),
                                           function(x)
                                               paste0("<", classNameForDisplay(x), ">")),
                                    use.names=F), nrow=1, dimnames = list("", colnames(out_tbl)))
        res <- rbind(colClasses, res)
        seps <- matrix("|", nrow = res_nrow + 1, ncol = 1, dimnames = list(NULL, "|"))
    }
    ## return
    cbind(res[, .wheredbkey(x), drop=FALSE], seps, res[, -.wheredbkey(x), drop=FALSE])
}

#' @rdname SQLDataFrame-class
#' @param object An \code{SQLDataFrame} object.
#' @importFrom lazyeval interp
#' @import S4Vectors
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
    if (nr > 0 && nc >= 0) {
        if (nr <= (nhead + ntail + 1L)) {
            out <- .printROWS(object, colClass = T)
        }
        else {
            out <- rbind(
                .printROWS(object, seq_len(nhead), colClass = T),
                c(rep.int("...", length(dbkey(object))),".", rep.int("...", nc)),
                .printROWS(object, tail(seq_len(nr), ntail)))
        }
        cat(class(object), " with ", nr, " rows and ", nc, " columns\n", sep = "")
        print(out, quote = FALSE, right = TRUE)
    }
})

###--------------
### coercion
###--------------

## refer to: getAnywhere(as.data.frame.tbl_sql) defined in dbplyr
.as.data.frame.SQLDataFrame <- function(x, row.names = NULL,
                                        optional = NULL, ...)
{
    res_tblData <- tblData(x) %>% arrange(!!!syms(dbkey(x)))
    out <- as.data.frame(collect(res_tblData))
    if (!is.null(ridx(x)))
        out <- out[ridx(x), ]
    rownames(out) <- NULL
    return(out)
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

#' @rdname SQLDataFrame-class
#' @name coerce
#' @aliases coerce,SQLDataFrame,data.frame-method
#' @export

setAs("SQLDataFrame", "data.frame", function(from)
{
    as.data.frame(from, optional = TRUE)
})

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
    for (i in seq_len(ncol(from))) {
        ifunique <- !any(duplicated(from[,i]))
        if (ifunique) break
    }
    dbkey <- colnames(from)[i]
    msg <- paste0("## Using the '", dbkey, "' as 'dbkey' column. \n",
                  "## Otherwise, construct a 'SQLDataFrame' by: \n",
                  "## 'makeSQLDataFrame(filename, dbkey = '')' \n")
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
             "to construct a 'SQLDataFrame' object")
    if (anyDuplicated(tolower(colnames(from))))
        stop("Please use distinct colnames (case-unsensitive)!")
    as(as.data.frame(from), "SQLDataFrame")
})

