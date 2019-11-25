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
#' test.db <- system.file("extdata/test.db", package = "SQLDataFrame")
#' conn <- DBI::dbConnect(DBI::dbDriver("SQLite"), dbname = test.db)
#' obj <- SQLDataFrame(conn = conn, dbtable = "state", dbkey = "state")
#' dim(obj)
#' dimnames(obj)
#' length(obj)
#' names(obj)

setMethod("dim", "SQLDataFrame", function(x)
{
    x@dim
})

#' @rdname SQLDataFrame-methods
#' @aliases dimnames dimnames,SQLDataFrame-method
#' @return \code{dimnames}: A list of character vectors.
#' @export

setMethod("dimnames", "SQLDataFrame", function(x)
{
    x@dimnames
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

.rids <- function(idx, rle) {
    cuml <- c(0, cumsum(runLength(rle)))
    sapply(idx, function(x) {
        d <- x - cuml
        d[sum(d>0)]
    })
}

.keyidx <- function(idx, rle) {
    list(pid = rle[idx],  
         rid = .rids(idx, rle))
}

.filtexp <- function(keysdf, partitionID) {
    vals <- runValue(keysdf$pid)
    vals1 <- paste0(partitionID, "== '", vals, "'")
    rids <- split(keysdf$rid, keysdf$pid)
    rids1 <- sapply(rids, function(x) paste0("rid %in% c(", paste(x, collapse = ", "), ")"))
    paste(paste(vals1, "&", rids1), collapse = " | ") 
}

.extractROWS_SQLDataFrame <- function(x, i)
{
    i <- normalizeSingleBracketSubscript(i, x)
    if (!is.null(ridx(x))){
        i <- ridx(x)[i]
    }
    if (!is.null(pidRle(x))) {
        new_pidRle <- pidRle(x)[i]
        res_keys <- .keyidx(i, pidRle(x))
        new_keyData <- keyData(x) %>%
            filter(rlang::parse_expr(.filtexp(res_keys, pid(x)))) %>%
            mutate(rid = row_number(!!sym(dbkey(x)[dbkey(x) != pid(x)][1])))
    } else {
        new_pidRle <- NULL
        new_keyData <- keyData(x) %>% filter(rid %in% i) %>%
            mutate(rid = row_number(!!syms(dbkey(x)[1])))
    }
    new_tblData <- semi_join(tblData(x), new_keyData)
    ## add @ridx slot as rank(i) if i is not sequential. using rank(i)
    ## instead of i here, because the @pidRle was updated with
    ## runlength == length(i), and @keyData$rid was recalculated with
    ## nrow == length(i). Printed results are sorted by dbkey, so
    ## rank(i) will resume the order of initial i in subsetting. 
    if (!identical(order(i), seq_along(i))) {
        new_ridx <- as.integer(rank(unique(i)))
        new_ridx <- new_ridx[match(i, unique(i))]
        ## now accommodates non-sequential and duplicate row indexes
    } else {
        new_ridx <- ridx(x)
    }
    if (is.null(new_ridx)) {
        new_nr <- new_keyData %>% ungroup %>% summarize(n = n()) %>%
            pull(n) %>% as.integer
    } else {
        new_nr <- length(new_ridx)
    }
    BiocGenerics:::replaceSlots(x, tblData = new_tblData,
                                keyData = new_keyData,
                                pidRle = new_pidRle,
                                dim = c(new_nr, ncol(x)),
                                ridx = new_ridx)
}
setMethod("extractROWS", "SQLDataFrame", .extractROWS_SQLDataFrame)

#' @importFrom stats setNames
.extractCOLS_SQLDataFrame <- function(x, i)
{
    xstub <- setNames(seq_along(x), names(x))
     ## accommodates when subsetting key columns.
    if (is.character(i) && any(i %in% dbkey(x)))
        i <- i[!i %in% dbkey(x)] 
    i <- normalizeSingleBracketSubscript(i, xstub)
    new_tblData <- tblData(x) %>% select(dbkey(x), names(xstub)[i])
    ## always retain the key columns!
    new_dimnames <- list(NULL, names(xstub)[i])
    new_dim <- c(nrow(x), length(i))
    BiocGenerics:::replaceSlots(x, tblData = new_tblData, dim = new_dim,
                                dimnames = new_dimnames)
}
setMethod("extractCOLS", "SQLDataFrame", .extractCOLS_SQLDataFrame)

#' @description \code{[i, j]} supports subsetting by \code{i} (for
#'     row) and \code{j} (for column) and respects ‘drop=FALSE’.
#' @rdname SQLDataFrame-methods
#' @param i Row subscript. Could be numeric / character / logical
#'     values, a named list of key values, and \code{SQLDataFrame},
#'     \code{data.frame}, \code{tibble} objects.
#' @param j Column subscript.
#' @param drop Whether to drop with reduced dimension. Default is
#'     TRUE.
#' @return \code{[i, j]}: A \code{SQLDataFrame} object or vector with
#'     realized column values (with single column subsetting and
#'     default \code{drop=TRUE}. )
#' @aliases [,SQLDataFrame,ANY-method
#' @importFrom tibble tibble
#' @export
#' @examples
#'
#' obj1 <- SQLDataFrame(conn = conn, dbtable = "state",
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
#' obj1[c("South:3615.0", "West:3559.0"), ]
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
#'
#' ## Subsetting with key columns
#'
#' obj["state"] ## list style subsetting, return a SQLDataFrame object with col = 0.
#' obj[c("state", "division")]  ## list style subsetting, return a SQLDataFrame object with col = 1.
#' obj[, "state"] ## realize specific key column value.
#' obj[, c("state", "division")] ## col = 1, but do not realize.
#' 

setMethod("[", "SQLDataFrame", function(x, i, j, ..., drop = TRUE)
{
    ## browser()
    if (!isTRUEorFALSE(drop)) 
        stop("'drop' must be TRUE or FALSE")
    if (length(list(...)) > 0L) 
        warning("parameters in '...' not supported")
    nc <- ncol(x)
    list_style_subsetting <- (nargs() - !missing(drop)) < 3L
    if (list_style_subsetting || !missing(j)) {
        if (list_style_subsetting) {
            if (!missing(drop)) 
                warning("'drop' argument ignored by list-style subsetting")
            if (missing(i)) 
                return(x)  ## x[] 
            j <- i  ## x[i]
        }
        if (!is(j, "IntegerRanges")) {
            x <- extractCOLS(x, j) ## x["key"] returns SQLSataFrame
                                   ## with 0 cols.
        }
        if (list_style_subsetting) 
            return(x)
    }
    if (!missing(i)) { 
        x <- extractROWS(x, i)
    }
    if (missing(drop)) 
        drop <- nrow(x) & ncol(x) %in% c(0L, 1L) ## if nrow(x)==0,
                                                 ## return the
                                                 ## SQLDataFrame with
                                                 ## 0 rows and 1
                                                 ## column(s)
    if (drop) {
        ## x[, "col"]: realize; x[,c("key", "other")]: do not realize??
        if (ncol(x) == 1L & nc > 1)
            return(x[[1L]])
        if (ncol(x) == 0 && !missing(j) && j %in% dbkey(x)) 
            return(x[[j]]) ## x[,"key"] returns realized value of that
                           ## key column.
        if (nrow(x) == 1L) 
            return(as(x, "list"))
    }
    x
})

## #' @rdname SQLDataFrame-methods
## #' @importFrom methods is as callNextMethod
## #' @aliases [,SQLDataFrame,SQLDataFrame-method 
## #' @export
## setMethod("[", signature = c("SQLDataFrame", "SQLDataFrame", "ANY"),
##           function(x, i, j, ..., drop = TRUE)
## {
##     browser()
##     if (!identical(dbkey(x), dbkey(i)))
##         stop("The dbkey() must be same between '", deparse(substitute(x)),
##              "' and '", deparse(substitute(i)), "'.", "\n")
##     new_keyData <- semi_join(keyData(x), keyData(i)) ## FIXME
##     new_tblData <- semi_join(tblData(x), new_keyData)
##     new_pidRle <- pidRle(i)
##     BiocGenerics:::replaceSlots(x, tblData = new_tblData, keyData = new_keyData,
##                                 pidRle = new_pidRle)
## })

#' @rdname SQLDataFrame-methods
#' @aliases [,SQLDataFrame,list-method 
#' @export
setMethod("[", signature = c("SQLDataFrame", "list", "ANY"),
          function(x, i, j, ..., drop = TRUE)
{
    browser()
    if (!identical(dbkey(x), union(dbkey(x), names(i))))
        stop("Please use: '", paste(dbkey(x), collapse=", "),
             "' as the query list name(s).")
    ## i <- do.call(paste, c(i[dbkey(x)], sep=":"))
    tmp <- lapply(i, function(x) paste0("c(", paste(x, collapse=","), ")"))
    exp <- paste(names(tmp), tmp, sep = " %in% ")
    filter.SQLDataFrame(x, rlang::parse_expr(exp))
    callNextMethod()
})

###--------------------
### "[[,SQLDataFrame" (do realization for single column only)
###--------------------
#' @rdname SQLDataFrame-methods
#' @export
setMethod("[[", "SQLDataFrame", function(x, i, j, ...)
{
## NOTE: will realize single column into R memory, will will be very
## expensive for very large BigQuery tables. Not recommended!
    ## browser()
    dotArgs <- list(...)
    if (length(dotArgs) > 0L) 
        dotArgs <- dotArgs[names(dotArgs) != "exact"]
    if (!missing(j) || length(dotArgs) > 0L) 
        stop("incorrect number of subscripts") 
    xstub <- setNames(seq_along(x), names(x))
    tryi <- try(normalizeSingleBracketSubscript(i, xstub), silent=TRUE)
    if (is(tryi, "try-error")) {
        if (i %in% dbkey(x)) {
            res <- tblData(x) %>% arrange(!!!syms(dbkey(x))) %>% pull(!!sym(i))
        } else {
            stop(attr(tryi, "condition")$message)  ## FIXME: cleaner way for printing?
        }
    } else {
    res <- tblData(x) %>% arrange(!!!syms(dbkey(x))) %>% pull(!!sym(names(xstub)[tryi]))
    }
    if (!is.null(ridx(x)))
        res <- res[ridx(x)]
    return(res)
})

#' @rdname SQLDataFrame-methods
#' @param name column name to be extracted by \code{$}.
#' @export
setMethod("$", "SQLDataFrame", function(x, name) x[[name]] )

#############################
### select, filter & mutate
#############################

#' @description Use \code{select()} function to select certain
#'     columns. 
#' @rdname SQLDataFrame-methods
#' @aliases select select,SQLDataFrame-methods
#' @return \code{select}: always returns a SQLDataFrame object no
#'     matter how may columns are selected. If only key column(s)
#'     is(are) selected, it will return a \code{SQLDataFrame} object
#'     with 0 col (only key columns are shown).
#' @param .data A \code{SQLDataFrame} object.
#' @param ... additional arguments to be passed.  \itemize{ \item
#'     \code{select()}: One or more unquoted expressions separated by
#'     commas. You can treat variable names like they are positions,
#'     so you can use expressions like ‘x:y’ to select ranges of
#'     variables. Positive values select variables; negative values
#'     drop variables. See \code{?dplyr::select} for more details.
#'     \item \code{filter()}: Logical predicates defined in terms of
#'     the variables in ‘.data’. Multiple conditions are combined with
#'     ‘&’. Only rows where the condition evaluates to ‘TRUE’ are
#'     kept. See \code{?dplyr::filter} for more details.  \item
#'     \code{mutate()}: Name-value pairs of expressions, each with
#'     length 1 or the same length as the number of rows in the group
#'     (if using ‘group_by()’) or in the entire input (if not using
#'     groups). The name of each argument will be the name of a new
#'     variable, and the value will be its corresponding value. Use a
#'     ‘NULL’ value in ‘mutate’ to drop a variable.  New variables
#'     overwrite existing variables of the same name.  }
#' @export
#' @importFrom tidyselect vars_select
#' @examples
#' 
#' ###################
#' ## select, filter, mutate
#' ###################
#' library(dplyr)
#' obj %>% select(division)  ## equivalent to obj["division"], or obj[, "division", drop = FALSE]
#' obj %>% select(region:size)
#' 
#' obj %>% filter(region == "West" & size == "medium")
#' obj1 %>% filter(region == "West" & population > 10000)
#' 
#' obj %>% mutate(p1 = population / 10)
#' obj %>% mutate(s1 = size)
#'
#' obj %>% select(region, size, population) %>% 
#'     filter(population > 10000) %>% 
#'     mutate(pK = population/1000)
#' obj1 %>% select(region, size, population) %>% 
#'     filter(population > 10000) %>% 
#'     mutate(pK = population/1000)  

select.SQLDataFrame <- function(.data, ...)
{
    ## always return a SQLDataFrame object, never realize a single column.
    dots <- quos(...)
    old_vars <- op_vars(tblData(.data)$ops)
    new_vars <- vars_select(old_vars, !!!dots, .include = op_grps(tblData(.data)$ops))
    .extractCOLS_SQLDataFrame(.data, new_vars) 
}

#' @description Use \code{filter()} to choose rows/cases where
#'     conditions are true. Note that after filtering, the original
#'     order of data will be dropped and data will be reordered by key
#'     columns.
#' @rdname SQLDataFrame-methods
#' @aliases filter filter,SQLDataFrame-method
#' @return \code{filter}: A \code{SQLDataFrame} object with subset
#'     rows of the input SQLDataFrame object matching conditions.
#' @export

filter.SQLDataFrame <- function(.data, ...)
{
    new_tblData <- tblData(.data) %>% dplyr::filter(...)
    ## @keyData
    new_keyData <- .update_keyData(new_tblData, dbkey(.data), pid(.data))
    ## @pidRle
    new_pidRle <- .update_pidRle(new_keyData, pid(.data))
    ## @dim, @dimnames
    new_nr <- new_keyData %>% ungroup %>% summarize(n = n()) %>%
        pull(n) %>% as.integer
    ## for "filter", no need to honor the existing ridx(x), so will
    ## reset as NULL.
    BiocGenerics:::replaceSlots(.data, tblData = new_tblData,
                                keyData = new_keyData,
                                pidRle = new_pidRle,
                                dim = c(new_nr, ncol(.data)),
                                ridx = NULL)
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
    new_tblData <- tblData(.data) %>% dplyr::mutate(...)
    cns <- setdiff(colnames(new_tblData), dbkey(.data))
    new_dimnames <- list(NULL, cns)
    new_dim <- c(nrow(.data), length(cns))
    ## "mutate" will preserver previous @ridx
    BiocGenerics:::replaceSlots(.data, tblData = new_tblData,
                                dim = new_dim, dimnames = new_dimnames)
}
#' @description \code{connSQLDataFrame} returns the connection of a
#'     SQLDataFrame object.
#' @rdname SQLDataFrame-methods
#' @export
#' @examples
#' 
#' ###################
#' ## connection info
#' ###################
#'
#' connSQLDataFrame(obj)
connSQLDataFrame <- function(x)
{
    tblData(x)$src$con
}
