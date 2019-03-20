## "union" method is defined for both S3 and S4 classes. Since
## SQLDataFrame is a S4 object, here I am using "setMethod" instead of
## "union.SQLDataFrame".

## "union" itself removes duplicates, and reorder by default using 1st column, then 2nd, ...
## SQL::union_all, I guess, no automatic ordering...

## setMethod("union", signature = "SQLDataFrame", function(x, y, ...)
## {
##     browser()
##     x1 <- .extract_tbl_from_SQLDataFrame(x)
##     y1 <- .extract_tbl_from_SQLDataFrame(y)
##     tbl.union <- dbplyr:::union.tbl_lazy(x1, y1)
##     x@tblData <- tbl.union
##     x@dbnrows <- tbl.union %>% summarize(n=n()) %>% pull(n)
##     x@indexes <- vector("list", 2)
##     return(x)
## })

## works as "rbind"... Be cautious to save the @tblData, which is
## supposed to be unique rows. So need to modify the @tblData and
## update @indexes. Call "union.tbl_lazy", reverse the automatic
## ordering, and then update @indexes.

## possible strategy:
## 1. use "union", then reverse the ordering.
## 2. use "union_all", then only extract unique lines. "SQL::unique?"

## following function works like "union" without automatic
## ordering. Could rename as just "union", and explain the strategy in
## documentation. Extend to "rbind" function by updating @indexes
## slot.
#' @export

## Since "dbplyr:::union.tbl_lazy(x,y)" only evaluates data from same source, need to rewrite, using the dbExecute(con, "ATTACH dbname AS aux")
setMethod("union", signature = c("SQLDataFrame", "SQLDataFrame"), function(x, y, ...)
{
    browser()
    x1 <- .extract_tbl_from_SQLDataFrame(x)
    y1 <- .extract_tbl_from_SQLDataFrame(y)
    
    if (is(x1$ops$x, "op_set_op")) {
        con <- x1$src$con
        ## check if the y1$src$con@dbname already attached.
        dbs <- dbGetQuery(con, "PRAGMA database_list")
        aux_y <- dbs[match(dbname(y), dbs$file), "name"]
        if (is.na(aux_y))
            aux_y <- .attach_database_from_SQLDataFrame(con, y)
        tbly <- .open_tbl_from_new_connection(con, aux_y, y)
        y1 <- tbly
    } else if (is(y1$ops$x, "op_set_op")) {
        con <- y1$src$con
        dbs <- dbGetQuery(con, "PRAGMA database_list")
        aux_x <- dbs[match(dbname(x), dbs$file), "name"]
        if (is.na(aux_x)) {
            aux_x <- .attach_database_from_SQLDataFrame(con, x)
        }
        tblx <- .open_tbl_from_new_connection(con, aux_x, x)
        x1 <- tblx
    } else {
        dbname <- tempfile(fileext = ".db")
        con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
        
        ## attach database into the existing connection.
        aux_x <- .attach_database_from_SQLDataFrame(con, x)
        tblx <- .open_tbl_from_new_connection(con, aux_x, x)
        if (same_src(x1, y1)) {
            aux_y <- aux_x
        } else {
            aux_y <- .attach_database_from_SQLDataFrame(con, y)
        } 
        tbly <- .open_tbl_from_new_connection(con, aux_y, y)
        x1 <- tblx
        y1 <- tbly
    }
    tbl.ua <- dbplyr:::union.tbl_lazy(x1, y1)

    ## recalculate the @dbconcatKey
    x@dbconcatKey <- tbl.ua %>%
        mutate(concatKey = paste(!!!syms(dbkey(x)), sep="\b")) %>%
        pull(concatKey)
    
    ## ## extract the new @dbconcatKey instead of recalculating
    ## tt <- do.call(rbind, strsplit(c(ROWNAMES(x), ROWNAMES(y)), split = "\b"))
    ## tt <- as.data.frame(tt, stringsAsFactors = FALSE)
    ## cls <- x1 %>% head %>% select(dbkey(x)) %>% as.data.frame() %>% sapply(class)
    ## for (i in seq_len(length(tt))) class(tt[,i]) <- unname(cls)[i]
    ## tt[with(tt, order(!!!syms(dbkey(x)))), ] ## doesn't work ... 
    
    x@tblData <- tbl.ua
    x@dbnrows <- tbl.ua %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)
    
    return(x)
})

.attach_database_from_SQLDataFrame <- function(con, sdf) {
    aux <- dbplyr:::random_table_name()
    dbExecute(con, paste0("ATTACH '", dbname(sdf), "' AS ", aux))
    return(aux)
}
.open_tbl_from_new_connection <- function(con, aux, sdf) {
    auxSchema <- in_schema(aux, ident(dbtable(sdf)))
    tblx <- tbl(con, auxSchema)
    tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblx, sdf)
    ## tblx$ops$args <- list(auxSchema)
    return(tblx)
}

.attach_and_open_tbl_in_new_connection <- function(con, sdf) {
    aux <- .attach_dbname_from_SQLDataFrame(con, sdf)
    ## dbs <- dbGetQuery(con, "PRAGMA database_list")
    .open_tbl_from_new_connection(con, aux, sdf)
}

.rbind_SQLDataFrame <- function(..., deparse.level = 1)
{
    browser()
    objects <- list(...)
    ## check consistent dbkey(), colnames(),
    keys <- lapply(objects, dbkey)
    if (length(unique(keys)) != 1)
        stop("Input SQLDataFrame objects must have identical dbkey()!")
    cnms <- lapply(objects, colnames)
    if (length(unique(cnms)) != 1 )
        stop("Input SQLDataFrame objects must have identical columns!")
    dbkey <- keys[[1]]
    cnm <- cnms[[1]]
    rnms_final <- do.call(c, lapply(objects, ROWNAMES))

    ## pairwise "union" with multiple input. 
    out <- union(objects[[1]], objects[[2]])
    objects <- objects[-c(1:2)]
    repeat{
        if(length(objects) == 0) break
        out <- union(out, objects[[1]])
        objects <- objects[-1]
    }

    ## Possible enhancement: save 'idx' as separate database table. 
    idx <- match(rnms_final, out@dbconcatKey)
    out@indexes[[1]] <- idx   ## need a slot setter here? so ridx(out) <- idx
    return(out)
}
setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)
