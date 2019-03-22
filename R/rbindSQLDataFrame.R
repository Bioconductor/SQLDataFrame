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
    tbls <- .join_union_prepare(x, y)
    tbl.out <- dbplyr:::union.tbl_lazy(tbls[[1]], tbls[[2]])
  
    x@tblData <- tbl.out
    x@dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n)
    x@indexes <- vector("list", 2)

    ## recalculate the @dbconcatKey
    x@dbconcatKey <- tbl.out %>%
        mutate(concatKey = paste(!!!syms(dbkey(x)), sep="\b")) %>%
        pull(concatKey)  
    ## ## extract the new @dbconcatKey instead of recalculating
    ## tt <- do.call(rbind, strsplit(c(ROWNAMES(x), ROWNAMES(y)), split = "\b"))
    ## tt <- as.data.frame(tt, stringsAsFactors = FALSE)
    ## cls <- x1 %>% head %>% select(dbkey(x)) %>% as.data.frame() %>% sapply(class)
    ## for (i in seq_len(length(tt))) class(tt[,i]) <- unname(cls)[i]
    ## tt[with(tt, order(!!!syms(dbkey(x)))), ] ## doesn't work ... 
    
    return(x)
})

.join_union_prepare <- function(x, y)
{
    ## browser()  
    if (is(x@tblData$ops, "op_double")) {
        con <- .con_SQLDataFrame(x)
        x1 <- .extract_tbl_from_SQLDataFrame(x)  ## lazy tbl. 
        dbs <- .dblist(con)

        if (is(y@tblData$ops, "op_double")) {
            ## attach all databases from y except "main", which is
            ## temporary connection from "union" or "join"
            cony <- .con_SQLDataFrame(y)
            y1 <- .extract_tbl_from_SQLDataFrame(y)
            dbsy <- .dblist(cony)[-1,]
            
            idx <- match(paste(dbsy$name, dbsy$file, sep=":"), paste(dbs$name, dbs$file, sep=":"))
            idx <- which(!is.na(idx))          
            if (length(idx)) dbsy <- dbsy[-idx, ]
            if (nrow(dbsy)) {
                for (i in seq_len(nrow(dbsy))) {
                    .attach_database(con, dbsy[i, "file"], dbsy[i, "name"])
                }
            }
            ## open the lazy tbl from new connection
            sqlCmd <- dbplyr::db_sql_render(cony, y1)
            y1 <- tbl(con, sqlCmd)
        } else {
            aux_y <- dbs[match(dbname(y), dbs$file), "name"]
            if (is.na(aux_y)) {
                y1 <- .attach_and_open_tbl_in_new_connection(con, y)
            } else {
                y1 <- .open_tbl_from_connection(con, aux_y, y)
            }
        }
    } else if (is(y@tblData$ops, "op_double")) {  
        con <- .con_SQLDataFrame(y)
        y1 <- .extract_tbl_from_SQLDataFrame(y)
        dbs <- .dblist(con)
        aux_x <- dbs[match(dbname(x), dbs$file), "name"]
        if (is.na(aux_x)) {
            x1 <- .attach_and_open_tbl_in_new_connection(con, x)
        } else {
            x1 <- .open_tbl_from_connection(con, aux_x, x)
        }
    } else {
        dbname <- tempfile(fileext = ".db")
        con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)        
        ## attach database into the local connection.
        x1 <- .attach_and_open_tbl_in_new_connection(con, x)
        dbs <- .dblist(con)
        aux_y <- dbs[match(dbname(y), dbs$file), "name"]
        if (is.na(aux_y)) {
            y1 <- .attach_and_open_tbl_in_new_connection(con, y)
        } else {
            y1 <- .open_tbl_from_connection(con, aux_y, y)
        }
    }
    return(list(x1, y1))
}

.dblist <- function(con) {
    res <- dbGetQuery(con, "PRAGMA database_list")
    return(res)
}
.dblist_SQLDataFrame <- function(sdf) {
    con <- .con_SQLDataFrame(sdf)
    .dblist(con)
}
.attach_database <- function(con, dbname, aux = NULL) {
    if (is.null(aux))
        aux <- dbplyr:::random_table_name()
    dbExecute(con, paste0("ATTACH '", dbname, "' AS ", aux))
    return(aux)
}
.open_tbl_from_connection <- function(con, aux, sdf) {
    if (aux == "main") {
        tblx <- .extract_tbl_from_SQLDataFrame(sdf)
    } else {
        ## auxSchema <- in_schema(aux, ident(dbtable(sdf)))
        auxSchema <- in_schema(aux, ident(dbtable(sdf)[1]))
        ## dirty and lazy fix ... works only when sdf connects to 1 dbtable...  
        ## FIXME: dbtable() for SDF generated from union/join, how to extract the dbtable()? 
        tblx <- tbl(con, auxSchema)
        tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblx, sdf)
    }
    ## tblx$ops$args <- list(auxSchema)
    return(tblx)
}

.attach_and_open_tbl_in_new_connection <- function(con, sdf) {
    aux <- .attach_database(con, dbname(sdf))
    ## dbs <- dbGetQuery(con, "PRAGMA database_list")
    .open_tbl_from_connection(con, aux, sdf)
}

.rbind_SQLDataFrame <- function(..., deparse.level = 1)
{
    ## browser()
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

#' @export
setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)
