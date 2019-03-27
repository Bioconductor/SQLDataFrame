## "union" method is defined for both S3 and S4 classes. Since
## SQLDataFrame is a S4 object, here I am using "setMethod" instead of
## "union.SQLDataFrame".
## "union" itself removes duplicates, and reorder by default using 1st column, then 2nd, ...
## SQL::union_all, I guess, no automatic ordering??

.union_SQLDataFrame <- function(x, y, copy = FALSE)
{
    ## browser()
    out <- .doCompatibleFunction(x, y, copy = copy,
                                 FUN = dbplyr:::union.tbl_lazy)  ## dbplyr:::union.tbl_lazy
    ## @dbconcatKey
    rnms <- unique(c(ROWNAMES(x), ROWNAMES(y)))
    tt <- as.data.frame(do.call(rbind, strsplit(rnms, split = "\b")),
                        stringsAsFactors = FALSE)
    cls <- out@tblData %>% head %>% select(dbkey(x)) %>% as.data.frame() %>% sapply(class)
    for (i in seq_len(length(tt))) class(tt[,i]) <- unname(cls)[i]
    od <- do.call(order, tt)
    dbrnms <- rnms[od]
    
    BiocGenerics:::replaceSlots(out, dbconcatKey = dbrnms)
}

#' @export
setMethod("union", signature = c("SQLDataFrame", "SQLDataFrame"), .union_SQLDataFrame)

.join_union_prepare <- function(x, y)
{
    ## browser()  
    if (is(x@tblData$ops, "op_double")) {
        con <- .con_SQLDataFrame(x)
        tblx <- .open_tbl_from_connection(con, "main", x)
        
        if (is(y@tblData$ops, "op_double")) {
            ## attach all databases from y except "main", which is
            ## temporary connection from "union" or "join"
            dbs <- .dblist(con)
            cony <- .con_SQLDataFrame(y)
            tbly <- .extract_tbl_from_SQLDataFrame(y)
            dbsy <- .dblist(cony)[-1,]
            
            idx <- match(paste(dbsy$name, dbsy$file, sep=":"), paste(dbs$name, dbs$file, sep=":"))
            idx <- which(!is.na(idx))          
            if (length(idx)) dbsy <- dbsy[-idx, ]
            for (i in seq_len(nrow(dbsy))) {
                .attach_database(con, dbsy[i, "file"], dbsy[i, "name"])
            }
            ## open the lazy tbl from new connection
            sql_cmd <- dbplyr::db_sql_render(cony, tbly)
            tbly <- tbl(con, sql_cmd)
        } else {
            tbly <- .attachMaybe_and_open_tbl_in_new_connection(con, y)
        }
    } else if (is(y@tblData$ops, "op_double")) {  
        con <- .con_SQLDataFrame(y)
        tbly <- .open_tbl_from_connection(con, "main", y)
        tblx <- .attachMaybe_and_open_tbl_in_new_connection(con, x)
    } else {
        dbname <- tempfile(fileext = ".db")
        con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
        tblx <- .attachMaybe_and_open_tbl_in_new_connection(con, x)
        tbly <- .attachMaybe_and_open_tbl_in_new_connection(con, y)
    }
    return(list(tblx, tbly))
}

.attachMaybe_and_open_tbl_in_new_connection <- function(con, sdf) {
    ## browser()
    dbs <- .dblist(con)
    aux <- dbs[match(dbname(sdf), dbs$file), "name"]
    if (is.na(aux))
        aux <- .attach_database(con, dbname(sdf))
    res_tbl <- .open_tbl_from_connection(con, aux, sdf)
    return(res_tbl)
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
        auxSchema <- in_schema(aux, ident(dbtable(sdf)))
        tblx <- tbl(con, auxSchema)
        tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblx, sdf)
    }
    return(tblx)
}

## .attach_and_open_tbl_in_new_connection <- function(con, sdf) {
##     aux <- .attach_database(con, dbname(sdf))
##     ## dbs <- dbGetQuery(con, "PRAGMA database_list")
##     .open_tbl_from_connection(con, aux, sdf)
## }

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

    idx <- match(rnms_final, out@dbconcatKey)
    out@indexes[[1]] <- idx
    return(out)
}

#' @export
setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)
