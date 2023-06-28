##-----------------------------------------------------##
## utility function to call directly from union/join
##-----------------------------------------------------##

.doCompatibleFunction <- function(x, y, localConn, ..., FUN) {
    tbls <- .join_union_prepare(x, y, localConn)
    tbl.out <- FUN(tbls[[1]], tbls[[2]], ...)
    ## dbnrows <- tbl.out %>% summarize(n=n()) %>% pull(n) %>% as.integer ## FIXME
    dbnrows <- tbl.out %>% transmute(cons = 1.0) %>% count(cons) %>% pull(n) %>% as.integer

    out <- BiocGenerics:::replaceSlots(x, tblData = tbl.out,
                                       dbnrows = dbnrows,
                                       indexes = vector("list", 2))
    return(out)
}

## this function switches between the connection type of input
## SQLDataFrame. If we define generic function and dispatch to
## different connection type, the original info (e.g., @indexes) from
## SQLDataFrame will be lost...

.join_union_prepare <- function(x, y, localConn)
{
    ## X and Y must built from same SQL database (e.g., SQLite, MySQL,
    ## etc.)
    connTypeX <- class(dbcon(x))
    connTypeY <- class(dbcon(y))
    stopifnot(identical(connTypeY, connTypeY))
    switch(connTypeX,
           "SQLiteConnection" = .join_union_prepare_sqlite(x, y),
           "MySQLConnection" = .join_union_prepare_mysql(x, y, localConn)
           )
} 

##-----------------------------------------------------##
## These utility functions are for SQLite connections. 
##-----------------------------------------------------##

.join_union_prepare_sqlite <- function(x, y)
{
    ## If x y from same connection
    if (identical(dbcon(x), dbcon(y))) {
        con <- dbcon(x)
        tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
        tbly <- .extract_tbl_from_SQLDataFrame_indexes(tblData(y), y)
    } else {
        ## if different connection, then choose and open the
        ## connection with attached databases from previous lazy
        ## operations (rbind, union, join, etc.) and attach a new
        ## database for the other object (x or y).
        dbs_xcon <- .dblist(dbcon(x))$file
        dbs_xcon <- dbs_xcon[dbs_xcon != ""]
        dbs_ycon <- .dblist(dbcon(y))$file
        dbs_ycon <- dbs_ycon[dbs_ycon != ""]
        if (all(length(dbs_xcon) >1, length(dbs_ycon) > 1))
            stop(.msg_realizeSQLDataFrame)
        if (length(dbs_ycon) > 1) {
            con <- dbcon(y)
            tbly <- .extract_tbl_from_SQLDataFrame_indexes(tblData(y), y)
            tblx <- .attachMaybe_and_open_tbl_in_new_connection(con, x)
        } else {
            con <- dbcon(x)
            tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
            tbly <- .attachMaybe_and_open_tbl_in_new_connection(con, y)
        }
    }
    return(list(tblx, tbly))
}

.msg_realizeSQLDataFrame <- paste0(
    "Please call 'saveSQLDataFrame()' ",
    "to realize the existing lazy operations ",
    "on SQLDataFrame (e.g., from 'rbind', 'join', 'union') ",
    "before further operations! \n")

.attachMaybe_and_open_tbl_in_new_connection <- function(con, sdf) {
    ## check if the y database is already attached to the x connection
    dbs <- .dblist(con)
    dbname <- dbcon(sdf)@dbname
    ## if yes, use the existing "aux" name
    aux <- dbs[match(dbname, dbs$file), "name"]
    ## If not, attach here, using a random name (other than "main")
    if (is.na(aux))
        aux <- .attach_database(con, dbname)
    ## open the y database table in x connection
    res_tbl <- .open_tbl_from_connection(con, aux, sdf)
    return(res_tbl)
}

.dblist <- function(con) {
    res <- dbGetQuery(con, "PRAGMA database_list")
    return(res)
}

.attach_database <- function(con, dbname, aux = NULL) {
    if (is.null(aux))
        aux <- dplyr:::random_table_name()
    dbExecute(con, paste0("ATTACH '", dbname, "' AS ", aux))
    return(aux)
}

.open_tbl_from_connection <- function(con, aux, sdf) 
{
    tblname <- tryCatch(dbtable(sdf),
                        warning=function(w)
                            stop(.msg_realizeSQLDataFrame))
    auxSchema <- in_schema(aux, ident(tblname))
    res_tbl <- tbl(con, auxSchema)
    res_tbl <- .extract_tbl_from_SQLDataFrame_indexes(res_tbl, sdf)
    return(res_tbl)
}

##-----------------------------------------------------##
## These utility functions are for MySQL connections. 
##-----------------------------------------------------##

.join_union_prepare_mysql <- function(x, y,
                                localConn) ## only used when both X and Y has no write permission.
{
    fedtablex <- dplyr:::random_table_name()
    fedtabley <- dplyr:::random_table_name()
    
    conx <- dbcon(x)
    cony <- dbcon(y)
    tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblData(x), x)
    tbly <- .extract_tbl_from_SQLDataFrame_indexes(tblData(y), y)

    if (identical(conx, cony)) return(list(tblx, tbly))

    ifwrite <- c(conx = .mysql_has_write_perm(conx),
                 cony = .mysql_has_write_perm(cony))
    conwrite <- names(ifwrite[ifwrite])[1]
    if (conwrite == "conx") {
        tbly <- .createFedTable_and_reopen_tbl(y,
                                               conx,
                                               fedtabley,
                                               remotePswd = .get_mysql_var(cony))
    } else if (conwrite == "cony") {
        tblx <- .createFedTable_and_reopen_tbl(x,
                                               cony,
                                               fedtablex,
                                               remotePswd = .get_mysql_var(conx))
    } else { ## situation will be rare, joining two SQLDataFrame
             ## objects, neither of which has write permission to
             ## their connections. Not recommended!
        if (missing(localConn) | !.mysql_has_write_perm(localConn))
            stop("Please provide a MySQL connection ",
                     "with write permission in argument: localConn")
        fedx <- .createFedTable_and_reopen_tbl(x,
                                               localConn,
                                               fedtablex,
                                               remotePswd = .get_mysql_var(dbcon(x)))
        fedy <- .createFedTable_and_reopen_tbl(y,
                                               localConn,
                                               fedtabley,
                                               remotePswd = .get_mysql_var(dbcon(y)))
    }
    return(list(tblx, tbly))
}    

## when creating a federated table, it needs a localConn where the
## federated table locates, and the remote password for construction
## the "CONNECTION" info.
.createFedTable_and_reopen_tbl <- function(sdf,
                                           localConn,
                                           ldbtableName,
                                           remotePswd = NULL) {
    .create_federated_table(remoteConn = dbcon(sdf),
                            dbtableName = dbtable(sdf),
                            localConn = localConn, 
                            ldbtableName = ldbtableName,
                            remotePswd = remotePswd) 
    res_tbl <- tbl(localConn, ldbtableName)  ## time consuming...
    res_tbl <- .extract_tbl_from_SQLDataFrame_indexes(res_tbl, sdf) ## time consuming...
    return(res_tbl)
}

