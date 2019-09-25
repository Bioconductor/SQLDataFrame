## this function switches between the connection type of input
## SQLDataFrame. If we define generic function and dispatch to
## different connection type, the original info (e.g., @indexes) from
## SQLDataFrame will be lost...

.join_union_prepare <- function(x, y, localConn)
{
    ## X and Y must built from same SQL database (e.g., SQLite, MySQL,
    ## etc.)
    connTypeX <- class(connSQLDataFrame(x))
    connTypeY <- class(connSQLDataFrame(y))
    stopifnot(identical(connTypeY, connTypeY))
    switch(connTypeX,
           "SQLiteConnection" = .join_union_prepare_sqlite(x, y),
           "MySQLConnection" = .join_union_prepare_mysql(x, y, localConn)
           )
} 

.join_union_prepare_sqlite <- function(x, y)
{
    if (is(tblData(x)$ops, "op_double") | is(tblData(x)$ops, "op_single")) {
        ## may change: !is(tblData(x)$ops, "op_base")
        con <- connSQLDataFrame(x)
        tblx <- .open_tbl_from_connection(con, "main", x)
        if (is(tblData(y)$ops, "op_double") | is(tblData(y)$ops, "op_single")) {
                ## attach all databases from y except "main", which is
                ## temporary connection from "union" or "join"
                dbs <- .dblist(con)
                cony <- connSQLDataFrame(y)
                tbly <- .extract_tbl_from_SQLDataFrame_indexes(tblData(y), y)
                dbsy <- .dblist(cony)[-1,]
                
                idx <- match(paste(dbsy$name, dbsy$file, sep=":"),
                             paste(dbs$name, dbs$file, sep=":"))
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
    } else if (is(tblData(y)$ops, "op_double") | is(tblData(y)$ops, "op_single")) {
        con <- connSQLDataFrame(y)
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

##-----------------------------------------------------##
## These utility functions are for SQLite connections. 
.attachMaybe_and_open_tbl_in_new_connection <- function(con, sdf) {
    dbs <- .dblist(con)
    dbname <- connSQLDataFrame(sdf)@dbname
    aux <- dbs[match(dbname, dbs$file), "name"]
    if (is.na(aux))
        aux <- .attach_database(con, dbname)
    res_tbl <- .open_tbl_from_connection(con, aux, sdf)
    return(res_tbl)
}
.dblist <- function(con) {
    res <- dbGetQuery(con, "PRAGMA database_list")
    return(res)
}
.dblist_SQLDataFrame <- function(sdf) {
    con <- connSQLDataFrame(sdf)
    .dblist(con)
}
.attach_database <- function(con, dbname, aux = NULL) {
    if (is.null(aux))
        aux <- dplyr:::random_table_name()
    dbExecute(con, paste0("ATTACH '", dbname, "' AS ", aux))
    return(aux)
}
.open_tbl_from_connection <- function(con, aux, sdf) {
    if (aux == "main") {
        tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblData(sdf), sdf)
    } else {
        auxSchema <- in_schema(aux, ident(dbtable(sdf)))
        tblx <- tbl(con, auxSchema)
        tblx <- .extract_tbl_from_SQLDataFrame_indexes(tblx, sdf)
    }
    return(tblx)
}

##################################################################################################
.join_union_prepare_mysql <- function(x, y,
                                localConn) ## only used when both X and Y has no write permission.
{
    fedtablex <- dplyr:::random_table_name()
    fedtabley <- dplyr:::random_table_name()
    
    conx <- connSQLDataFrame(x)
    cony <- connSQLDataFrame(y)
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
                                               remotePswd = .get_mysql_var(connSQLDataFrame(x)))
        fedy <- .createFedTable_and_reopen_tbl(y,
                                               localConn,
                                               fedtabley,
                                               remotePswd = .get_mysql_var(connSQLDataFrame(y)))
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
    .create_federated_table(remoteConn = connSQLDataFrame(sdf),
                            dbtableName = dbtable(sdf),
                            localConn = localConn, 
                            ldbtableName = ldbtableName,
                            remotePswd = remotePswd) 
    res_tbl <- tbl(localConn, ldbtableName)  ## time consuming...
    res_tbl <- .extract_tbl_from_SQLDataFrame_indexes(res_tbl, sdf) ## time consuming...
    return(res_tbl)
}
