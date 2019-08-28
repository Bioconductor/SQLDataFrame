.available_tbls <- function(x)
{
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = x)
    tbls <- DBI::dbListTables(con)
    return(tbls)
}

.wheredbkey <- function(x) {
    stopifnot(is(x, "SQLDataFrame"))
    match(dbkey(x), colnames(tblData(x)))
}

ridx <- function(x)
{
    x@indexes[[1]]
}

normalizeRowIndex <- function(x)
{
    ridx <- ridx(x)
    if (is.null(ridx))
        ridx <- seq_len(x@dbnrows)
    return(ridx)
}

.extract_tbl_from_SQLDataFrame_indexes <- function(tbl, sdf)
{
    ridx <- ridx(sdf)
    if (!is.null(ridx))
        tbl <- .extract_tbl_rows_by_key(tbl, dbkey(sdf),
                                        dbconcatKey(sdf), ridx)
    tbl <- tbl %>% select(dbkey(sdf), colnames(sdf))
    ## columns ordered by "key + otherCols"
    return(tbl)
}

.create_federated_table <- function(remoteConn, dbtableName,
                                    localConn, ldbtableName, remotePswd=NULL)
{
    ## browser()
    ## open docker, require credentials here:
    
    stopifnot(is(remoteConn, "MySQLConnection"))
    ## show create table in remoteConn for column options
    createinfo <- dbGetQuery(remoteConn,
                             build_sql("SHOW CREATE TABLE ", sql(dbtableName), con = remoteConn))
    columninfo <- createinfo[createinfo$Table == dbtableName, "Create Table"]
    if (!missing(ldbtableName))
        ## columninfo <- gsub("CREATE TABLE .+ (",
        columninfo <- gsub(paste0("CREATE TABLE `", dbtableName, "`"),
                           paste0("CREATE TABLE `", ldbtableName, "`"),
                           columninfo)
    columninfo <- gsub("ENGINE=.+ ", "ENGINE=FEDERATED ", columninfo)
    ## create table (temporary?) in localConn, with same column options, using federated engine.
    remoteInfo <- dbGetInfo(remoteConn)
    sql_conn <-build_sql(sql(columninfo),
                         sql(paste0(" connection='mysql://",
                                    remoteInfo$user,
                                    ifelse(is.null(remotePswd), "",
                                           paste0(":", remotePswd)),
                                    "@", remoteInfo$host, "/",
                                    remoteInfo$dbname, "/",
                                    dbtableName, "'")),
                         con = localConn)
    dbExecute(localConn, sql_conn)
}
