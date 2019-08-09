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

.con_SQLDataFrame <- function(x)
{
    tblData(x)$src$con
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
    ## ordered by "key + otherCols"
    return(tbl)
}

## create federated table from connection (for 1 SQLDataFrame)
.create_federated_table <- function(mysqlConn, username, host, database, dbtableName,
                                    localConn, localDbtable)
{
    return(NULL)
    ## open docker, require credentials here:
    
    stopifnot(is(mysqlConn, "MySQLConnection"))
    ## show create table in mysqlConn for column options
    createinfo <- dbGetQuery(mysqlConn,
                             build_sql("SHOW CREATE TABLE ", sql(dbtableName), con = mysqlConn))
    columninfo <- createinfo[createinfo$Table == dbtableName, "Create Table"]
    columninfo <- gsub("ENGINE=.+ ", "ENGINE=FEDERATED ", columninfo)
    ## create table (temporary?) in localConn, with same column options, using federated engine. 
    dbExecute(localConn, build_sql(sql(columninfo),
                                   sql(paste0(" connection='mysql://",
                                              username, "@", host,
                                              "/", database, "/",
                                              dbtableName, "'")),
                                   con = localConn))
    ## dbCreateTable(localConn, name = local_dbtable,
    ##               fields = dbColumnInfo(mysqlConn, dbtable),
    ##               temporary = TRUE)
}

    

## issues:
## 1. open a local mysql connetion under User="liuqian". (JessyMysql)
## 2. create a table using "ENGINE=FEDERATED" in R using DBI.
## 3. 


## check if same connection

## saveSQLDataFrame, realize a lazy SQLDataFrame using federated table. create table. 
