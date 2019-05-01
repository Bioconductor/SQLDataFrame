.available_tbls <- function(x)
{
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = x)
    tbls <- DBI::dbListTables(con)
    return(tbls)
}

.wheredbkey <- function(x) {
    stopifnot(is(x, "SQLDataFrame"))
    match(dbkey(x), colnames(x@tblData))
}

.con_SQLDataFrame <- function(x)
{
    x@tblData$src$con
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
