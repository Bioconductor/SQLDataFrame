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
    sdf1 <- objects[[1L]]
    tbl1 <- .extract_tbl_from_SQLDataFrame(sdf1)
    tbls_apend <- lapply(objects[-1L], .extract_tbl_from_SQLDataFrame)
    ## open a new connection, or "src_dbi", and write database table.
    dbname <- tempfile(fileext = ".db")
    dbtable <- dplyr:::random_table_name()
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)  ## dbname, using tempdir().
    copy_to(con, tbl1, name = dbtable,    ## dbtable, randomly-generated table name.
            temporary = FALSE, overwrite = TRUE,
            unique_indexes = NULL, indexes = list(dbkey(sdf1)),
            analyze = TRUE)
    for (i in seq_len(length(tbls_apend))) {
        ## copy_to(con, collect(tbls_apend[[i]]), name = dbtable, temporary = FALSE, append = TRUE)
        dplyr::db_insert_into(con, table = dbtable, value = collect(tbls_apend[[i]]), temporary = FALSE, append = TRUE)
        ## DBI::dbWriteTable()
        ## DBI::db_insert_into()
    }
    ## msg1: a new table is generated in a new database, use 'dbname()', 'dbtable()' to see details. 
    msg <- paste0("## A temporary database table is generated. \n",
                  "## Use dbname() and dbtable() to return the path. \n")
    message(msg)
    dat <- SQLDataFrame(dbname = dbname, dbtable = dbtable, dbkey = dbkey(sdf1))
    return(dat)
}

setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)


## random_table_name <- function(n = 10) {
##     paste0(sample(letters, n, replace = TRUE), collapse = "")

## wants to do: copy_to(dest=con, df="tbl_dbi"lazy tbl, name=deparse(substitute(df)), append = T)
## would check identical(con, "tbl_dbi"$src$con), if TRUE, compute(df, name, temporary, indexes, analyze, ?append, ..)
## we can make "compute()" call by attaching the "tbl_dbi" connected database to the "con" database before calling of "copy_to()" ??
## next, would "compute(append=T)" work?


append
