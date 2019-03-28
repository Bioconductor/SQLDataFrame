makeSQLDataFrameFromFile <- function(filename, dbkey, dbname = NULL,
                                     dbtable = NULL, overwrite = FALSE)
{
    fn <- filename
    stopifnot(file.exists(filename))
    if (is.null(dbname)) {
        dbname <- tempfile(fileext = ".db")
    } else if (file.exists(dbname)) {
        dbname <- file_path_as_absolute(dbname)
        if (overwrite == FALSE)
            stop("The 'dbname' already exists! Please provide a new value ",
                 "OR change 'overwrite = TRUE'. ")
    } else {
        file.create(dbname)
    }
    
    if (is.null(dbtable))
        dbtable <- basename(filename) ## OR dbplyr:::random_table_name()

    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    dbWriteTable(con, dbtable, value = filename)
    SQLDataFrame(dbname = dbname,
                 dbtable = dbtable,
                 dbkey = dbkey)  ## dbkey??
}
