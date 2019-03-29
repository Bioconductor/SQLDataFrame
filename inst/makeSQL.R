## found <- dbExistsTable(conn, name)

con <- DBI::dbConnect(RSQLite::SQLite(), dbname = tempfile(fileext=".db"))
con
## <SQLiteConnection>
##   Path: /tmp/RtmpnrGa2p/fileba561bf342f.db
##   Extensions: TRUE

dbWriteTable(con, "mtcars", value = "inst/extdata/mtcars.txt", sep = "\t", row.names = FALSE)

dbWriteTable(con, "mtcars", value = "inst/extdata/mtcars_nornms.txt", sep = "\t", overwrite = TRUE)

dbWriteTable(con, "mtcarsCsv", value = "inst/extdata/mtcars.csv", overwrite = TRUE)

connection_import_file(conn@ptr, name, value, sep, eol, skip)

## > conn
## <SQLiteConnection>
##   Path: /tmp/RtmpnrGa2p/fileba557570b9f.db
##   Extensions: TRUE


mtc <- tibble::rownames_to_column(mtcars)

## DataFrame input
obj <- makeSQLDataFrame(mtc, dbkey = "rowname")
obj

## character input
filename <- file.path(tempdir(), "mtc.csv")
write.csv(mtc, file= filename, row.names = FALSE)
obj <- makeSQLDataFrame(filename, dbkey = "rowname")
obj

## write index. but could not take "overwrite". need to manually "DROP INDEX table_name.index_name".
dbname <- file.path(tempdir(), "test.db")
con <- dbConnect(RSQLite::SQLite(), dbname = dbname)
dbWriteTable(con, "mtcars", mtc, overwrite = TRUE)
dbplyr:::db_create_indexes.DBIConnection(con, "mtcars", indexes = list("rowname"))
