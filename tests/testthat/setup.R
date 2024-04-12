example_df <- mtcars

## SQLite
tf <- tempfile()
con <- DBI::dbConnect(RSQLite::SQLite(), tf)
DBI::dbWriteTable(con, "mtcars", example_df, overwrite=TRUE)
DBI::dbDisconnect(con)

## DuckDB
tf1 <- tempfile()
con1 <- DBI::dbConnect(duckdb::duckdb(), tf1)
DBI::dbWriteTable(con1, "mtcars", example_df, overwrite=TRUE)
DBI::dbDisconnect(con1)

## Parquet
tf2 <- tempfile()
arrow::write_dataset(mtcars, tf2, format = "parquet")
 
