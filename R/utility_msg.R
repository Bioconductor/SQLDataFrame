.msg_saveSQLDataFrame <- function(x, con, dbtable)
{
    type <- class(con)
    switch(type,
           MySQLConnection = .msg_save_mysql(x, con, dbtable),
           SQLiteConnection = .msg_save_sqlite(x, con, dbtable),
           BigQueryConnection = .msg_save_bigquery(x, con, dbtable)
           ) 
}

.msg_save_mysql <- function(x, con, dbtable) {
    info <- dbGetInfo(con)
    databaseLine <- paste0("mysql ", info$serverVersion, " [",
                           .mysql_info(con),
                           ":/", info$dbname, "] \n")  ## legacy format from lazy_tbl
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## sdf <- SQLDataFrame(\n",
                  "##   host = '", info$host, "',\n",
                  "##   user = '", info$user, "',\n",
                  "##   password = '',", "   ## Only if required!", "\n",
                  "##   type = 'MySQL',\n",
                  "##   dbname = '", info$dbname, "',\n",
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

.msg_save_sqlite <- function(x, con, dbtable) {
    databaseLine <- paste0("sqlite ", dbplyr:::sqlite_version(),
                           " [", con@dbname, "] \n")
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## sdf <- SQLDataFrame(\n",
                  "##   dbname = '", con@dbname, "',\n",
                  "##   type = 'SQLite',\n",
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

.msg_save_bigquery <- function(x, con, dbtable) {
    databaseLine <- paste0("BigQuery", " [", con@project, ".",
                           con@dataset, ".", dbtable, "] \n")
    msg <- paste0("## A new database table is saved! \n",
                  "## Source: table<", dbtable, "> [",
                  paste(dim(x), collapse = " X "), "] \n",
                  "## Database: ", databaseLine, 
                  "## Use the following command to reload into R: \n",
                  "## sdf <- SQLDataFrame(\n",
                  "##   host = '", con@project, "',\n",
                  "##   dbname = '", con@dataset, "',\n",
                  "##   type = 'BigQuery',\n",
                  "##   billing = '", con@project, "',\n", 
                  "##   dbtable = '", dbtable, "',\n",
                  "##   dbkey = ", ifelse(length(dbkey(x)) == 1, "", "c("),
                  paste(paste0("'", dbkey(x), "'"), collapse=", "),
                  ifelse(length(dbkey(x)) == 1, "", ")"), ")", "\n")
    message(msg)
}

