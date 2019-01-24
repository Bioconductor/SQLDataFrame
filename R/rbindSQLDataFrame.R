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

    ## use temporary dir and random table name. 
    dbname <- tempfile(fileext = ".db")
    dbtable <- dplyr:::random_table_name()

    ## open a new connection to write database table.
    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    ## attach the dbname of the to-be-copied "lazy tbl" to the new connection.
    ## FIXME: possible to attach an online database?
    auxName <- "aux"
    DBI::dbExecute(con, paste0("ATTACH '", sdf1@tblData$src$con@dbname, "' AS ", auxName))
    ## open the to-be-copied "lazy tbl" from new connection.
    tbl1 <- tbl(con, in_schema("aux", sdf1@tblData$ops$x))
    ## apply all @indexes to "tbl_dbi" object (that opened from destination connection).
    tbl1 <- .extract_tbl_from_SQLDataFrame_indexes(tbl1, sdf1) ## reorder by "key + otherCols"

    copy_to(con, tbl1, name = dbtable,
            temporary = FALSE, overwrite = TRUE,
            unique_indexes = NULL, indexes = list(dbkey(sdf1)),
            analyze = TRUE)

    for (i in seq_len(length(objects))[-1]) {
        src_append <- objects[[i]]@tblData$src
        if (! same_src(src_append, sdf1@tblData$src)) {
            auxName <- paste0("aux", i)
            DBI::dbExecute(con, paste0("ATTACH '", src_append$con@dbname, "' AS ", auxName))
        }
        tbl_append <- tbl(con, in_schema(auxName, objects[[i]]@tblData$ops$x))
        tbl_append <- .extract_tbl_from_SQLDataFrame_indexes(tbl_append, objects[[i]])

        ## vars <- op_vars(tbl_append)
        ## tbl_append_aliased <- select(tbl_append, !!!syms(vars))
        ## sql <- db_sql_render(con, tbl_append_aliased$ops)
        sql_tbl_append <- db_sql_render(con, tbl_append)
        dbExecute(con, build_sql("INSERT INTO ", sql(dbtable), " ", sql_tbl_append))
    }

    ## message 
    nrow <- tbl(con, dbtable) %>% summarize(n=n()) %>% pull(n)
    ncol <- length(colnames(tbl1))
    msg <- paste0("## A temporary database table is generated: \n",
                  "## Source: table<", dbtable, "> [", nrow, " X ", ncol, "] \n",
                  "## Database: ", db_desc(con), "\n",
                  "## Use the following command to reload into R: \n",
                  "## dat <- SQLDataFrame(\n",
                  "##   dbname = \"", dbname, "\",\n",
                  "##   dbtable = \"", dbtable, "\",\n",
                  "##   dbkey = ", ifelse(length(dbkey(sdf1)) == 1, "", "c("),
                  paste(paste0("'", dbkey(sdf1), "'"), collapse=", "),
                  ifelse(length(dbkey(sdf1)) == 1, "", ")"), ")", "\n")

    message(msg)
    dat <- SQLDataFrame(dbname = dbname, dbtable = dbtable, dbkey = dbkey(sdf1))
    return(dat)
}

setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)


## random_table_name <- function(n = 10) {
##     paste0(sample(letters, n, replace = TRUE), collapse = "")



