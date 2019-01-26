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
    DBI::dbExecute(con, paste0("ATTACH '", dbname(sdf1), "' AS ", auxName))
    ## open the to-be-copied "lazy tbl" from new connection.
    tbl1 <- tbl(con, in_schema("aux", ident(dbtable(sdf1))))
    ## apply all @indexes to "tbl_dbi" object (that opened from destination connection).
    tbl1 <- .extract_tbl_from_SQLDataFrame_indexes(tbl1, sdf1) ## reorder by "key + otherCols"

    copy_to(con, tbl1, name = dbtable,
            temporary = FALSE, overwrite = TRUE,
            unique_indexes = NULL, indexes = list(dbkey(sdf1)),
            analyze = TRUE)
    rnms <- ROWNAMES(sdf1)[order(ridx(sdf1))]
    
    for (i in seq_len(length(objects))[-1]) {
        src_append <- src_dbi(.con_SQLDataFrame(objects[[i]]))
        if (! same_src(src_append, sdf1@tblData$src)) {
            auxName <- paste0("aux", i)
            DBI::dbExecute(con, paste0("ATTACH '", dbname(objects[[i]]), "' AS ", auxName))
        }
        rnms_append <- ROWNAMES(objects[[i]])[order(ridx(objects[[i]]))]
        rnms_update <- union(rnms, rnms_append)
        rnms_diff <- setdiff(rnms_update, rnms)

        tbl_append <- tbl(con, in_schema(auxName, ident(dbtable(objects[[i]]))))
        tbl_append <- .extract_tbl_from_SQLDataFrame_indexes(tbl_append, objects[[i]][rnms_diff, ])

        sql_tbl_append <- dbplyr::db_sql_render(con, tbl_append)
        ## equivalent to dbplyr::sql_render(tbl_append, con)
        dbExecute(con, build_sql("INSERT INTO ", sql(dbtable), " ", sql_tbl_append))
        rnms <- rnms_update
    }
    rnms_final <- do.call(c, lapply(objects, ROWNAMES))
    idx <- match(rnms_final, rnms)
    
    ## message 
    nrow <- tbl(con, dbtable) %>% summarize(n=n()) %>% pull(n)
    ## nrow <- do.call(sum, lapply(objects, nrow))
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
                  ifelse(length(dbkey(sdf1)) == 1, "", ")"), ")", "\n",
                  "## dat <- dat[c(",
                  paste(idx, collapse = ", "), "), ]", "\n")

    message(msg)
    dat <- SQLDataFrame(dbname = dbname, dbtable = dbtable, dbkey = dbkey(sdf1))
    res <- dat[idx, ]
    return(res)
}

setMethod("rbind", signature = "SQLDataFrame", .rbind_SQLDataFrame)

## random_table_name <- function(n = 10) {
##     paste0(sample(letters, n, replace = TRUE), collapse = "")



