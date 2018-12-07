wheredbkey <- function(x) {
    stopifnot(is(x, "SQLDataFrame"))
    match(dbkey(x), colnames(x@tblData))
}
