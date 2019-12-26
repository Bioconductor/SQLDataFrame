.doCompatibleFunction <- function(x, y, ..., FUN)
{
    ## browser()
    new_tblData <- FUN(tblData(x), tblData(y), ...)

    if (!identical(dbkey(x), dbkey(y))) {
        new_dbkey <- c(dbkey(x), dbkey(y))
    } else {
        new_dbkey <- dbkey(x)
    }

    ## if (identical(FUN, dbplyr:::union_all.tbl_lazy))
    ##     new_tblData <- new_tblData %>% distinct(!!!syms(new_dbkey), .keep_all=TRUE)
    ## FIXME: Error: Can only find distinct value of specified columns if .keep_all is FALSE
    
    if (!is.null(pid(x))) {
        new_pid <- pid(x)
    } else if (!is.null(pid(y))) {
        new_pid <- pid(y)
    } else {
        new_pid <- NULL
    }
    ## @keyData
    new_keyData <- .update_keyData(new_tblData, new_dbkey, new_pid)
    ## @pidRle
    new_pidRle <- .update_pidRle(new_keyData, new_pid)
    ## @dim, @dimnames
    new_nr <- new_keyData %>% ungroup %>% summarize(n = n())
    new_nr <- collectm(new_nr) %>% pull(n) %>% as.integer
    new_nc <- ncol(new_tblData) - length(new_dbkey)
    new_cns <- setdiff(colnames(new_tblData), new_dbkey)
    ## for "filter", no need to honor the existing ridx(x), so will
    ## reset as NULL.
    BiocGenerics:::replaceSlots(x, tblData = new_tblData,
                                keyData = new_keyData,
                                pidRle = new_pidRle,
                                dim = c(new_nr, new_nc),
                                dimnames = list(NULL, new_cns),
                                ridx = NULL)
}

