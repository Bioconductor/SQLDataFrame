#' Construct SQLDataFrame from file.
#' @description Given a file name, \code{makeSQLDataFrame} will write
#'     the file contents into SQLite database, and open the database
#'     table as SQLDataFrame.
#' @param filename A \code{data.frame} or \code{DataFrame} object, or
#'     a character string of the filepath to the text file that to be
#'     saved as SQL database table.
#' @param dbkey A character vector of column name(s) that could
#'     uniquely identify each row of the filename. Must be provided in
#'     order to construct a SQLDataFrame.
#' @param dbname A character string of the filepath of to-be-saved
#'     database file. If not provided, will use a
#'     \code{tempfile(fileext = ".db")}.
#' @param dbtable A character string for the to be saved database
#'     table name. If not provided, will use the name of the input
#'     \code{data.frame} or \code{DataFrame} object, or the
#'     \code{basename(filename)} without extension if \code{filename}
#'     is a character string.
#' @param overwrite Whether to overwrite the \code{dbtable} if already
#'     exists. Default is FALSE.
#' @param sep a character string to separate the terms.  Not
#'     ‘NA_character_’. Default is \code{,}.
#' @param index Whether to create an index table. Default is TRUE.
#' @param ... additional arguments to be passed.
#' @importFrom tools file_path_as_absolute file_path_sans_ext
#' @importFrom tibble rownames_to_column
#' @import DBI
#' @details The provided file must has one or more columns to unique
#'     identify each row (no duplicate rows allowed). The file must be
#'     rectangular without rownames. (if rownames are needed, save it
#'     as a column.)
#' @examples
#' mtc <- tibble::rownames_to_column(mtcars)
#'
#' ## data.frame input
#' obj <- makeSQLDataFrame(mtc, dbkey = "rowname")
#' obj
#'
#' ## character input
#' filename <- file.path(tempdir(), "mtc.csv")
#' write.csv(mtc, file= filename, row.names = FALSE)
#' obj <- makeSQLDataFrame(filename, dbkey = "rowname")
#' obj
#' 
#' @export

makeSQLDataFrame <- function(filename,
                             dbkey = character(),
                             dbname = NULL,
                             dbtable = NULL,
                             overwrite = FALSE, sep = ",",
                             index = TRUE,
                             ...)
{
    ## browser()

    stopifnot(is.data.frame(filename) | is(filename, "DataFrame") | isSingleString(filename))
    stopifnot(isSingleString(dbkey))

    if (isSingleString(filename)) {
        stopifnot(file.exists(filename))
        if (is.null(dbtable))
            dbtable <- tools::file_path_sans_ext(basename(filename))
    } else {
        if (is.null(dbtable))
            dbtable <- deparse(substitute(filename))
        if (is(filename, "DataFrame"))
            filename <- as.data.frame(filename)
    }
    if (is.null(dbname)) {
        dbname <- tempfile(fileext = ".db")
    } else if (file.exists(dbname)) {
        dbname <- tools::file_path_as_absolute(dbname)
    } else {
        file.create(dbname)
    }

    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    dbWriteTable(con, dbtable, value = filename, overwrite = overwrite, sep = sep, ...)
    if (index)
        dbplyr:::db_create_indexes.DBIConnection(con, dbtable, indexes = list(dbkey), unique = TRUE)
    ## FIXME: take "overwrite" as input? NO...
    
    out <- SQLDataFrame(dbname = dbname, dbtable = dbtable,
                        dbkey = dbkey)
    msg <- msg_saveSQLDataFrame(out, dbname, dbtable)
    message(msg)
    return(out)
}

