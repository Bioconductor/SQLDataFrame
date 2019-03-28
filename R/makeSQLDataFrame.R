#' Construct SQLDataFrame from file.
#' @description Given a file name, \code{makeSQLDataFrame} will write
#'     the file contents into SQLite database, and open the database
#'     table as SQLDataFrame.
#' @param filename A character string of the filepath to the text file
#'     that to be saved as SQL database table.
#' @param dbkey A character vector of column name(s) that could
#'     uniquely identify each row of the filename. Must be provided in
#'     order to construct a SQLDataFrame.
#' @param dbname A character string of the filepath of to-be-saved
#'     database file. If not provided, will use a
#'     \code{tempfile(fileext = ".db")}.
#' @param dbtable A character string for the to be saved database
#'     table name. If not provided, will use the
#'     \code{basename(filename)} without extension.
#' @param overwrite Whether to overwrite the \code{dbtable} if already
#'     exists. Default is FALSE.
#' @param sep a character string to separate the terms.  Not
#'     ‘NA_character_’. Default is \code{,}.
#' @importFrom tools file_path_as_absolute file_path_sans_ext
#' @import DBI
#' @details The provided file must has one or more columns to unique
#'     identify each row (no duplicate rows allowed). The file must be
#'     rectangular without rownames. (if rownames are needed, save it
#'     as a column.)
#' @examples
#' mtc <- tibble::rownames_to_column(mtcars)
#' filename <- file.path(tempdir(), "mtc.csv")
#' write.csv(mtc, file= filename, row.names = FALSE)
#' aa <- makeSQLDataFrame(filename, dbkey = "rowname")
#' aa
#' 
#' @export

makeSQLDataFrame <- function(filename = character(),
                             dbkey = character(),
                             dbname = NULL,
                             dbtable = NULL,
                             overwrite = FALSE, sep = ",",
                             ...)
{
    ## browser()

    stopifnot(is.character(filename), length(filename) == 1L)
    stopifnot(file.exists(filename))
    if (is.null(dbname)) {
        dbname <- tempfile(fileext = ".db")
    } else if (file.exists(dbname)) {
        dbname <- tools::file_path_as_absolute(dbname)
    } else {
        file.create(dbname)
    }
    
    if (is.null(dbtable))
        dbtable <- tools::file_path_sans_ext(basename(filename))

    con <- DBI::dbConnect(RSQLite::SQLite(), dbname = dbname)
    dbWriteTable(con, dbtable, value = filename, overwrite = overwrite, sep = sep, ...)
    out <- SQLDataFrame(dbname = dbname, dbtable = dbtable,
                        dbkey = dbkey)
    msg <- msg_saveSQLDataFrame(out, dbname, dbtable)
    message(msg)
    return(out)
}

