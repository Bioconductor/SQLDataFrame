#' Construct SQLDataFrame from file.
#' @description Given a file name, \code{makeSQLDataFrame} will write
#'     the file contents into SQL database, and open the database
#'     table as SQLDataFrame.
#' @param filename A \code{data.frame} or \code{DataFrame} object, or
#'     a character string of the filepath to the text file that to be
#'     saved as SQL database table. For filepath, the data columns
#'     should not be quoted on disk.
#' @param dbtable A character string for the to be saved database
#'     table name. If not provided, will use the name of the input
#'     \code{data.frame} or \code{DataFrame} object, or the
#'     \code{basename(filename)} without extension if \code{filename}
#'     is a character string.
#' @param dbkey A character vector of column name(s) that could
#'     uniquely identify each row of the filename. Must be provided in
#'     order to construct a SQLDataFrame.
#' @param conn a valid \code{DBIConnection} from \code{SQLite} or
#'     \code{MySQL}. If provided, arguments of `user`, `host`,
#'     `dbname`, `password` will be ignored.
#' @param host host name for SQL database.
#' @param user user name for SQL database.
#' @param password password for SQL database connection.
#' @param dbname database name for SQL connection. For SQLite
#'     connection, it uses a \code{tempfile(fileext = ".db")} if not
#'     provided.
#' @param type The SQL database type, supports "SQLite" and "MySQL".
#' @param overwrite Whether to overwrite the \code{dbtable} if already
#'     exists. Default is FALSE.
#' @param sep a character string to separate the terms.  Not
#'     ‘NA_character_’. Default is \code{,}.
#' @param index Whether to create an index table. Default is FALSE.
#' @param ... additional arguments to be passed.
#' @return A \code{SQLDataFrame} object.
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
#' write.csv(mtc, file= filename, row.names = FALSE, quote = FALSE)
#' obj <- makeSQLDataFrame(filename, dbkey = "rowname")
#' obj
#'
#' ## save as MySQL database
#' \dontrun{
#' localConn <- DBI::dbConnect(dbDriver("MySQL"),
#'                             host = "",
#'                             user = "",
#'                             password = "",
#'                             dbname = "")
#' makeSQLDataFrame(filename, dbtable = "mtcMysql", dbkey = "rowname", conn = localConn)
#' }
#' @export

makeSQLDataFrame <- function(filename,
                             dbtable = NULL,
                             dbkey = character(),
                             conn, 
                             host, user, dbname = NULL,
                             password = NULL, ## required for certain MySQL connection.
                             type = c("SQLite", "MySQL"),
                             overwrite = FALSE, sep = ",",
                             index = FALSE,
                             ...)
{
    stopifnot(is.data.frame(filename) | is(filename, "DataFrame") | isSingleString(filename))

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
    type <- match.arg(type)

    if (missing(conn)) {
        if (type == "SQLite") {
            if (is.null(dbname)) {
                dbname <- tempfile(fileext = ".db")
            } else if (file.exists(dbname)) {
                dbname <- tools::file_path_as_absolute(dbname)
            } else {
                file.create(dbname)
            }
            conn <- DBI::dbConnect(dbDriver("SQLite"),
                                   dbname = dbname)
        } else if (type == "MySQL") {
            conn <- DBI::dbConnect(dbDriver("MySQL"),
                                   host = host,
                                   user = user,
                                   password = password,
                                   dbname = dbname)
        }
    } else {
        ifcred <- c(host = !missing(host), user = !missing(user),
                    dbname = !missing(dbname), password = !missing(password))
        if (any(ifcred))
            message("These arguments are ignored: ",
                    paste(names(ifcred[ifcred]), collapse = ", "))
    }
    
    dbWriteTable(conn, dbtable, value = filename,
                 overwrite = overwrite, sep = sep, ...)
    
    if (index) ## FIXME: default is FALSE, which is different from
               ## saveSQLDataFrame.
        dbplyr:::db_create_indexes.DBIConnection(conn, dbtable,
                                                 indexes = list(dbkey),
                                                 unique = TRUE)
    
    out <- SQLDataFrame(conn = conn, dbtable = dbtable,
                        dbkey = dbkey)
    msg <- .msg_saveSQLDataFrame(out, conn, dbtable)
    message(msg)
    return(out)
}
