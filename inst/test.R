## class
a <- .SQLDataFrame()
## SQLDataFrame with 0 rows and 0 columns
str(a)
## Formal class 'SQLDataFrame' [package "SQLDataFrame"] with 9 slots
##   ..@ dbpath         : chr(0) 
##   ..@ dbtable        : chr(0) 
##   ..@ key            : chr(0) 
##   ..@ rownames       : NULL
##   ..@ nrows          : int 0
##   ..@ listData       : Named list()
##   ..@ elementType    : chr "ANY"
##   ..@ elementMetadata: NULL
##   ..@ metadata       : list()

dbpath(a)
dbtable(a)
key(a)

## example database tables
colData <- data.frame(sampleID = letters[1:6],
                      Treatment=rep(c("ChIP", "Input"), 3),
                      ages = sample(25:35, 6))
colDatal <- data.frame(sampleID = letters,
                      Treatment=rep(c("ChIP", "Input"), 13),
                      ages = sample(20:40, 26, replace=T))

con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/test.db")
dbListTables(con)
dbWriteTable(con, "colData", colData)
dbWriteTable(con, "colDatal", colDatal)
## dbDisconnect(con)
cold.db <- con %>% tbl("colData")


##
a <- SQLDataFrame(dbname = "inst/test.db", dbtable = "colData",
                  dbkey = "sampleID")
b <- SQLDataFrame(dbname = "inst/test.db", dbtable = "colDatal",
                  dbkey = "sampleID", row.names = letters)
c <- SQLDataFrame(dbname = "inst/test.db", dbtable = "colDatal",
                  dbkey = "sampleID",
                  col.names = c("Treatment", "ages", "random"))
colnames(c)
names(c)
dim(c)
length(c)
c[[2]]
c[[3]]  ## error: subscript is out of bounds
c[["Treatment"]]
c[["ages"]]
c[[1:2]]  ## error: attempt to extract more than one element
## normalizeDoubleBracketSubscript(i, x, allow.NA = TRUE) exact =
## TRUE, allow.NA = TRUE, allow.nomatch = TRUE)
c[[NA]]
c[["random"]] 
d <- DataFrame(a=letters, b=LETTERS, c=1:26)

## todo:
## rbind, as.data.frame(), as("DataFrame")...
