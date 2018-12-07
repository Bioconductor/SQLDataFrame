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

con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
DBI::dbListTables(con)
dbWriteTable(con, "colData", colData)
dbWriteTable(con, "colDatal", colDatal)
## dbDisconnect(con)
library(dplyr)
cold.db <- con %>% tbl("colData")
coldl.db <- con %>% tbl("colDatal")

##
a <- SQLDataFrame(dbname = "inst/test.db", dbtable = "colData", dbkey = "sampleID")
b <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "colDatal", dbkey = "sampleID", row.names = letters)

colData %>% as.tibble() %>% dplyr::slice(1L)
## # A tibble: 1 x 3
##   sampleID Treatment  ages
##   <fct>    <fct>     <int>
## 1 a        ChIP         32
cold.db %>% dplyr::slice(1L)
## Error in UseMethod("slice_") : no applicable method for 'slice_'
##   applied to an object of class "c('tbl_dbi', 'tbl_sql',
##   'tbl_lazy', 'tbl')"

colData %>% filter(row_number() %in% c(3L,5L))
##   sampleID Treatment ages
## 1        c      ChIP   35
## 2        e      ChIP   27
colData %>% filter(between(row_number(), 3,n()))
##   sampleID Treatment ages
## 1        c      ChIP   35
## 2        d     Input   28
## 3        e      ChIP   27
## 4        f     Input   26
colData %>% filter(row_number() %in% seq(3,n()))  ## equivalent to "between"

cold.db %>% mutate(rowID = row_number())
## Error: Window function `row_number()` is not supported by this database


