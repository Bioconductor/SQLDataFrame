## class
a <- .SQLDataFrame()  ## expected error!
## SQLDataFrame with 0 rows and 0 columns
str(a)
dbname(a)
dbtable(a)
dbkey(a)

## example database tables
colData <- data.frame(sampleID = letters,
                      Treatment=rep(c("ChIP", "Input"), 13),
                      Ages = sample(20:40, 26, replace=T))

library(DBI)
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
DBI::dbListTables(conn)
## dbRemoveTable(conn, "colDatal")
dbWriteTable(conn, "colData", colData)

state <- data.frame(division = as.character(state.division),
                    region = as.character(state.region),
                    state = state.name,
                    population = state.x77[,"Population"],
                    row.names = NULL,
                    stringsAsFactors = FALSE)
## change $population column a little bit so not unique, but "region+population" still unique.
state$population[state$state == "Arizona"] <- state$population[state$state == "Kansas"]
state$population[state$state == "Kentucky"] <- state$population[state$state == "Michigan"]
state$population[state$state == "New Mexico"] <- state$population[state$state == "West Virginia"]
state$population[state$state == "South Dakota"] <- state$population[state$state == "Montana"]
state$size <- cut(state$population, breaks = c(0, 1000, 5000, 30000), labels = c("small", "medium", "large"))

DBI::dbWriteTable(conn, "state", state)
DBI::dbListTables(conn)

###
## primary key
###
ss <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "state", dbkey = "state")

rownames(ss)
colnames(ss)
ss[1:5, ]
## row subsetting by primary key. 
ss[c("Alabama", "Alaska"),]
ROWNAMES(ss)
ss[, 1:2]
ss[, 2:3]
ss[, 2, drop=FALSE]
ss[["state"]]
## list-style-subsetting
ss[c("region", "population")]
ss["region"]
sum(ss$population)

## write out the SQLDataFrame as a database table, save by default to
## the input dbname(), use argument "name.." to save the table
## name. (arg: database name, table name, database connection type,
## ...)
tt <- ss[10:25, 3:4]
str(tt)
tbl <- .extract_tbl_from_SQLDataFrame(tt)
compute(tbl)
saveSQLDataFrame(tt, dbtable = "tt")


tt@tblData$src$con
dbListTables(tt@tblData$src$con)
dbDisconnect(conn)
conn <- dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
dbListTables(conn)

identical(conn, tt@tblData$src$con)  ## FALSE
str(conn)
str(tt@tblData$src$con)
conn@ref
## <environment: 0x55ac04c69698>
tt@tblData$src$con@ref
## <environment: 0x55ac03d894d8>
### Summary: when constructing "SQLDataFrame" opened an connection,
### and it was save as a temporary table (only visible) to the current
### connection: "sqldf@tblData$src$con" and will be automatically
### deleted when the connection expires, so it was not physically
### written into the dabase.
ttnew <- SQLDataFrame("inst/extdata/test.db", "tt")

###
## composite key
###
ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db",
                    dbtable = "state",
                    dbkey = c("region", "population"))
## region & population could uniquely identify each record.
rownames(ss1)
colnames(ss1)
ss1[1:5, ]
ROWNAMES(ss1) 
ss1[c("South", "3615"),]  ## expected ERROR because ROWNAMES(ss1) does not work. But could use something like "ss1[filter(region == "South" & population = "3615"), ]"
ss1[c("South\b3615", "West\b2280"), ]
ss1[list(c("South", "West"), c("3615", "2280")), ]

ss2 <- ss1[1:9, ]
ROWNAMES(ss2)
## ss2[c("South\b3615.0", "West\b365.0"), ]  ## transmute(paste) %>% pull()
ss2[c("South\b3615", "West\b365"), ]   ## collect(dbkey(x)) %>% transmute(paste) %>% pull()
## row subsetting with list object works, checks dbkey(), but doesn't matter with ordering. 
ss1[list(population = c("3615", "365", "4981"), region = c("South", "West", "South")), ]
ss2[list(region = c("South", "West", "West"), population = c("3615", "365", "2280")), ]
ss2[data.frame(region = c("South", "West", "West"), population = c("3615", "365", "2280")), ]
ss2[tibble(region = c("South", "West", "West"), population = c("3615", "365", "2280")), ]

ss1[list(region="South", population = "3615", other = "random"), ]
## Error in ss1[list(region = "South", population = "3615", other = "random"),  : 
##   Please use: "region, population" as the query list name(s).
ss1[ROWNAMES(ss2), ]

ss1[, 1:2]
ss1[, 2:3]
ss1[, 2, drop=FALSE]
ss1[["state"]]
## list-style-subsetting
ss1["state"]
ss1[c("division", "size")]
sum(ss1$population)

###
## sql manipulations for rows
### 

library(dplyr)
cold.db <- conn %>% tbl("colData")
b <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "colData", dbkey = "sampleID")

## row subsetting with character vector (add to test_method.R)
b[letters[10:15], ]
## col indexes corresponds to non-key-columns only. (add to test_method.R)
b[,2]  ## return age value.
b[,1] ## return "Treatment" value.

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


## now have b,

b[1:2]
b[2:3]
b[c(1,3)]
b[1]  ## works
b[2]
b[3]
b[,2]
b[,2, drop=FALSE]
b[1:5, ]
b[1:5, 2:3]
b[1:100, ]  ## expect_error(, "subscript contains out-of-bounds indices")
b[, 4]  ## expect_error(, "subscript contains out-of-bounds indices")

###
## SQL examples: compute
###
con1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":MEMORY:")  ## works
con1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = "/home/qian/Documents/Research/rsqlite/test.db") ## works
## summary: "compute" works for "SQLiteConnection" object with both in memory or on-disk database. 
dbWriteTable(con1, "mtcars", mtcars, overwrite = T)
DBI::dbListTables(con1)
mtcars.db <- tbl(con1, "mtcars")
mt <- mtcars.db %>% select(mpg:hp)
mt %>% show_query()
compute(mt, name = "mt")
DBI::dbListTables(con1)
tbl(con1, "mt")
DBI::dbReadTable(con1, "mt")

mt1 <- mt %>% filter(mpg > 21)
compute(mt1)
DBI::dbListTables(con1)
compute(mt1, name = "mt1")
DBI::dbListTables(con1)

## try the existing database
### works in writing to the current connection, but when opening a new
### R session, the table will be lost. So the "compute()" only saves a
### temporary table.
conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
DBI::dbListTables(conn)
state.db <- tbl(conn, "state")
st <- state.db %>% select(region:size) %>% filter(region == "West")
st %>% show_query()
compute(st, name = "st")
DBI::dbListTables(conn)
compute(st)
DBI::dbListTables(conn)
DBI::dbRemoveTable(conn, "st")
## DBI::dbRemoveTable(conn, "lnyqcyiovw")

conn1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
dbListTables(conn1)  ## does not include the previous saved tables. so
                     ## the table is not written into the database!

###
## SQL examples: concatenation
###
con1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":MEMORY:")
dbWriteTable(con1, "mtcars", mtcars)
DBI::dbListTables(con1)
dbGetQuery(con1, 'SELECT * FROM mtcars LIMIT 2')
dbGetQuery(
    con1,
        "SELECT * FROM mtcars WHERE (
       SELECT cyl || '\b' || gear IN ('6.0\b4.0', '6.0\b3.0')
     )
     LIMIT 2;"
    )
filt <- mtcars[mtcars$carb == 4, c("cyl", "gear")]
filtp <- paste(paste0(filt[,1], ".0"), paste0(filt[,2], ".0"), sep="\b")
dbGetQuery(
    con1,
    "SELECT * FROM mtcars WHERE (cyl || '\b' || gear IN ($1))",
    param = list(filtp)
    )

## All data manipulation on SQL tbls are lazy: they will not actually
## run the query or retrieve the data unless you ask for it: they all
## return a new tbl_dbi object. Use compute() to run the query and
## save the results in a temporary in the database, or use collect()
## to retrieve the results to R. You can see the query with
## show_query().  For best performance, the database should have an
## index on the variables that you are grouping by.  Use explain() to
## check that the database is using the indexes that you expect.
## There is one excpetion: do() is not lazy since it must pull the
## data into R.

## ? src_sql to work directly on tbl_dbi object? 
dbGetQuery(conn, "SELECT * FROM state WHERE state = 'Alabama'")
dbGetQuery(conn, "SELECT * FROM state WHERE state IN ('Alabama', 'Alaska')")
dbGetQuery(conn, "SELECT * FROM state WHERE state IN ($1)", param = list(c("Alaska", "Alabama")))
dbGetQuery(conn, "SELECT * FROM state WHERE (division || '\b' || region IN ('Pacific\bWest') )")

## concatenation of multiple columns when matching  *** (using || as infix operator)
DBI::dbGetQuery(conn,
           "SELECT * FROM state WHERE (division || '\b' || region IN ($1) )",
           param = list(paste0(c("Mountain", "Pacific"), "\b", "West")))

dbGetQuery(conn, "SELECT region || '_' || region || '_' || state as FullName FROM state")
dbGetQuery(conn, "SELECT * FROM state WHERE state || '\b' || region LIKE '%a\bS%'")

###----------------------------------------------------------
## copy_to(append?), db_insert_into, dbWriteTable(append)
###----------------------------------------------------------
### here, "src_memdb()$con" is equivalent to "dbConnect(RSQLite::SQLite(), ":memory:")"
m1 <- mtcars[1:5, ]
m2 <- mtcars[6:10, ]
copy_to(src_memdb(), m1, "m1", overwrite = T)
src_tbls(src_memdb())
dbListTables(src_memdb()$con)  ## equivalent to above

###----------------------------------------------------------------------
### "db_insert_into": works for in-memory data.frame, not for "tbl_dbi"
dbReadTable(src_memdb()$con, "m1")
db_insert_into(src_memdb()$con, "m1", m2)  ## works!
dbReadTable(src_memdb()$con, "m1")

copy_to(src_memdb(), m1, "m1", overwrite = T)  ## overwrite m1 as original 5 rows.
m2.db <- src_memdb() %>% copy_to(m2)
db_insert_into(src_memdb()$con, "m1", m2.db)
## Error in (function (classes, fdef, mtable)  : 
##   unable to find an inherited method for function ‘dbWriteTable’ for signature ‘"SQLiteConnection", "character", "tbl_dbi"’

###-------------------------------------------------------------------------------
### "dbWriteTable": "append" works for in-memory data.frame, not for "tbl_dbi". 
dbWriteTable(src_memdb()$con, name = "m1", value = m1, overwrite =T)
dbWriteTable(src_memdb()$con, name = "m1", value = m2, append = T)  ## "append" works for in-memory data.frame. 
dbReadTable(src_memdb()$con, "m1")

dbWriteTable(src_memdb()$con, name = "m1", value = m1, overwrite =T)
dbWriteTable(src_memdb()$con, name = "m1", value = m2.db, append = T)  
## Error in (function (classes, fdef, mtable)  : 
##   unable to find an inherited method for function ‘dbWriteTable’ for signature ‘"SQLiteConnection", "character", "tbl_dbi"’

###-----------------------------------------------------------------
### "copy_to": "overwrite" works, but "append" doesn't work!
copy_to(src_memdb(), m1, "m1", overwrite = T)
copy_to(src_memdb()$con, m2, "m1", append = T)
## Error: Table `m1` exists in database, and both overwrite and append are FALSE

###--------------
## sdf[sdf, ]
###--------------
ss1[ss2, ]
ss1[ss3, ]

###---------------------
## filter.SQLDataFrame
###---------------------
ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "state", dbkey = c("region", "population"))
ss2 <- ss1[1:10, 2:3]
filter.SQLDataFrame(ss2, region == "South")
filter(ss2, region == "South")
filter(ss2, region == "South" & population > 1000)  ## a bit more efficient than [TRUE, ]. 
ss2 %>% filter(region == "South"& population > 1000)

### non-key subsetting
ss2 %>% filter(region == "South"& population > 1000)
ss2 %>% filter(state %in% c("Alabama", "Arkansas"))
ss2 %>% filter(size == "medium")  ## @indexes[[1]] not null, update
ss1 %>% filter(size == "medium")  ## @indexes[[1]] is null, update

## less-efficient way of filtering using "[i, ]"
ss2[ss2$region == "South", ]
ss2[ss2$region == "South" & ss2$population > 1000, ]

### key-subsetting: must provide specific values
ss2[list(region = "South", population = "3615"), ]

###---------------------------------------
## rbind
###---------------------------------------

## same source
ss3 <- ss1[11:15, 2:3]
ss23 <- rbind(ss2, ss3)  ## works for same_src(ss2@tblData$src, ss3@tblData$src)
str(ss23)
ss23@tblData

## different source
saveSQLDataFrame(ss3, "../temp.db")
ss3.new <- SQLDataFrame(dbname = "../temp.db", dbtable="ss3", dbkey = c("region", "population"))
ss23.new <- rbind(ss2, ss3.new) 
saveSQLDataFrame(ss23.new, "../temp.db")

## overlapping rows
ss3 <- ss1[6:15, 2:3]
ss23 <- rbind(ss2, ss3)

ss2 <- ss1[10:1, 2:3]
set.seed(123)
idx <- sample(15, 5)
ss3 <- ss1[idx, 2:3]
ss23 <- rbind(ss2, ss3)

ss32 <- rbind(ss3, ss2)

###---------
## union
###---------
ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "state", dbkey = c("region", "population"))
ss2 <- ss1[1:10, 2:3]
ss3 <- ss1[8:15, 2:3]
ss4 <- ss1[15:17, 2:3]
ss5 <- ss1[20:30, 2:3]
ss6 <- ss1[31:50, 2:3]
ss7 <- ss1[18:19, 2:3]
aa <- SQLDataFrame::union(ss2, ss3)
aa1 <- SQLDataFrame::union(aa, ss4)

aa <- SQLDataFrame::union(SQLDataFrame::union(SQLDataFrame::union(ss2, ss3), ss4), ss5) ## works, union 4 elements. 
aa <- SQLDataFrame::union(SQLDataFrame::union(SQLDataFrame::union(SQLDataFrame::union(ss2, ss3), ss4), ss5), ss6) ## Error in result_create(conn@ptr, statement) : parser stack overflow
## reproducible dbplyr error for bug report: https://github.com/tidyverse/dbplyr/issues/253

## reverse "sort".
aa <- sample(letters)


## bug1, after "devtools::document()", need to reconstruct aa, and
## aa1, otherwise, doesn't show correctly.
## bug2, show method for "aa1" returns error from ".extract_tbl_from_SQLDataFrame".
## Error in result_create(conn@ptr, statement) : parser stack overflow... 

aa2 <- SQLDataFrame::union(ss3, ss4)  ## works, dim: 10X2
aa3 <- SQLDataFrame::union(ss2, ss4)  ## works, dim: 13X2
aa4 <- SQLDataFrame::union(aa2, aa3)  ## works, dim: 17*2
aa5 <- SQLDataFrame::union(aa4, ss4)  ## Error in result_create(conn@ptr, statement) : parser stack overflow. With internal dbplyr:::union_all.tbl_lazy, then distinct()

aa <- SQLDataFrame::union(SQLDataFrame::union(SQLDataFrame::union(SQLDataFrame::union(ss2, ss3), ss4), ss5), ss6)  ## now works, with internal of dbplyr:::union.tbl_lazy. 

x1 <- .extract_tbl_from_SQLDataFrame(ss2)
y1 <- .extract_tbl_from_SQLDataFrame(ss3)
tbl.union <- union(x1, y1)
show_query(tbl.union)

con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
query.sql <- dbplyr::db_sql_render(con, tbl.union)
dbExecute(con, build_sql("CREATE TABLE aaunion AS ", query.sql))
dbExecute(con, build_sql("CREATE TABLE ", sql("aaunion1"), " AS ", query.sql))
dbExecute(con, build_sql("CREATE TABLE ", "aaunion2", " AS ", query.sql))

tbl.unionall <- union_all(x1, y1)

## explore the SQL ordering
dbExecute(con, build_sql("CREATE TABLE mtc1 AS SELECT * FROM mtcars LIMIT 6"))
dbExecute(con, build_sql("CREATE TABLE mtc2 AS SELECT * FROM mtcars WHERE carb == 1"))
dbGetQuery(con, build_sql("SELECT mpg,cyl,carb FROM mtc1 UNION SELECT mpg,cyl,carb FROM mtc2"))
dbGetQuery(con, build_sql("SELECT mpg,cyl,carb FROM mtc1 UNION SELECT mpg,cyl,carb FROM mtc2 ORDER BY mpg"))  ## identical with above. so by default "order by" first column. 
dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION SELECT cyl,mpg,carb FROM mtc2"))
dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION SELECT cyl,mpg,carb FROM mtc2 ORDER BY cyl,mpg"))  ## identical with above. so by default "order by" first column, then 2nd, ...
dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION SELECT cyl,mpg,carb FROM mtc2 ORDER BY cyl,carb"))  ## with specific orders given by user. 

dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION ALL SELECT cyl,mpg,carb FROM mtc2"))
## "UNION ALL" doesn't do any automatic ordering, need user to provide. 
dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION ALL SELECT cyl,mpg,carb FROM mtc2 UNIQUE mpg")) 
dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION SELECT cyl,mpg,carb FROM mtc2 ORDER BY cyl,carb")) 

## multiple "UNION ALL"
dbGetQuery(con, build_sql("SELECT cyl,mpg,carb FROM mtc1 UNION ALL SELECT cyl,mpg,carb FROM mtc2 UNION ALL SELECT cyl, mpg, carb FROM mtcars WHERE cyl == 6"))

##---------------------------------------
## modify the "state" table, to make "region+population" unique. 2/7/2019. 
##---------------------------------------
con <- DBI::dbConnect(RSQLite::SQLite(), dbname="inst/extdata/test.db")
stt <- dbReadTable(con, "state")

##-----------------------------------------------
## dbExecute, build_sql, dbGetQuery, in_schema
##-----------------------------------------------
con <- dbConnect(RSQLite::SQLite(), ":memory:")
copy_to(con, mtcars)
tmp <- tempfile()
DBI::dbExecute(con, paste0("ATTACH '", tmp, "' AS aux"))
DBI::dbExecute(con, "CREATE TABLE aux.mtc(model text, mpg text)")
## [1] 0
DBI::dbExecute(con, "INSERT INTO aux.mtc SELECT model, mpg FROM mtcars")
## [1] 32
DBI::dbExecute(con, "CREATE TABLE aux.mtc AS SELECT mpg, cyl, drat FROM mtcars")
## equivalent to above 2 lines.
con %>% tbl(in_schema("aux", "mtc")) 
bsql <- build_sql("SELECT * FROM ", in_schema("aux", "mtc"), " Limit 5")
dbExecute(con, bsql)  
dbGetQuery(con, bsql)
bsql <- build_sql("SELECT * FROM ", in_schema("aux", "mtc"), " Limit ?")
dbGetQuery(con, bsql1, params = list(6))
## You can also use ‘dbExecute()’ to call a stored procedure that
## performs data manipulation or other actions that do not return a
## result set. To execute a stored procedure that returns a result
## set use ‘dbGetQuery()’ instead.

tbl(con, in_schema("aux", "mtc"))$src
## src:  sqlite 3.22.0 [:memory:]
## tbls: df, mtcars, sqlite_stat1, sqlite_stat1, sqlite_stat4, sqlite_stat4
### NOTE: the "aux.mtc" has same source as "con".
debug(dbplyr:::copy_to.src_sql)
aa <- tbl(con, in_schema("aux", "mtc"))
copy_to(con, aa, "mtc")  ## success! same source copy to calls "compute()"
undebug(dbplyr:::copy_to.src_sql)

##---------------------------
## debug "overwrite = TRUE"
##---------------------------
con <- dbConnect(RSQLite::SQLite(), ":memory:")
copy_to(con, mtcars)
mtc.db <- tbl(con, "mtcars") %>% select(mpg:disp)
copy_to(con, mtc.db, "mtc.db")
tbl(con, "mtc.db")
copy_to(con, mtc.db, "mtc.db", overwrite = TRUE, temporary = FALSE)
## compute calls "do_compute.DBIConnection()", dbExecute(con, "CREATE TEMPORARY TABLE name AS query")
## copy_to, copy_to.DBIConnection, copy_to.src_sql, compute(), compute.tbl_sql(), db_compute(), db_compute.DBIConnection(), db_save_query(), db_save_query.DBIConnection(), dbExecute(con, build_sql("CREATE TEMPORARY TABLE name AS ", show_query(mtc.db)), the last step of creating table does not have "overwrite" argument.  

##------------------------
## debug  copy_to(types)
##------------------------
copy_to(con, mtc.db, "mtc.db1", types = c("NUMERIC", "TEXT", "NUMERIC"))
tbl(con, "mtc.db1")  ## data type didn't change...

src_dbi(dbConnect(org.Hs.eg.db))
library(organism.dplyr)
src <- src_organism("TxDb.Hsapiens.UCSC.hg19.knownGene")
tbl(src, "id")

###------------
## Join/bind
###------------
con1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":MEMORY:")
mtc <- mtcars
mtc$id <- rownames(mtcars)
m1 <- mtc[1:10, c("id", "mpg", "cyl", "disp")]
m2 <- mtc[6:15, c("id", "hp", "drat", "wt")]
copy_to(con1, m1)
copy_to(con1, m2)
m1.db <- tbl(con1, "m1")
m2.db <- tbl(con1, "m2")

### mutating join
left_join(m1.db, m2.db, by = "id") %>% collect()
left_join(m2.db, m1.db, by = "id") %>% collect()
inner_join(m1.db, m2.db, by = "id")
inner_join(m1.db, m2.db, by = "id") %>% show_query()

right_join(m1.db, m2.db, by = "id")  ## ERROR
full_join(m1.db, m2.db, by = "id")  ## ERROR
## Error in result_create(conn@ptr, statement) : 
##   RIGHT and FULL OUTER JOINs are not currently supported

### filtering join
semi_join(m1.db, m2.db, by = "id")  ## overlapping rows
anti_join(m1.db, m2.db, by = "id")  ## non-overlapping rows in m1, not in m2
anti_join(m2.db, m1.db, by = "id")  ## non-overlapping rows, in m2, not in m1

### Binding
bind_rows(m1.db, m2.db)  ## ERROR, doesn't work for lazy tbl like "tbl_dbi" object. 
bind_cols(m1.db, m2.db)  ## ERROR
## Error in cbind_all(x) : 
##   Argument 1 must be a data frame or a named atomic vector, not a tbl_dbi/tbl_sql/tbl_lazy/tbl


## QUESTION: how to save the binding rows/cols in SQLDataFrame? @tblData save as a list of `tbl_dbi`? Virtual class of `SQLDataFrame` and subclass after subsetting, rbind? ...

## realize and use saveSQLDataFrame? rbind(dbname = , dbtable =, ), return(SQLDataFrame()). 
## copy_to, the 1st copy_to(append). key cols... check key ... 

## cbind: left_join, inner_join.

## sql(). [filter(col1 %in% c(), col2 == ...), ]. ROWNAMES matching, sdf@indexes. 


ss1 <- SQLDataFrame("inst/extdata/test.db", dbtable = "state", dbkey = c("region", "population"))
ss2 <- ss1[1:10, 2:3]
ss3 <- ss1[11:15, 2:3]

###---------------------------------------
## join, left_join, semi_join, anti_join
###---------------------------------------
ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db",
                    dbtable = "state",
                    dbkey = c("region", "population"))
ss2 <- ss1[1:10, 2:3]
ss4 <- ss1[6:15, 1, drop = FALSE]
saveSQLDataFrame(ss4, "inst/extdata/test.db")
## FIXME:
## Error in result_create(conn@ptr, statement) : database is locked

aa <- left_join(ss2, ss4, by = dbkey(ss2))
sql.aa <- dbplyr::db_sql_render(.con_SQLDataFrame(aa), aa@tblData)
con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
sql.aa1 <- build_sql("CREATE TABLE aa AS ", sql.aa)
dbExecute(con, sql.aa1)
dbRemoveTable(con, "aa")
dbDisconnect(con)

sql.paste <- gsub("FROM `state`", paste0("FROM `", in_schema("aux", "state"), "`"), sql.aa1)
con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "../temp.db")
dbExecute(con, paste0("ATTACH '", .con_SQLDataFrame(aa)@dbname, "' AS aux"))
dbExecute(con, sql.paste)

ss2.tbl <- .extract_tbl_from_SQLDataFrame(ss2)
ss4.tbl <- .extract_tbl_from_SQLDataFrame(ss4)
left_join(ss2.tbl, ss4.tbl)  ## only takes 2 arguments for "x" and "y"!! 
temp <- left_join(ss2.tbl, ss4.tbl, by = dbkey(ss2))   ## same source
temp %>% show_query()
temp$src$con

is(temp)
## [1] "tbl_dbi"  "oldClass"
names(temp)
temp$src   ## single output
## src:  sqlite 3.22.0 [/home/qian/Documents/Research/rsqlite/SQLDataFrame/inst/extdata/test.db]
## tbls: colData, mtc, mtcars, new, sqlite_stat1, sqlite_stat4, state
names(temp$ops)  
## [1] "name" "x"    "y"    "args"
temp$ops$name
## [1] "join"
temp$ops$args
is(temp$ops$x)
## [1] "tbl_dbi"  "oldClass"
is(temp$ops$y)
## [1] "tbl_dbi"  "oldClass"

str(temp$ops$x)
names(temp$ops$x)
## [1] "src" "ops"
temp$ops$x$src
## src:  sqlite 3.22.0 [/home/qian/Documents/Research/rsqlite/SQLDataFrame/inst/extdata/test.db]
## tbls: colData, mtc, mtcars, new, sqlite_stat1, sqlite_stat4, state
temp$ops$x$src$con
## <SQLiteConnection>
##   Path: /home/qian/Documents/Research/rsqlite/SQLDataFrame/inst/extdata/test.db
##   Extensions: TRUE
names(temp$ops$x$ops)
## [1] "name" "x"    "dots" "args"
temp$ops$x$ops$name
## [1] "select"
is(temp$ops$x$ops$x)
## [1] "op_select"
str(temp$ops$x$ops$x)
names(temp$ops$x$ops$x)
## [1] "name" "x"    "dots" "args"

aa <- left_join(ss2, ss4, by = dbkey(ss2))

## Summary: tbl_dbi$ops, depending on the "$name", "$x" would be "op_**". E.g.,
## if $name == "select", then $x is "op_select"; names($ops): name, x, dots, args
## if $name == "mutate", then $x is "op_mutate"; names($ops): name, x, dots, args
## if $name == "join", then $x is "tbl_dbi", $y is "tbl_dbi", names($ops): name, x, y, args
## "tbl_dbi" is a list of 2: "src", "ops"

## want these to work: left_join(ss2, ss4), returns a SQLDataFrame(dbname=, dbtable=, dbkey=dbkey(ss2)) with @tblData ()

## replaceSlot @tblData, update @indexes to be NULL. Update @dbnrows, update @dbtable.
