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
con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
DBI::dbListTables(con)
## dbRemoveTable(con, "colDatal")
dbWriteTable(con, "colData", colData)

state <- data.frame(division = as.character(state.division),
                    region = as.character(state.region),
                    state = state.name,
                    population = state.x77[,"Population"],
                    row.names = NULL,
                    stringsAsFactors = FALSE)
## change $population column a little bit so not unique
state$population[state$state == "Arizona"] <- state$population[state$state == "Kansas"]
state$population[state$state == "Kentucky"] <- state$population[state$state == "Louisiana"]
state$population[state$state == "New Mexico"] <- state$population[state$state == "West Virginia"]
state$population[state$state == "South Dakota"] <- state$population[state$state == "North Dakota"]
state$size <- cut(state$population, breaks = c(0, 1000, 5000, 30000), labels = c("small", "medium", "large"))

DBI::dbWriteTable(con, "state", state)
DBI::dbListTables(con)

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
ROWNAMES(ss1)  ## only works for primary key. 
ss1[c("Alabama", "Alaska"),]  ## expected ERROR because ROWNAMES(ss1) does not work. 
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
cold.db <- con %>% tbl("colData")
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
## SQL examples
###
con1 <- DBI::dbConnect(RSQLite::SQLite(), dbname = ":MEMORY:")
dbWriteTable(con1, "mtcars", mtcars)
DBI::dbListTables(con1)
dbGetQuery(con, 'SELECT * FROM mtcars LIMIT 2')
dbGetQuery(
    con,
        "SELECT * FROM mtcars WHERE (
       SELECT cyl || '\b' || gear IN ('6.0\b4.0', '6.0\b3.0')
     )
     LIMIT 2;"
    )
filt <- mtcars[mtcars$carb == 4, c("cyl", "gear")]
filtp <- paste(paste0(filt[,1], ".0"), paste0(filt[,2], ".0"), sep="\b")
dbGetQuery(
    con,
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
dbGetQuery(con, "SELECT * FROM state WHERE state = 'Alabama'")
dbGetQuery(con, "SELECT * FROM state WHERE state IN ('Alabama', 'Alaska')")
dbGetQuery(con, "SELECT * FROM state WHERE state IN ($1)", param = list(c("Alaska", "Alabama")))
dbGetQuery(con, "SELECT * FROM state WHERE (division || '\b' || region IN ('Pacific\bWest') )")

## concatenation of multiple columns when matching  *** (using || as infix operator)
DBI::dbGetQuery(con,
           "SELECT * FROM state WHERE (division || '\b' || region IN ($1) )",
           param = list(paste0(c("Mountain", "Pacific"), "\b", "West")))

dbGetQuery(con, "SELECT region || '_' || region || '_' || state as FullName FROM state")
dbGetQuery(con, "SELECT * FROM state WHERE state || '\b' || region LIKE '%a\bS%'")

