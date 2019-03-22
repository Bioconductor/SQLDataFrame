###---------------------------------------
## join, left_join, semi_join, anti_join
###---------------------------------------
ss <- SQLDataFrame(dbname = "inst/extdata/test.db",
                    dbtable = "state",
                    dbkey = c("region", "population"))
ss1 <- ss[1:10, 1, drop = FALSE]
ss2 <- ss[5:15, 2, drop=FALSE]

ss3 <- ss[8:15, 3, drop=FALSE]
ss4 <- ss[6:15, 3, drop = FALSE]

## saveSQLDataFrame(ss4, "inst/extdata/test.db")
## FIXME:
## Error in result_create(conn@ptr, statement) : database is locked

## left_join
aa <- left_join(ss1, ss2, by = dbkey(ss2))  ## 1:10
aa1 <- left_join(aa, ss4, by = dbkey(ss2))  ## 
aa2 <- left_join(ss4, aa, by = dbkey(ss2))
saveSQLDataFrame(aa1)  ## works!

## inner_join, left_join
aa <- inner_join(ss1, ss2, by = dbkey(ss1))  ## 5:10
aa1 <- inner_join(aa, ss3, by = dbkey(ss1))  ## 8:10
aa2 <- left_join(ss1, aa1, by = dbkey(ss1))  ## 1:10

aa3 <- left_join(aa, aa1, by = dbkey(ss1))   ## 8:10, duplicate cols. 
aa4 <- left_join(aa, aa1, by = c(dbkey(ss1), "division", "state"))   ## 8:10, unique cols.
aa5 <- inner_join(aa, aa1, by = c(dbkey(ss1), "division", "state"))  ## works!
aa6 <- inner_join(aa, aa1, by = c(dbkey(ss1), "division"))  ## works!
aa6 <- inner_join(aa, aa1)  ## works! By default, using overlapping cols as "by="

## semi_join, left_join, inner_join
aa <- semi_join(ss1, ss2)  ## 5:10 rows
aa1 <- left_join(aa, ss3)  ## 5:10 rows
aa2 <- inner_join(aa1, ss4)  ## inner(5:10, 6:15) => 6:10. By default, using overlapping cols as "by="
aa3 <- left_join(ss4, aa1)  ## left(6:15, 5:10) => 6:15

## anti_join, 
aa <- anti_join(ss2, ss1)  ## anti(5:15, 1:10) => 11:15
aa1 <- left_join(aa, ss1)  ## left(11:15, 1:10) => 11:15, NA
aa2 <- inner_join(aa, ss4) ## inner(11:15, 8:15) => 11:15, all values
aa3 <- semi_join(ss3, aa)  ## semi(8:15, 11:15) => 11:15, filtering
aa4 <- anti_join(aa, ss4)  ## anti(11:15, 6:15) => 0 rows

saveSQLDataFrame(aa3)  ## works!
saveSQLDataFrame(aa2)

## CRAZY, *_join + union
aa <- left_join(ss1, ss2, by = dbkey(ss2))  ## 1:10
aa1 <- left_join(ss3, ss2, by = dbkey(ss2))  ## 8:15
bb <- rbind(aa, aa1)
## Error in rbind(...) : 
##   Input SQLDataFrame objects must have identical columns!
bb <- rbind(aa[,2, drop=FALSE], aa1[,2,drop=FALSE])  ## FAIL! rewrite dbtable() method!! 
saveSQLDataFrame(bb)

##########################
### test dbplyr:: *_join
##########################
tb1 <- .extract_tbl_from_SQLDataFrame(ss1)
tb2 <- .extract_tbl_from_SQLDataFrame(ss2)
tb3 <- .extract_tbl_from_SQLDataFrame(ss3)

left_join(tb1, tb2)
inner_join(tb1, tb2)
semi_join(tb1, tb2)
anti_join(tb1, tb2)
right_join(tb1, tb2)  ## not supported!
full_join(tb1, tb2)  ## not supported!
## Joining, by = c("region", "population")
## Error in result_create(conn@ptr, statement) : 
##   RIGHT and FULL OUTER JOINs are not currently supported

## join(union), or union(join)...
## saveSQLDataFrame()

## SQLDataFrame() constructor, open a new local connection? (temp file .db)??
## openSQLtable(), ...

sql.aa <- dbplyr::db_sql_render(.con_SQLDataFrame(aa), aa@tblData)
con <- DBI::dbConnect(RSQLite::SQLite(), dbname = "inst/extdata/test.db")
sql.aa1 <- build_sql("CREATE TABLE aa AS ", sql.aa)
dbExecute(con, sql.aa1)
dbRemoveTable(con, "aa")
dbDisconnect(con)
