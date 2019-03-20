ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "state", dbkey = c("region", "population"))
ss2 <- SQLDataFrame(dbname = "inst/extdata/test1.db", dbtable = "state1", dbkey = c("region", "population"))

ss3 <- SQLDataFrame(dbname = "inst/extdata/test2.db", dbtable = "state2", dbkey = c("region", "population"))

ss11 <- ss1[1:10, 2:3]
ss12 <- ss1[8:15, 2:3]

ss21 <- ss2[8:15,2:3]
ss22 <- ss2[15:18, 2:3]

ss31 <- ss3[15:18, 2:3]


tbl11 <- .extract_tbl_from_SQLDataFrame(ss11)
tbl12 <- .extract_tbl_from_SQLDataFrame(ss12)

tbl21 <- .extract_tbl_from_SQLDataFrame(ss21)
tbl22 <- .extract_tbl_from_SQLDataFrame(ss22)

u1 <- dbplyr:::union.tbl_lazy(tbl11, tbl12)


u2 <- dbplyr:::union.tbl_lazy(tbl11, tbl21)
## Error: `x` and `y` must share the same src, set `copy` = TRUE (may be slow)
## Call `rlang::last_error()` to see a backtrace


## 3/19/2019. reimplement "union" function. 
u2 <- SQLDataFrame::union(ss11, ss21)
u3 <- SQLDataFrame::union(u2, ss22)

aa <- rbind(ss11, ss22)
aa <- rbind(ss11, ss22, ss12) 

## debug saveSQLDataFrame.

aa <- rbind(ss11, ss21, ss31)
saveSQLDataFrame(aa)


## tryCatch error message from "union" where the attaching databases were done. 
## Error in result_create(conn@ptr, statement) : 
##   too many attached databases - max 10

## union, open new connection to a local database (temp), reuse in saveSQLDataFrame.
## rbind, pairwise union, go down the tree, lhs, rhs, fun(), leaf()... 
