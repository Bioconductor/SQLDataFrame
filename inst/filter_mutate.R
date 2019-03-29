ls()

dbfile <- system.file("extdata/test.db", package = "SQLDataFrame")
obj <- SQLDataFrame(dbname = dbfile, dbtable = "state", dbkey = "state")
obj1 <- obj %>% filter(region == "West" & size == "medium")
class(obj1@tblData$ops)
[1] "op_base_remote" "op_base"        "op"       
obj1 <- saveSQLDataFrame(obj1)

obj2 <- obj %>% mutate(size1 = size)
class(obj2@tblData$ops)
## [1] "op_mutate" "op_single" "op"

saveSQLDataFrame(obj2)  ## works!
## need to update saveSQLdataFrame for "mutate".  -- done!

## for "mutate", open a temporary connection, similar to "union/join"
obj3 <- obj2 %>% mutate(p1 = population)
aa <- saveSQLDataFrame(obj3, dbtable = "obj3_1")

aa <- rbind(obj[1:5, ], obj[10:15, ])
aa1 <- aa %>% filter(region == "West")
aa2 <- saveSQLDataFrame(aa1, dbtable = "aa1_2")

aa1 <- aa %>% mutate(r1 = region)
aa2 <- saveSQLDataFrame(aa1, dbtable = "aa1_3")
