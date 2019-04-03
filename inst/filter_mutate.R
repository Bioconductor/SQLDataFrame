############
### filter
############

dbfile <- system.file("extdata/test.db", package = "SQLDataFrame")
obj <- SQLDataFrame(dbname = dbfile, dbtable = "state", dbkey = "state")
obj1 <- obj %>% filter(region == "West" & size == "medium")
obj1

class(obj1@tblData$ops)
## [1] "op_base_remote" "op_base"        "op"       
obj1 <- saveSQLDataFrame(obj1)

obj2 <- obj %>% mutate(size1 = size)
class(obj2@tblData$ops)
## [1] "op_mutate" "op_single" "op"

## obj3 <- saveSQLDataFrame(obj2)  ## works!
## need to update saveSQLdataFrame for "mutate".  -- done!

## for "mutate", open a temporary connection, similar to "union/join"
obj4 <- obj2 %>% mutate(p1 = population)
aa <- saveSQLDataFrame(obj4, dbtable = "obj4_1")

aa <- rbind(obj[1:5, ], obj[10:15, ])
aa1 <- aa %>% filter(region == "West")
aa2 <- aa1 %>% mutate(s1 = size)  ## dbtable() works!

aa <- obj %>% mutate(s1 = size)
aa1 <- rbind(aa[1:6, ], aa[11:16, ]) ## mutate + union, works!
aa2 <- aa %>% filter(size == "medium")  ## mutate + filter, works!
aa3 <- aa1 %>% mutate(p1 = population)  ## mutate + rbind + mutate, works!
aa4 <- aa1 %>% mutate(p2 = population -1)  ## numeric calculation, works!

aa1 <- saveSQLDataFrame(aa1, dbtable = "aa1_1") ## works!
aa2 <- saveSQLDataFrame(aa2, dbtalbe = "aa2_1") ## works!
