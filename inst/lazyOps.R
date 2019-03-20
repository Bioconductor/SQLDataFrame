ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db", dbtable = "state", dbkey = c("region", "population"))
ss2 <- ss1[1:10, 2:3]
ss3 <- ss1[8:15, 2:3]
ss4 <- ss1[15:17, 2:3]
ss5 <- ss1[6:15, 1, drop = FALSE]

aa <- rbind(ss2, ss3, ss4)
bb <- left_join(ss2, ss5, by = dbkey(ss2))


##----------------
## op_vars, op_grps, op_sort
##----------------
identical(op_vars(aa@tblData), colnames(aa@tblData))
## [1] TRUE

op_grps(aa@tblData)
## character(0)

op_sort(aa@tblData)
## NULL

##----------------------------------------------
## op_set_op,
## op_select, op_join, op_filter, op_mutate,
## op_base_remote
##----------------------------------------------
tbla <- aa@tblData
tblb <- bb@tblData

class(tbla$ops)
## [1] "op_set_op" "op_double" "op"
class(tbla$ops$x$ops)
## [1] "op_select" "op_single" "op"
class(tbla$ops$y$ops)
## [1] "op_select" "op_single" "op"

class(tbla$ops$x$ops$x)
## [1] "op_set_op" "op_double" "op"
is(tbla$ops$x$ops$x$x)
## [1] "tbl_dbi"  "oldClass"
is(tbla$ops$x$ops$x$x$ops)
## [1] "op_select"
is(tbla$ops$x$ops$x$x$ops$x)
## [1] "op_select"
is(tbla$ops$x$ops$x$x$ops$x$x)
## [1] "op_filter"
is(tbla$ops$x$ops$x$x$ops$x$x$x)
## [1] "op_mutate"
class(tbla$ops$x$ops$x$x$ops$x$x$x$x)
## [1] "op_base_remote" "op_base"        "op"
class(tbla$ops$x$ops$x$x$ops$x$x$x$x$x)
## [1] "ident"    "character"          

is(tbla$ops$x$ops$x$y)
## [1] "tbl_dbi"  "oldClass"
is(tbla$ops$x$ops$x$y$ops)
## [1] "op_select"


is(tbla$ops$y$ops$x)
## [1] "op_select"
is(tbla$ops$y$ops$x$x)
## [1] "op_filter"
is(tbla$ops$y$ops$x$x$x)
## [1] "op_mutate"
is(tbla$ops$y$ops$x$x$x$x)
## [1] "op_base_remote"
class(tbla$ops$y$ops$x$x$x$x)
## [1] "op_base_remote" "op_base"        "op"
is(tbla$ops$y$ops$x$x$x$x$x)
## [1] "ident"               "sql"                 "character"          
## [4] "vector"              "data.frameRowLabels" "SuperClassMethod"   

class(tblb$ops)
## [1] "op_join" "op_double" "op"

### Summary: dbplyr/R/lazy-ops.R, dbplyr/R/verb-set-ops.R, dbplyr/R/tbl-lazy.R(single methods)
## "op_base": $x,$vars
##           i.e., FROM (no operation added on the dbtable)
## "op_single": $name,$x,$dots,$args.
##              i.e., filter,mutate,arrange,select,rename,summarize,distinct,group_by, ...
## "op_set_op"("op_double"): $name,$x,$y,$args
##              i.e., intersect,union_all,setdiff,union("op_set_op", $args$type="UNION")
