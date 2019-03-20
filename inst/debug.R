## debug, ```aa <- union(ss2, ss3)```, shows correctly, then
## `devtools::document()`, doesn't show. Reconstruct, and show
## correctly again.

ss1 <- SQLDataFrame(dbname = "inst/extdata/test.db",
                    dbtable = "state",
                    dbkey = c("region", "population"))
ss2 <- ss1[1:10, 2:3]
ss3 <- ss1[8:15, 2:3]
aa <- SQLDataFrame::union(ss2, ss3)
devtools::document()
aa
## Error in (function (classes, fdef, mtable)  : 
##   unable to find an inherited method for function ‘dbkey’ for signature ‘"SQLDataFrame"’

## Martin: Looks like a problem for "devtools". No need to worry. 


traceback()
## 3: nrow(object) at SQLDataFrame-class.R#296
## 7: colnames(x) at SQLDataFrame-class.R#127
## 11: colnames(x@tblData) at SQLDataFrame-class.R#138
## 13: dimnames(x)
## 14: dimnames.tbl_sql(x)
aa@tblData
## Error in (function (classes, fdef, mtable)  : 
##   unable to find an inherited method for function ‘dbkey’ for signature ‘"SQLDataFrame"’
is(aa@tblData)
## [1] "tbl_dbi"  "oldClass"
methods("dimnames")
## [1] dimnames,DataTable-method    dimnames,SQLDataFrame-method
## [3] dimnames.data.frame          dimnames.tbl_sql*           
## see '?methods' for accessing help and source code
getAnywhere("dimnames.tbl_sql")
## A single object matching ‘dimnames.tbl_sql’ was found
## It was found in the following places
##   registered S3 method for dimnames from namespace dbplyr
##   namespace:dbplyr
## with value
## function (x) 
## {
##     list(NULL, op_vars(x$ops))
## }
## <bytecode: 0x559b95c73a48>
## <environment: namespace:dbplyr>
aa@tblData$ops
## $name
## [1] "set_op"

## $x
## Error in (function (classes, fdef, mtable)  : 
##   unable to find an inherited method for function ‘dbkey’ for signature ‘"SQLDataFrame"’
aa@tblData$ops$x
## 19: dbkey(x)
## 18: .f(.x[[i]], ...)
## 17: map(.x[sel], .f, ...)
## 16: map_if(ind_list, is_helper, eval_tidy)
## 15: vars_select_eval(.vars, quos)
## 14: tidyselect::vars_select(op_vars(op$x), !!!op$dots, .include = op_grps(op$x))
## 13: op_vars.op_select(x$ops)
## 12: op_vars(x$ops)
## 11: dim.tbl_sql(x)
## 10: dim(x)
## 9: nrow(x)

undebug(tidyselect::vars_select)
undebug(tidyselect:::vars_select_eval)
undebug(rlang:::eval_tidy)
aa@tblData$ops$x
rlang:::eval_tidy
## function (expr, data = NULL, env = caller_env()) 
## {
##     .Call(rlang_eval_tidy, expr, data, env)
## }
## <bytecode: 0x559b95599600>
## <environment: namespace:rlang>

## ind_list[[1]]
## <quosure>
## expr: ^dbkey(x)
## env:  0x5581f88b8ab0

con <- dbConnect(RSQLite::SQLite(), ":memory:")
copy_to(con, mtcars)
mtc <- tbl(con, "mtcars")
mtc1 <- filter(mtc, cyl == 4)
mtc2 <- filter(mtc, mpg > 30)
bb <- dbplyr:::union.tbl_lazy(mtc1, mtc2)
colnames(bb)
