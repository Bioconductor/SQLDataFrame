con <- dbConnect(RSQLite::SQLite(), ":memory:")
src <- src_dbi(con)     

dbWriteTable(con, "mtcars", mtcars)
mtc <- tbl(con, "mtcars") %>% filter(mpg == 21)
mtc
# Source:   lazy query [?? x 11]
# Database: sqlite 3.22.0 [:memory:]
    mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
  <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
1    21     6   160   110   3.9  2.62  16.5     0     1     4     4
2    21     6   160   110   3.9  2.88  17.0     0     1     4     4

####################################
## get the "sql" query from "mtc"
qry <- db_sql_render(con, mtc)
qry
<SQL> SELECT *
FROM `mtcars`
WHERE (`mpg` = 21.0)

################################################
## retrieve "mtc" from lazy query. Both works!
tbl(src, qry)
# Source:   SQL [?? x 11]
# Database: sqlite 3.22.0 [:memory:]
    mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
  <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
1    21     6   160   110   3.9  2.62  16.5     0     1     4     4
2    21     6   160   110   3.9  2.88  17.0     0     1     4     4
> tbl(con, qry)
# Source:   SQL [?? x 11]
# Database: sqlite 3.22.0 [:memory:]
    mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
  <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
1    21     6   160   110   3.9  2.62  16.5     0     1     4     4
2    21     6   160   110   3.9  2.88  17.0     0     1     4     4

