dbname <- system.file("extdata/test.db", package = "SQLDataFrame")
obj <- SQLDataFrame(dbname = dbname, dbtable = "state",
                    dbkey = "state")
obj1 <- SQLDataFrame(dbname = dbname, dbtable = "state",
                     dbkey = c("region", "population"))

###############
## subsetting
###############

obj[1]
obj["region"]
obj$region
obj[]
obj[,]
obj[NULL, ]
obj[, NULL]

## by numeric / logical / character vectors
obj[1:5, 2:3]
obj[c(TRUE, FALSE), c(TRUE, FALSE)]
obj[c("Alabama", "South Dakota"), ]
obj1[c("South\b3615.0", "West\b3559.0"), ]
### Remeber to add `.0` trailing for numeric values. If not sure,
### check `ROWNAMES()`.

## by SQLDataFrame
obj_sub <- obj[sample(10), ]
obj[obj_sub, ]

## by a named list of key column values (or equivalently data.frame /
## tibble)
obj[data.frame(state = c("Colorado", "Arizona")), ]
obj[tibble(state = c("Colorado", "Arizona")), ]
obj[list(state = c("Colorado", "Arizona")), ]
obj1[list(region = c("South", "West"),
          population = c("3615.0", "365.0")), ]
### remember to add the '.0' trailing for numeric values. If not sure,
### check `ROWNAMES()`.

obj1[list(region = c("South", "West"),
          population = c("3615.0", "365.0")),
     other = "random", ]
### expect_error


###################
## filter & mutate 
###################

## subsetting by \code{filter} with any column variable
obj %>% filter(region == "West" & size == "medium")
obj1 %>% filter(region == "West" & population > 10000)

obj %>% mutate(p1 = population / 10)
obj %>% mutate(s1 = size)
