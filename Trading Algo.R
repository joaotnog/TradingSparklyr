library(plyr)
library(tidyr)
library(sparklyr)
library(quantmod)
library(mleap)
library(rpivotTable)
library(dplyr)



fundamentals = read.csv("C:/Users/Joao/Google Drive/Trading Algo/fundamentals.csv") %>%
  mutate(company=copany,
         date = as.Date(as.character(date))) %>%
  select(-copany)

symbols <- unique(as.character(fundamentals$company))

stocks.raw <- list()
for (s in symbols)
{
  print(paste0("Reading ",s,"..."))
  try({stocks.raw[[s]] <- get(getSymbols(s))})
}

stocks = lapply(stocks.raw, function(x) {
  dt <- as.data.frame(x) %>%
    select(contains("Adjusted")) %>%
    mutate(date = index(x))
  return(dt)
}) %>%
  join_all(type="full") %>%
  gather(company, price, -date) %>%
  mutate(company = gsub(".Adjusted","",company))






simple.return <- function(x,shift) {
    x/lag(x,shift)-1
  }

dt <- fundamentals %>%
  left_join(stocks) %>%
  filter(complete.cases(.)) %>%
  group_by(company) %>%
  # mutate_at(colnames(dt)[2:34], na.locf0) %>%
  mutate_at(colnames(dt)[2:34],funs(Trend=mean(diff(rank(.,lag(.,1),lag(.,2),lag(.,3),lag(.,4))),na.rm=TRUE)) %>%
  mutate_at(colnames(dt)[2:34],funs(Shock=./lag(.))) %>%
  mutate(return = simple.return(price,1)) %>%
  ungroup() %>%
  filter(!is.na(target.measure)) %>%
  filter(complete.cases(.)) %>%
  group_by(date) %>%
  mutate(target.measure=ifelse(return>quantile(return,.7),1,0)) %>%
  ungroup %>%
  mutate(time_period = cut(as.numeric(date),quantile(as.numeric(date),seq(0,1,.1)),labels = FALSE,include.lowest = TRUE))



# Exploratory analysis: cross section fundamentals deciles vs return
decile.fun <- function(v)
{
  trunc(v*10)+1
}

dt %>%
  group_by(date) %>%
  mutate_if(is.numeric,funs(decile.fun((rank(.,na.last="keep")-1)/sum(!is.na(.))))) %>%
  ungroup %>%
  mutate(target.measure = dt$target.measure) %>%
  rpivotTable(cols = "Revenues",
              aggregatorName = "Average",
              vals = "return",
              rendererName = "Line Chart")
  





sc <- spark_connect(master = "local")


dt_tbl <- copy_to(sc, dt, overwrite = TRUE)


run.ts.pipeline <- function(time = 5)
{
  print(paste0("Running until time period ",time,"..."))
  
  pipeline <- ml_pipeline(sc) %>%
    ft_r_formula(target_measure ~ .) %>%
    ml_random_forest_classifier()
  
  grid <- list(
    random_forest = list(
      num_trees = c(20, 100, 400),
      max_depth = c(5, 10, 20)
    )
  )
  cv <- ml_cross_validator(
    sc, estimator = pipeline, estimator_param_maps = grid,
    evaluator = ml_binary_classification_evaluator(sc),
    num_folds = 3, parallelism = 4
  )
  
  dt_tbl_train <- dt_tbl %>% filter(between(time_period,1,time)) %>% select(-X,-return,-date,-company,-price)
  dt_tbl_test <- dt_tbl %>% filter(between(time_period,time,time+1)) %>% select(-X,-return,-date,-company,-price)
  
  cv_model <- ml_fit(cv, dt_tbl_train)
  cv_metrics <- ml_validation_metrics(cv_model)
  
  pred_train <- ml_transform(cv_model,dt_tbl_train)
  pred_test <- ml_transform(cv_model,dt_tbl_test)
  
  performance_train <- ml_binary_classification_eval(pred_train)*2-1
  performance_test <- ml_binary_classification_eval(pred_test)*2-1
  predictive_power <- performance_test/performance_train-1

  actual_vs_predicted <- unlist(collect(pred_test)$probability)[c(FALSE,TRUE)]
  
  return(list(cv_metrics = cv_metrics,
              performance_train = performance_train,
              performance_test = performance_test,
              predictive_power = predictive_power))  
}


results = lapply(3:9,run.ts.pipeline)


# gini per time period prediction
plot(sapply(results,function(x) {x$performance_test}),type="l")
# predictive power per time period
plot(sapply(results,function(x) {x$predictive_power}),type="l")  

 


