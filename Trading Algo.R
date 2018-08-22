library(plyr)
library(tidyr)
library(sparklyr)
library(quantmod)
library(mleap)
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


save(stocks, file = "C:/Users/Joao/Google Drive/Trading Algo/stocks.Rdata")



simple.return <- function(x,shift) {x/lag(x,shift)-1}


dt <- fundamentals %>%
  left_join(stocks) %>%
  group_by(company) %>%
  mutate_at(colnames(dt)[2:34], na.locf0) %>%
  mutate(target.measure = simple.return(price,1)) %>%
  ungroup() %>%
  filter(!is.na(target.measure)) %>%
  filter(complete.cases(.))





sc <- spark_connect(master = "local")



dt_tbl <- copy_to(sc, dt, overwrite = TRUE)


# define and fit a pipeline
pipeline <- ml_pipeline(sc) %>%
  # ft_string_indexer("company", "company_cat") %>%
  # ft_vector_assembler(
  #   c(colnames(dt_tbl)[5:6],"company_cat"),
  #   "features"
  # ) %>%
  ft_vector_assembler(
      c(colnames(dt_tbl)[2:34]),
      "features"
    ) %>%
  ml_random_forest_regressor(
    label_col = "target_measure",
    num_trees = 10, max_depth = 20)

pipeline_model <- pipeline %>%
  ml_fit(dt_tbl)

pred_lr <- ml_predict(pipeline_model, dt_tbl)
  




