library(plyr)
library(tidyr)
library(dplyr)
library(sparklyr)
library(quantmod)


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
  
dt <- fundamentals %>%
  left_join(stocks)

sc <- spark_connect(master = "local")
