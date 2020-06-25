# MIT License
#
# Copyright (c) 2016 Decentlab GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


library(httr)

query <- function(domain,
                  apiKey,
                  timeFilter = "",
                  device = "//",
                  location = "//",
                  sensor = "//",
                  includeNetworkSensors = FALSE,
                  channel = "//",
                  aggFunc = "",
                  aggInterval = "",
                  doCast = TRUE,
                  convertTimestamp = TRUE,
                  timezone = "UTC") {
  selectVar <- 'value'
  fill <- ''
  interval <- ''
  baseUrl <- paste0(
    "https://",
    domain,
    "/api/datasources/proxy/1/query?db=main&epoch=ms&q="
  )
  
  if (aggFunc != "") {
    selectVar <- paste0(aggFunc, '("value") as value')
    fill <- 'fill(null)'
  }
  
  if (aggInterval != "") {
    interval <- paste0(', time(', aggInterval, ')')
  }
  
  if (timeFilter != "") {
    timeFilter <- paste0(' AND ', timeFilter)
  }
  
  filter <- paste0(" location =~ ", location,
                   " AND node =~ ", device,
                   " AND sensor =~ ", sensor,
                   " AND ((channel =~ ", channel, " OR channel !~ /.+/)",
                   if (includeNetworkSensors) ")" else " AND channel !~ /^link-/)")
  
  q <- paste(
    'SELECT ',
    selectVar,
    ' FROM "measurements" ',
    ' WHERE ', filter, timeFilter,
    ' GROUP BY "uqk" ',
    interval,
    fill,
    sep = ' '
  )
  
  res <- httr::GET(paste0(baseUrl, URLencode(q)),
                   httr::add_headers(Authorization = paste0("Bearer ", apiKey)))
  json <- httr::content(res)
  
  series <- json$results[[1]]$series
  
  if (is.null(series)) {
    stop("No series returned: ", json)
  }
  
  lists <- lapply(series, function(s) {
    tbl <- do.call(rbind, lapply(s$values, rbind))
    cbind(tbl, rep(s$tags$uqk, dim(tbl)[1]))
  })
  
  mat <- do.call(rbind, lists)  # stack groups in rows
  # replace nulls with NAs, otherwise unlist will remove nulls
  mat[sapply(mat, is.null)] <- NA
  
  time <- unlist(mat[, 1])
  value <- unlist(mat[, 2])
  series <- unlist(mat[, 3])
  df <- cbind.data.frame(time, series, value)
  
  if (doCast) {
    require(reshape)
    df <- reshape::cast(df, time ~ series)
  }
  if (convertTimestamp) {
    df$time <- as.POSIXct(df$time / 1000,
                          origin = "1970-01-01",
                          tz = timezone)
  }
  return(df)
}

getLast <- function(domain,
                    apiKey,
                    timeFilter = "",
                    device = "//",
                    location = "//",
                    sensor = "//",
                    includeNetworkSensors = FALSE,
                    channel = "//",
                    convertTimestamp = TRUE,
                    timezone = "UTC") {
  doCast <- FALSE
  aggFunc <- "last"
  selectVar <- 'value'
  fill <- ''
  interval <- ''
  baseUrl <- paste0(
    "https://",
    domain,
    "/api/datasources/proxy/1/query?db=main&epoch=ms&q="
  )
  
  if (timeFilter != "") {
    timeFilter <- paste0(' AND ', timeFilter)
  }
  
  if (aggFunc != "") {
    selectVar <- paste0(aggFunc, '("value") as value')
    fill <- 'fill(null)'
  }
  
  
  filter <- paste0(" location =~ ", location,
                   " AND node =~ ", device,
                   " AND sensor =~ ", sensor,
                   " AND ((channel =~ ", channel, " OR channel !~ /.+/)",
                   if (includeNetworkSensors) ")" else " AND channel !~ /^link-/)")
  
  q <- paste(
    'SELECT ',
    selectVar,
    ' FROM "measurements" ',
    ' WHERE ', filter, timeFilter,
    ' GROUP BY "uqk", "location", "sensor", "channel", "unit" ',
    interval,
    fill,
    sep = ' '
  )
  
  res <- httr::GET(paste0(baseUrl, URLencode(q)),
                   add_headers(Authorization = paste0("Bearer ", apiKey)))
  json <- httr::content(res)
  
  if (res$status_code != 200) {
    stop(json$message, json$error)
  }
  series <- json$results[[1]]$series
  
  if (is.null(series)) {
    stop("No series returned")
  }
  
  lists <- lapply(series, function(s) {
    tbl <- do.call(rbind, lapply(s$values, rbind))
    i <- 1
    for (tag in s$tags) {
      tagcol <- rep(tag, dim(tbl)[1])
      tbl <- cbind(tbl, tagcol)
      colnames(tbl)[dim(tbl)[2]] <- names(s$tags)[i]
      i <- i + 1
    }
    tbl
  })
  
  mat <- do.call(rbind, lists)  # stack groups in rows
  # replace nulls with NAs, otherwise unlist will remove nulls
  mat[sapply(mat, is.null)] <- NA
  df <- data.frame(time = unlist(mat[, 1]), value = unlist(mat[, 2]),
                   apply(mat[, 3:dim(mat)[2]], 2, unlist))
  
  if (convertTimestamp) {
    df$time <- as.POSIXct(df$time / 1000,
                          origin = "1970-01-01",
                          tz = timezone)
  }
  
  return(df)
}