library(readr)                       # library for csv data reading
library(vars)                        # library for simple VAR model
library(frequencyConnectedness)      # library for spillovers calculation

# read data
returns <- read_csv("C:/Users/12077/Desktop/returns_cleaned.csv")

# get all currency names
coins <- colnames(returns)[c(2:length(colnames(returns)))]

# get all cryptocurrencies names and conventional currencies names
crypto <- coins[c(1:103)]
conven <- coins[c(104:length(coins))]

# do one lag term VAR fitting, the running time increase exponentially with the lag term, p.
start_time <- Sys.time()   # record the starting time
fit <- VAR(returns[,coins], p=3, type = 'both')
end_time <- Sys.time()    # record the ending time
print(end_time - start_time)

# calculate spillovers
bounds <- c(pi+0.00001, pi/2, pi/7, 0)
dy12 <- spilloverBK09(fit, n.ahead=100, no.corr=F, partition=bounds)

# network analysis (need to parallel)
magic_fun <- function(x){
  return (x*100)}

# load the libraries for parallel processing
library(Rmpi)
library(doMPI)
library(foreach)
library(iterators)

cl = startMPIcluster()
registerDoMPI(cl)
clusterSize(cl)

foreach(m = 1:3) %dopar% {
  library(igraph)                      # library for graph analysis
  df <- data.frame(lapply(dy12$tables[m], magic_fun))
  
  #load data as graph
  g <- graph.adjacency(as.matrix(df), mode="directed", weighted=TRUE) # For directed
  g <- simplify(g, remove.loops = TRUE)   # omit self links
  
  # determine the width and the color of edges
  E(g)$width <- log(E(g)$weight * 10)
  color_map <- vector(mode='numeric')
  for (i in c(1:length(ends(g, es=E(g), names=T)[,1]))) {
    if (is.element(ends(g, es=E(g), names=T)[i,1], crypto) && is.element(ends(g, es=E(g), names=T)[i,2], conven)) {
      color_map <- append(color_map, '#6C3483')     # color links from cryptocurrencies to conventional currencies
    } else {
      if (is.element(ends(g, es=E(g), names=T)[i,1], conven) && is.element(ends(g, es=E(g), names=T)[i,2], crypto)) {
        color_map <- append(color_map, '#6C3483')
      } else {
        color_map <- append(color_map, '#1C2833')
      }
    }
  }
  E(g)$edge.color <- color_map
  
  # determine the color of nodes. The opacity reflects the within-market in-degree/out-degree
  g_c.in <- g
  g_c.in <- delete.edges(g_c.in, which(E(g_c.in)$edge.color=='#6C3483'))
  in_degree <- strength(g_c.in, mode='in')
  indeg <- (in_degree-min(in_degree))/(max(in_degree)-min(in_degree)) + 0.5
  color_map <- vector(mode='numeric')
  for (i in coins) {
    if (is.element(i, crypto)) {
      color_map[i] <- adjustcolor('darkred', alpha.f=indeg[i])
    } else {
      color_map[i] <- adjustcolor('darkblue', alpha.f=indeg[i])
    }
  }
  V(g)$color <- color_map
  
  # determine the size of nodes. The size reflects cross-market in-degree/out-degree
  g_c.out <- g
  g_c.out <- delete.edges(g_c.out, which(E(g_c.out)$edge.color=='#1C2833'))
  V(g)$size <- log(strength(g_c.out, mode='in') * 150)
  
  # draw the plot
  svg(file=paste(c('img', m, '.svg'), collapse=""))
  g %>%
    add_layout_(with_fr(maxit=5000), normalize()) %>%
    plot(edge.arrow.size=0.2, vertex.color=V(g)$color, vertex.label=NA,
         edge.color = adjustcolor(E(g)$edge.color, .02), vertex.frame.color=NA,
         edge.curved=0.3, shape='circle', vertex.arrow.size=1.5, vertex.size=V(g)$size)
  dev.off()
}

closeCluster(cl)