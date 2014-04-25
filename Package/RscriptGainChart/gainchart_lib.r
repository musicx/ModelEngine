library(logging)
library(getopt)
basicConfig()
options(stringsAsFactors=FALSE)

eps <- 1e-32

# single score performance calculate function
perf_score <- function(is_bad, score, unit_wgt, dol_wgt, ID, scrvar, data_label, var_label, 
                       catch_data=NULL, catch_vars=NULL, 
                       min_score=NULL, max_score=NULL, rank=1000, 
                       order_descending=TRUE, NA_score=-9999)
{
  loginfo("perf score start...")
  loginfo("score name: %s", scrvar)
  loginfo("data label: %s", data_label)
  loginfo("Unit weight: %s", names(unit_wgt))
  loginfo("Dollar weight: %s", names(dol_wgt))
  if(order_descending)
  {
    loginfo("Order: Desc")
  }
  else
  {
    loginfo("Order: Asc")
  }

  names(is_bad) <- "is_bad"
  names(score) <- "score"
  names(unit_wgt) <- "unit_wgt"
  names(dol_wgt) <- "dol_wgt"
  
  bad <- is_bad
  names(bad)<- "bad"
  bad[which(bad$bad>1.5),] <- 0
  bad[which(bad$bad>0.5),] <- 1
  good <- 1-bad
  names(good)<-"good"
  if(is.null(max_score)) { max_score <- ceiling(max(score))}
  if(is.null(min_score)) { min_score <- floor(min(score))}
  loginfo("max score: %s", max_score)
  loginfo("min score: %s", min_score)
  loginfo("rank: %s", rank)


  base_step <- ceiling(max_score-min_score)/rank
  loginfo("generate raw data...")
  raw_data <- cbind(good, bad, unit_wgt, dol_wgt, score, catch_data)
  
  raw_data$pmt <- raw_data$dol_wgt
  raw_data$n <- raw_data$unit_wgt
  raw_data$good_pmt <- raw_data$pmt*raw_data$good
  raw_data$good <- raw_data$unit_wgt*raw_data$good
  raw_data$bad_pmt <- raw_data$pmt*raw_data$bad
  raw_data$bad <- raw_data$unit_wgt*raw_data$bad
  
  loginfo("filter out missing scores/pmt/n/good/bad...")
  raw_data <- raw_data[which(!is.na(raw_data$score)),]
  raw_data$score <- as.numeric(raw_data$score)
  raw_data <- raw_data[which(!is.na(raw_data$pmt)),]
  raw_data <- raw_data[which(!is.na(raw_data$n)),]
  raw_data <- raw_data[which(!is.na(raw_data$good)),]
  raw_data <- raw_data[which(!is.na(raw_data$bad)),]
  
  #raw_data$score[which(is.na(raw_data$score))] <- NA_score
  loginfo("calculate score group...")
  if(order_descending)
  {
    raw_data$score_grp <- floor((raw_data$score-min_score)/base_step)*base_step+min_score
  }
  else
  {
    raw_data$score_grp <- ceiling((raw_data$score-min_score)/base_step)*base_step+min_score
  }
  raw_data$score_grp[which(raw_data$score_grp <= min_score)] <- min_score+base_step
  raw_data$score_grp[which(raw_data$score_grp > max_score)] <- max_score
  
  if(!is.null(catch_vars))
  {
    for (var in catch_vars)
    {
      raw_data[var] <- raw_data[var]*raw_data$unit_wgt
    }
  }
  
  loginfo("aggregate on score group")
  rank_data <- aggregate(raw_data[c("n", "good","bad","pmt", "good_pmt", "bad_pmt", catch_vars)], by=raw_data["score_grp"], FUN=sum)
  order_data <- rank_data[order(rank_data$score_grp, decreasing=order_descending),]
  order_data$sum_unit <- order_data$n
  order_data$bad_unit <- order_data$bad
  order_data$good_unit <- order_data$good
  order_data$catch_unit <- cumsum(order_data$bad)/sum(order_data$bad)
  order_data$hit_unit <- cumsum(order_data$bad)/cumsum(order_data$n)
  order_data$opt_unit <- cumsum(order_data$n)/sum(order_data$n)
  order_data$sum_dol <- order_data$pmt
  order_data$bad_dol <- order_data$bad_pmt
  order_data$good_dol <- order_data$good_pmt
  order_data$catch_dol <- cumsum(order_data$bad_pmt)/sum(order_data$bad_pmt)
  order_data$hit_dol <- cumsum(order_data$bad_pmt)/cumsum(order_data$pmt)
  order_data$opt_dol <- cumsum(order_data$pmt)/sum(order_data$pmt)
  order_data$table <- ID
  order_data$scrvar <- scrvar
  order_data$data_label <- data_label
  order_data$var_label <- var_label
  
  catch_vars_sum <- NULL
  catch_vars_rate <- NULL
  if(!is.null(catch_vars))
  {
    catch_vars_sum <- paste(catch_vars, "_sum", sep="")
    catch_vars_rate <- paste("catch_", catch_vars, sep="")
    for (var in catch_vars)
    {
      order_data[paste(var,"_sum", sep="")] <- cumsum(order_data[var])
      order_data[paste("catch_", var, sep="")] <- cumsum(order_data[var])/sum(order_data[var])
    }
  }
  loginfo("perf score end...")
  loginfo("")
  return (order_data[c("score_grp", "sum_unit", "bad_unit", "good_unit",
                       "catch_unit", "hit_unit", "opt_unit", "sum_dol",
                       "bad_dol", "good_dol", "catch_dol", "hit_dol", "opt_dol",
                       "table", "scrvar", "data_label", "var_label",
                       catch_vars, catch_vars_sum, catch_vars_rate)])
  
}

# data_list: data filenames list
# scores_vars: score name list
# order_list: each score's order in the scores_vars
# class_vars: classifier variable list
# data_label_list: each data's label
# catch_vars: catch variables
score_performance<-function(data_list, target_var, scores_vars, 
                            unit_weight_var, dollar_weight_var, order_list=NULL,
                            class_vars=NULL, data_label_list=NULL, catch_vars=NULL, 
                            max_score=NULL, min_score=NULL, rank=10000)
{
  # read in all the dataset
  loginfo("Score Performance Calculate Start...")
  data_num = length(data_list)
  if(is.null(data_label_list))
  {
    for(i in c(1:data_num) )
    {
      if(i==1) {data_label_list <- c("dev")}
      if(i==2) {data_label_list <- c(data_label_list, "oot")}
      if(i > 2) {data_label_list <- c(data_label_list, paste("oot",i-1,sep=""))}
    }
  }
  if(length(data_label_list)!=data_num)
  {
    logerror("Lable list number is not the same with data file number")
    q(save="no", status=-1)
  }
  loginfo(paste("data label list: ", data_label_list))
  
  data <- NULL
  if(data_num == 1){
    loginfo("Start to read in csv file: %s , it may take a while...", data_list[1])
    data <- read.csv(data_list[1])
    loginfo("Finish reading csv file: %s ", data_list[1])
    data$dummy_time_window <- data_label_list[1]
  }
  else{
    for( i in c(1:data_num) )
    {
      loginfo("Start to read in csv file: %s , it may take a while...", data_list[i])
      tmp <- read.csv(data_list[i])
      loginfo("Finish reading csv file: %s ", data_list[i])
      tmp$dummy_time_window <- data_label_list[i]
      if(i==1)
      { 
          data <- tmp 
      }
      else 
      { 
          data <- rbind(data, tmp)
      }
    }
  }
  
  # if max_score/min_score does not exist, calculate them
  if(is.null(max_score)){max_score <- ceiling(max(data[scores_vars]))}
  if(is.null(min_score)){min_score <- floor(min(data[scores_vars]))}
  if(is.null(order_list)) {order_list<- rep("decreasing", length(scores_vars))}
  # get all classes
  if(data_num==1) {class_vars <- c("dummy_time_window", class_vars)}
  if(is.null(class_vars)) {class_vars <- c("dummy_time_window")}
  class_unique <- data.frame(apply(data[class_vars], 2, unique))
  
  gen_class_max<-function(class_unique, class_vars){
    class_var_num <- length(class_vars)
    if(class_var_num == 1){return (class_unique)}
    y <- merge(class_unique[[1]], class_unique[[2]], by=NULL)
    if(class_var_num > 2){
      for(j in 3:j ){
        y <- merge(y, class_unique[[j]], by=NULL)
      }
    }
    names(y)<-class_vars
    return (y)
  }
  
  class_matrix <- gen_class_max(class_unique, class_vars)
  step <- (max_score -min_score)/rank
  ranks <- data.frame(seq(from=(max_score), to=(min_score+step), by=-step))  
  names(ranks)<- 'rank'
  ranks$rank <- round(ranks$rank/step)
  scores <- NULL
  for(i in c(1:dim(class_matrix)[1])){
    sub_data <- data
    for(j in c(1:length(scores_vars)))
    {
      score_name <- c(scores_vars[j])
      for(k in c(1:length(class_vars)))
      {
        sub_data <- sub_data[sub_data[class_vars[k]]==class_matrix[i,k],]
      }
      is_bad <- sub_data[target_var]
      if (is.numeric(sub_data[score_name]))
      {
          loginfo("Score is numeric")
      }
      else
      {
          loginfo("score is not numeric")
      }
      score <- sub_data[score_name]
      unit_wgt <- sub_data[unit_weight_var]
      dol_wgt <- sub_data[dollar_weight_var]
      catch_data <- sub_data[catch_vars]
      desc_order <- TRUE
      ID <- paste(class_matrix[i,], score_name, sep="|")
      data_label <- sub_data['dummy_time_window'][1,1]
      var_label <- 'Overall'
      if (tolower(order_list[j]) != "decreasing") { desc_order <- FALSE }
      single_score_rank <- perf_score(is_bad, score, unit_wgt, dol_wgt, 
                                      ID, score_name, data_label, var_label,                       
                                      catch_data, catch_vars, min_score, max_score, rank, desc_order)
      single_score_rank$rank <- round(single_score_rank$score_grp/step)
      row.names(single_score_rank)<-NULL
      
      single_scores <- merge(ranks, single_score_rank, by="rank", all=TRUE, sort=FALSE)
      single_scores <- single_scores[order(single_scores$rank, decreasing=TRUE),]
      row.names(single_scores) <- NULL
      single_scores$sum_unit[is.na(single_scores$sum_unit)] <- 0.0
      single_scores$bad_unit[is.na(single_scores$bad_unit)] <- 0.0
      single_scores$good_unit[is.na(single_scores$good_unit)] <- 0.0
      single_scores$sum_dol[is.na(single_scores$sum_dol)] <- 0.0
      single_scores$bad_dol[is.na(single_scores$bad_dol)] <- 0.0
      single_scores$good_dol[is.na(single_scores$good_dol)] <- 0.0
      single_scores$score_grp <- single_scores$rank*step

      if(!is.null(catch_vars))
      {
        for (var in catch_vars)
        {
          single_scores[var][is.na(single_scores[var])] <- 0.0
          single_scores[paste(var,"_sum", sep="")] <- cumsum(single_scores[var])
          single_scores[paste("catch_", var, sep="")] <- cumsum(single_scores[var])/sum(single_scores[var])
        }
      }
      single_scores$catch_unit <- cumsum(single_scores$bad_unit)/sum(single_scores$bad_unit)
      single_scores$hit_unit <- cumsum(single_scores$bad_unit)/cumsum(single_scores$sum_unit)
      single_scores$opt_unit <- cumsum(single_scores$sum_unit)/sum(single_scores$sum_unit)
      single_scores$catch_dol <- cumsum(single_scores$bad_dol)/sum(single_scores$bad_dol)
      single_scores$hit_dol <- cumsum(single_scores$bad_dol)/cumsum(single_scores$sum_dol)
      single_scores$opt_dol <- cumsum(single_scores$sum_dol)/sum(single_scores$sum_dol)
      single_scores$table <- ID
      single_scores$scrvar <- score_name
      single_scores$data_label <- data_label
      single_scores$var_label <- var_label
      scores <- rbind(scores, single_scores)
      row.names(scores) <- NULL
    }
  } 
  loginfo("Score Performance Calculate End...")   
  return (scores)
}

opt_performance<-function(data_list, target_var, scores_vars, 
                          unit_weight_var, dollar_weight_var, order_list=NULL,
                          class_vars=NULL, data_label_list=NULL, catch_vars=NULL, 
                          max_score=NULL, min_score=NULL, score_rank=1000, opt_rank=1000, scores=NULL)
{
  if(is.null(scores))
  {
    scores <- score_performance(data_list, target_var, scores_vars, unit_weight_var,
                              dollar_weight_var, order_list, class_vars, data_label_list, catch_vars,
                              max_score, min_score, score_rank)
  }
  loginfo("Operation Point Performance Calculate Start...")
  tables <- unique(scores$table)
  ranks <- data.frame(seq(from=(1), to=(opt_rank), by=1))  
  names(ranks)<- c("rank")
  opts <- NULL
  for(idx in c(1:length(tables)))
  {
    
    sub_scores <- scores[scores$table==tables[idx],]
    sub_scores$unit_rank <- ceiling(sub_scores$opt_unit*opt_rank)
    sub_scores$dol_rank <- ceiling(sub_scores$opt_dol*opt_rank)
    sub_scores$unit_rank[which(sub_scores$unit_rank <= 0)] <- 1
    sub_scores$dol_rank[which(sub_scores$dol_rank <= 0)] <- 1
    
    table_name <- sub_scores$table[1]
    score_name <- sub_scores$scrvar[1]
    data_label <- sub_scores$data_label[1]
    var_label <- sub_scores$var_label[1]
    unit_data <- aggregate(sub_scores[c("sum_unit", "bad_unit", "good_unit", catch_vars)], by=sub_scores["unit_rank"], FUN=sum)
    order_unit_data <- unit_data[order(unit_data$unit_rank),]
    row.names(order_unit_data)<- NULL
    order_unit_data$catch_unit <- cumsum(order_unit_data$bad_unit)/sum(order_unit_data$bad_unit)
    order_unit_data$hit_unit <- cumsum(order_unit_data$bad_unit)/cumsum(order_unit_data$sum_unit)

    full_unit_data <- merge(ranks, order_unit_data, by.x="rank", by.y="unit_rank", all=TRUE, sort=FALSE)
    order_unit_data <- full_unit_data[order(full_unit_data$rank),]
    if(!is.null(catch_vars)){
      for (var in catch_vars)
      {
        order_unit_data[var][is.na(order_unit_data[var])] <- 0.0
        order_unit_data[paste(var,"unit", sep="_")] <- order_unit_data[var]
        order_unit_data[paste(var,"_sum_unit", sep="")] <- cumsum(order_unit_data[var])
        order_unit_data[paste("catch_unit_", var, sep="")] <- cumsum(order_unit_data[var])/sum(order_unit_data[var])
      }
    }
    row.names(order_unit_data)<- NULL
    dol_data  <- aggregate(sub_scores[c("sum_dol", "bad_dol", "good_dol", catch_vars)], by=sub_scores["dol_rank"], FUN=sum)
    order_dol_data <- dol_data[order(dol_data$dol_rank),]
    row.names(order_dol_data)<- NULL
    order_dol_data$catch_dol <- cumsum(order_dol_data$bad_dol)/sum(order_dol_data$bad_dol)
    order_dol_data$hit_dol <- cumsum(order_dol_data$bad_dol)/cumsum(order_dol_data$sum_dol)
    if(!is.null(catch_vars)){
      for (var in catch_vars)
      {
        order_dol_data[var][is.na(order_dol_data[var])] <- 0.0
        order_dol_data[paste(var,"dol", sep="_")] <- order_dol_data[var]
      }
    }
    
    full_data <- merge(order_unit_data, order_dol_data, by.x="rank", by.y="dol_rank", all=TRUE, sort=FALSE)
    order_data <- full_data[order(full_data$rank),]
    if(!is.null(catch_vars)){
      for (var in catch_vars)
      {
        order_data[paste(var,"dol", sep="_")][is.na(order_data[paste(var,"dol", sep="_")])] <- 0.0
        order_data[paste(var,"_sum_dol", sep="")] <- cumsum(order_data[paste(var,"dol", sep="_")])
        order_data[paste("catch_dol_", var, sep="")] <- cumsum(order_data[paste(var,"dol", sep="_")])/sum(order_data[paste(var,"dol", sep="_")])
      }
    }
    row.names(order_data)<- NULL
    order_data$table <- table_name
    order_data$scrvar <- score_name
    order_data$data_label <- data_label
    order_data$var_label <- var_label
    order_data$sum_unit[is.na(order_data$sum_unit)] <- 0.0
    order_data$bad_unit[is.na(order_data$bad_unit)] <- 0.0
    order_data$good_unit[is.na(order_data$good_unit)] <- 0.0
    order_data$sum_dol[is.na(order_data$sum_dol)] <- 0.0
    order_data$bad_dol[is.na(order_data$bad_dol)] <- 0.0
    order_data$good_dol[is.na(order_data$good_dol)] <- 0.0
    order_data$catch_unit <- cumsum(order_data$bad_unit)/sum(order_data$bad_unit)
    order_data$hit_unit <- cumsum(order_data$bad_unit)/cumsum(order_data$sum_unit)
    order_data$catch_dol <- cumsum(order_data$bad_dol)/sum(order_data$bad_dol)
    order_data$hit_dol <- cumsum(order_data$bad_dol)/cumsum(order_data$sum_dol)  
    opts <- rbind(opts, order_data)
  }
  loginfo("Operation Point Performance Calculate End...")
  return(opts)
}

generate_table<-function(scores, opts, scores_vars)
{
  loginfo("start generate tables...")
  label_list <- unique(scores$data_label)
  scores_table <- NULL
  opts_table <- NULL
  score_top_list <- c("catch", "hit", "opt", catch_vars) 
  opt_top_list <- c("catch", "hit", catch_vars) 

  for (top in score_top_list)
  {
    for (leader in c("unit", "dol") )
    {
      for (time_window in label_list)
      {
        label_scores <- scores[scores$data_label==time_window,]
        label_opts <- opts[opts$data_label==time_window,]
        for (score_name in scores_vars)
        {
          loginfo("target dataset score name: %s", score_name)
          loginfo("target dataset time_window: %s", time_window)
          sub_scores <- label_scores[label_scores$scrvar==score_name,]
          sub_opts <- label_opts[label_opts$scrvar==score_name,]
          loginfo("target sub scores dimision: %s", dim(sub_scores))
          loginfo("target sub opts dimision: %s", dim(sub_opts))
          if(is.null(scores_table))
          {
            scores_table <- data.frame(cbind(sub_scores$rank, sub_scores$score_grp))
            names(scores_table) <- c("ID", "Score")
          }
          if(is.null(opts_table))
          {
            opts_table <- data.frame(cbind(sub_opts$rank))
            names(opts_table) <- "ID"
          }
          
          
          if(top=="catch" || top == "hit" || top == "opt")
          {
            target_col <- paste(top, leader, sep="_")
            col_names <- names(scores_table)
            col_names <- c(col_names, paste(leader, top, time_window, score_name, sep="|"))
            scores_table <- data.frame(cbind(scores_table, sub_scores[target_col]))
            names(scores_table)<-col_names
            
            if(top != "opt")
            {
              opt_names <- names(opts_table)
              opt_names <- c(opt_names, paste(leader, top, time_window, score_name, sep="|"))
              opts_table <- data.frame(cbind(opts_table, sub_opts[target_col]))
              names(opts_table)<-opt_names
            }
          }   
          else {
            score_target_col <- paste('catch', top, sep="_")
            col_names <- names(scores_table)
            col_names <- c(col_names, paste(leader, top, time_window, score_name, sep="|"))
            scores_table <- data.frame(cbind(scores_table, sub_scores[score_target_col]))
            names(scores_table)<-col_names
            
            opt_target_col <- paste('catch', leader, top, sep="_")
            opt_names <- names(opts_table)
            opt_names <- c(opt_names, paste(leader, top, time_window, score_name, sep="|"))
            opts_table <- data.frame(cbind(opts_table, sub_opts[opt_target_col]))
            names(opts_table)<-opt_names
          }
        }
      }
    }
  }
  loginfo("finish generate tables...")

  loginfo("start write table to csv file ...")
  write.table(scores, file="scores_raw.csv", sep=",", row.names=F)
  write.table(opts, file="opts_raw.csv", sep=",", row.names=F)
  write.table(scores_table, file="score_table.csv", sep=",", row.names=F)
  write.table(opts_table, file="opt_table.csv", sep=",", row.names=F)
  loginfo("finish write table to csv file ...")
}

