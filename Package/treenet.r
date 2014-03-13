#!/usr/local/risk/linux/bin/Rscript

# generate TreeNet spec in csv format
gen_tree_spec<-function(target_tree, ntrees, outfile){
    write(paste("[1]", target_tree$initF),file=outfile[1],append=TRUE)
    for ( a in 1:ntrees[1] ){
        temp <- data.frame(target_tree$trees[[a]])
        temp_str <- paste(0:(nrow(temp)-1), 
                          unlist(temp[1]), 
                          unlist(temp[2]), 
                          unlist(temp[3]), 
                          unlist(temp[4]), 
                          unlist(temp[5]), 
                          unlist(temp[6]), 
                          unlist(temp[7]), 
                          unlist(temp[8]), 
                          sep=",")
        write(temp_str, file=outfile[1], append=TRUE)
    }
}

# Variable Appearance Count in the TreeNet
relative.count <- function(object, n.trees, scale.=FALSE, sort. = TRUE)
{
   if( missing( n.trees ) ){
      if ( object$train.fraction < 1 ){
         n.trees <- gbm.perf( object, method="test", plot.it=FALSE )
      }
      else if ( !is.null( object$cv.error ) ){
         n.trees <- gbm.perf( object, method="cv", plot.it = FALSE )
      }
      else{
         # If dist=multinomial, object$n.trees = n.trees * num.classes
         # so use the following instead.
         n.trees <- length( object$train.error )
      }
      cat( paste( "n.trees not given. Using", n.trees, "trees.\n" ) )
      if (object$distribution == "multinomial"){
          n.trees <- n.trees * object$num.classes
      }
   }
   get.rel.count <- function(obj)
   {
      lapply(split(rep(1, length(obj[[1]])),obj[[1]]),sum) # 6 - Improvement, 1 - var name
      #lapply(split(obj[[6]]),obj[[1]]),sum) # 6 - Improvement, 1 - var name
   }

   temp <- unlist(lapply(object$trees[1:n.trees],get.rel.count))
   rel.inf.compact <- unlist(lapply(split(temp,names(temp)),sum))
   rel.inf.compact <- rel.inf.compact[names(rel.inf.compact)!="-1"]

   # rel.inf.compact excludes those variable that never entered the model
   # insert 0's for the excluded variables
   rel.inf <- rep(0,length(object$var.names))
   i <- as.numeric(names(rel.inf.compact))+1
   rel.inf[i] <- rel.inf.compact

   names(rel.inf) <- object$var.names

   if (scale.){
      rel.inf <- rel.inf / max(rel.inf)
   }
   if (sort.){
      rel.inf <- rev(sort(rel.inf))
   }

   return(rel.inf=rel.inf)
}

# generate tree info: spec/vars importance/vars count
gen_tree_infos <- function(target_tree, ntrees, out_dir){
    outfile1 <- paste(out_dir[1], "treeNet_spec.csv",sep="/")
    loginfo("Generate tree spec: %s", outfile1)
    gen_tree_spec(target_tree, ntrees, outfile1)

    outfile2 <- paste(out_dir[1], "treeNet_var_imp.csv",sep="/")
    loginfo("Generate variable importance file: %s", outfile2)
    tree_inf <- relative.influence(target_tree, ntrees, sort. = T)
    write.table(tree_inf, outfile2[1], sep=",")

    outfile3 <- paste(out_dir[1], "treeNet_var_count.csv",sep="/")
    loginfo("Generate variable count file: %s", outfile3)
    tree_count <- relative.count(target_tree, ntrees, sort. = T)
    write.table(tree_count, outfile3[1], sep=",")
}

# Common function that read in csv file
read_data <- function(filename, dlm=",")
{
# TODO: filter more na.strings
    data <- read.csv(filename, 
                     header=T, 
                     na.strings="None", 
                     row.names=NULL, 
                     sep=dlm)
    return (data)
}

# Common function that build treeNet scritps
build_tn <- function(data, target, weight, tree_size, 
                     tree_depth=3, minobs=300, sample_rate=0.6, learning_rate=0.3)
{
    loginfo("start building treeNet model...")
    loginfo("data dim: %s", dim(data))
    loginfo("target variable lenght: %s", length(target))
    loginfo("weight variable lenght: %s", length(weight))
    loginfo("tree_size: %s",tree_size)
    loginfo("tree_depth: %s", tree_depth)
    loginfo("minobsinnode: %s", minobs)
    loginfo("sample rate: %s", sample_rate)
    loginfo("learning_rate: %s", learning_rate)

    tn <- gbm.fit(x=data, y=target, w=weight, distribution="bernoulli", 
                  n.trees = tree_size,
                  bag.fraction=sample_rate,
                  shrinkage=learning_rate,
                  interaction.depth=tree_depth,
                  n.minobsinnode=minobs,
                  verbose=TRUE
                  )
    loginfo("Finish building treeNet model...")
    return (tn)
}

# a framework to train a treeNet model
tn_frame <- function( dev_data_file, model_var_list, target, weight, tn_model_file, 
                     oot_data_file, reserve_vars, dev_score_csv, oot_score_csv,
                     tree_size=2000, sample_rate=0.6, learning_rate=0.05, tree_depth=3, minobs=300,
                      dlm=",")
{
# 1st step: build a treeNet object
    loginfo("start read in dev data file %s", dev_data_file)
    data <- read_data(dev_data_file, dlm) 
    loginfo("finish read in dev data file %s", dev_data_file)
    
    loginfo("read model variable list from file %s", model_var_list)
    cols <- read.table(model_var_list, col.names=c("vars"), fill=FALSE, strip.white=TRUE, stringsAsFactor=FALSE)
    loginfo(paste("model vars: ", cols$vars))

    loginfo("target variable: %s", target)
    loginfo("weight variable: %s", weight)

    loginfo("start filtering records that have variable %s >= 2", target)
    loginfo("raw data dim %s", dim(data))
    target_col <- names(data) %in% target
    data_f <- data[which(data[target_col]<2),]
    loginfo("filtered data dim: %s", dim(data_f))

    select_cols <- names(data_f) %in% cols$vars
    X_raw <- data_f[select_cols]
    loginfo("X dim: %s", dim(X_raw))
    X <- as.matrix(X_raw)

    Y <- data_f[target_col]

    wgt_col <- names(data_f) %in% weight
    W <- data_f[wgt_col]

    if (!is.numeric(X)) {
        logerror("The modeling variables contains non-numeric value")
        q(save="no", status=-1)
    }
    if (!is.numeric(Y[[1]])){
        logerror("The target variable contains non-numeric value")
        q(save="no", status=-1)
    }
    if (!is.numeric(W[[1]])){
        logerror("The weight variable contains non-numeric value")
        q(save="no", status=-1)
    }

    tn <- build_tn(X, Y[[1]], W[[1]], tree_size, tree_depth, 
                   minobs, sample_rate, learning_rate) 
    
    loginfo("Save the model to %s in Rdata format", tn_model_file)
    save(tn, file=tn_model_file)
#TODO: add more check on the spec folder
    gen_tree_infos(tn, tree_size, ".")

# 2nd step: generate tn scores on both dev/oot data
    loginfo("read in resever variables from file %s", reserve_vars)
    outcols <- read.table(reserve_vars, col.names=c("vars"), fill=FALSE, strip.white=TRUE, stringsAsFactor=FALSE)
    out_cols <- names(data) %in% outcols$vars
    
    loginfo("start generate score for dev date")
    tn_score <- predict(tn, data[select_cols], type="response", n.trees=tree_size)
    loginfo("finish generate score for dev date")
    dev_scores <- cbind(data[out_cols], tn_score)
    loginfo("start write down dev scores to file: %s", dev_score_csv)
    write.table(dev_scores, file=dev_score_csv, sep=dlm, row.names=F)
    loginfo("finish write down dev scores")

    loginfo("read in oot data from file: %s", oot_data_file)
    oot <- read_data(oot_data_file, dlm)
    select_cols <- names(oot) %in% cols$vars
    oot_f <- oot[select_cols]
    loginfo("start generate score for oot date")
    tn_score_2 <- predict(tn, oot_f, type="response", n.trees=tree_size)
    loginfo("finish generate score for oot date")
    out_cols <- names(oot) %in% outcols$vars
    oot_scores <- cbind(oot[out_cols], tn_score_2)
    loginfo("start write down oot scores to file: %s", oot_score_csv)
    write.table(oot_scores, file=oot_score_csv, sep=dlm, row.names=F)
    loginfo("finish write down oot scores")

    loginfo("Job Done")
}

library(getopt)
library(logging)
basicConfig()

spec = matrix(c('help', 'h', 0, 'logical',
                'dev_data', 'd', 1, 'character',
                'oot_data', 'o', 1, 'character',
                'model_var_list', 'm', 1, 'character',
                'resver_vars','r', 1, 'character',
                'target_var','t', 1, 'character',
                'weight_var','w', 1, 'character',
                'tn_model','n', 1, 'character',
                'dev_scores','e', 1, 'character',
                'oot_scores','f', 1, 'character',
                'tree_size','z', 2, 'integer',
                'sample_rate','s', 2, 'double',
                'learning_rate','l', 2, 'double',
                'tree_depth','p', 2, 'integer',
                'dlm','i', 2, 'character',
                'minobs', 'b', 2, 'integer',
                'log', 'g', 2, 'character'
                ), byrow=TRUE, ncol=4)
opt = getopt(spec)

if( !is.null(opt$help) ){
    cat(getopt(spec, usage=TRUE))
    q(save="no", status=1)
}

if( !is.null(opt$log) ){
    addHandler(writeToFile, file=opt$log)
}

if (is.null(opt$tree_size)) {opt$tree_size = 2000 }
if (is.null(opt$tree_depth)) {opt$tree_depth = 3 }
if (is.null(opt$sample_rate)) {opt$sample_rate = 0.6 }
if (is.null(opt$learning_rate)) {opt$learning_rate = 0.05 }
if (is.null(opt$minobs)) {opt$minobs = 300}
if (is.null(opt$dlm)) {opt$dlm = "," }

loginfo("dev data file: %s", opt$dev_data)
loginfo("oot data file: %s", opt$oot_data)
loginfo("model var list file: %s", opt$model_var_list)
loginfo("reserve var list file: %s", opt$resver_vars)
loginfo("target_var: %s", opt$target)
loginfo("weight_var: %s", opt$weight_var)
loginfo("Tree size: %s", opt$tree_size)
loginfo("Tree depth: %s", opt$tree_depth)
loginfo("sample rate: %s", opt$sample_rate)
loginfo("learning rate: %s", opt$learning_rate)
loginfo("minobs: %s", opt$minobs)

library(gbm)

tn_frame(opt$dev_data, opt$model_var_list, opt$target_var, opt$weight_var, opt$tn_model, 
         opt$oot_data, opt$resver_vars, opt$dev_scores, opt$oot_scores, opt$tree_size, 
         opt$sample_rate, opt$learning_rate, opt$tree_depth, opt$minobs, opt$dlm)
