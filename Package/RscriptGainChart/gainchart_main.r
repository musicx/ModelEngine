library(logging)
library(getopt)
basicConfig()

source("gainchart_lib.r")

trim <- function(x) gsub("^\\s+|\\s+$", "", x)

spec = matrix(c('help', 'h', 0, 'logical',
                'dataset_list', 'd', 1, 'character',
                'target_var','t', 1, 'character',
                'unit_weight_var','u', 1, 'character',
                'dol_weight_var','w', 1, 'character',
                'score_names','s', 1, 'character',
                'max_score','a', 1, 'double',
                'min_score','i', 1, 'double',
                'rank','r', 2, 'integer',
                'catch_vars','c', 2, 'character',
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

if(is.null(opt$rank)) { opt$rank=1000}

data_list <- trim(strsplit(opt$dataset_list, ',')[[1]])
target_var <- trim(opt$target_var)
scores_vars <- trim(strsplit(opt$score_names, ',')[[1]])
unit_weight_var <- trim(opt$unit_weight_var)
dollar_weight_var <- trim(opt$dol_weight_var)
max_score <- opt$max_score
min_score <- opt$min_score

loginfo(paste("data files: ", data_list))
loginfo("target_var: %s", target_var)
loginfo(paste("scores_vars: ", scores_vars))
loginfo("unit_weight: %s", unit_weight_var)
loginfo("dol_weight: %s", dollar_weight_var)
loginfo("Max score: %s", max_score)
loginfo("Min score: %s", min_score)

score_rank <- opt$rank
opt_rank <- opt$rank
loginfo("rank: %s", opt$rank)

catch_vars <- NULL
if(!is.null(opt$catch_vars))
{
    catch_vars <- trim(strsplit(opt$catch_vars,',')[[1]])
}
loginfo(paste("catch vars: ", catch_vars))

scores <- score_performance(data_list, target_var, scores_vars, 
                        unit_weight_var, dollar_weight_var, catch_vars = catch_vars,
                        max_score=max_score, min_score=min_score, rank=score_rank)

opts <- opt_performance(data_list, target_var, scores_vars, 
                        unit_weight_var, dollar_weight_var, catch_vars = catch_vars,
                        max_score=max_score, min_score=min_score, score_rank=score_rank, opt_rank=opt_rank, scores=scores)

generate_table(scores, opts, scores_vars)

