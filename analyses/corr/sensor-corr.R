                                        # Generate daily data, by, for example:
# ./dump-all-daily.pl| awk 'NR==1 || $2 == 2016 {print}' > all-daily-2016.tsv
library(Matrix)
D = read.csv("all-daily-2017.tsv", sep="\t", header=T);

summary(D);

sensormap = factor(paste(D$station, D$sensor))

DD = sparseMatrix(i=1+D$epoch_days, j=as.numeric(sensormap), x=D$val)

colnames(DD) = levels(sensormap);

#C = cor(as.matrix(DD));
C = cor(as.matrix(DD), method='spearman')

indices <- data.frame(ind= which(C > 0.99, arr.ind=TRUE))
indices$rnm <- rownames(C)[indices$ind.row]
indices$cnm <- rownames(C)[indices$ind.col]

nontriv_indices = which(indices$ind.row > indices$ind.col)

write.table(indices[nontriv_indices,], 'nontriv_cor.tsv', sep="\t")
