library(Matrix)

source("mysparse.R");

#D0 = read.csv("all-daily.tsv", sep="\t");

summary(D0);

m <- (D0$station != "Mobile_New") & (D0$sensor == "BENZN" | D0$sensor == "Benzene-Namal" | D0$sensor == "Benzine HDS");

nowind <- which(D0$station ==  "Igud (check-post)" & D0$sensor == "WDS" & D0$val < 2);
nowind.days <- D0$date[nowind];

if(FALSE) {
    sharav.dry <- which(D0$station ==  "Igud (check-post)" & ((D0$sensor == "RH" & D0$val < 61)));
    sharav.hot <- which(D0$station ==  "Igud (check-post)" & ((D0$sensor == "TEMP" & D0$val > 26)));
    sharav = sharav.dry & sharav.hot;
    sharav.days <- D0$date[sharav];
}

wind.dir <- function(fr, dir) {
    range=10;
    if(dir < range) {
        wind <- which(fr$station ==  "Igud (check-post)" & fr$sensor == "WDD" & (fr$val > ((dir-range) %% 360) | fr$val <= (dir+range)));
    } else {
        wind <- which(fr$station ==  "Igud (check-post)" & fr$sensor == "WDD" & (fr$val > (dir-range) & fr$val <= (dir+range)));
    }
    wind_days <- fr$date[wind];
    return (wind_days);
}

wind.speed <- function(fr, spd) {
    range=0.5;
    wind <- which(fr$station ==  "Igud (check-post)" & fr$sensor == "WDS" & (fr$val > spd-range) & (fr$val <= spd+range));
    wind_days <- fr$date[wind];
    return (wind_days);
}

cor.from.dates <- function(fr, prefilter, filter.d) {

    filter_b <- fr$date %in% filter.d;
    D = fr[prefilter & filter_b,];

    sensormap = factor(D$station);

    DD = my.sparse(i=1+D$epoch_days, j=as.numeric(sensormap), x=D$val);

    colnames(DD) = levels(sensormap);

    C = cor(as.matrix(DD), method="spearman", use="pairwise.complete.obs");

    return(C);
}

# get the initial vector of names
C <- cor.from.dates(D0, m, wind.dir(D0, 0));
out = data.frame(r=rownames(C)[row(C)], c=colnames(C)[col(C)]);
ncols=length(as.vector(C));

out = matrix(ncol=2+ncols, nrow=9);
out[1,] = c("wind dir", "N", sprintf("%s:%s", rownames(C)[row(C)], colnames(C)[col(C)]));
for(spd in c(0:7)) {
    wspd = spd;
    w <- wind.speed(D0, wspd);
    if(length(w) > 0) {
        corrs = as.vector(cor.from.dates(D0, m, w));
        out[spd+2,] = c(wspd, length(w), corrs);
    }
    print(spd)
}

if(FALSE) {
indices <- data.frame(ind= which(abs(C) > 1e-3, arr.ind=TRUE))
indices$rnm <- rownames(C)[indices$ind.row]
indices$cnm <- rownames(C)[indices$ind.col]

nontriv_indices = which(indices$ind.row > indices$ind.col)

write.table(sprintf("%s %d observations", filter_desc, nrow(D)), "cor-benzn.txt", append=TRUE);
write.table(C, "cor-benzn.txt", append=TRUE, sep="\t");
}

