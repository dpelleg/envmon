M=read.csv('missing-hourly.csv', sep=",")
#Mneg=read.csv('negatives.csv', sep="\t")

# stations which are down for long periods of time (D. Carmel came online in late January, was up most of the year)
downstations_re = '^(Dor chemicals|Mobile_New|GADIV CTO)$';
downstations = grepl(M$station, pattern=downstations_re);

M = M[!downstations,]

nonscada_re = '^(SO2|NOX|TOLUEN|BENZN|PM10|PM2.5|NO2|NO|O3|formaldehyde|O-Xyle|EthylB|CO|1-3butadiene)$';
nonscada=grepl(M$sensor, pattern=nonscada_re);
fullnames=paste(M$station, M$sensor);

pol_list = c('SO2', 'NOX', 'TOLUEN', 'BENZN', 'PM10', 'PM2.5', 'NO2', 'NO', 'O3', 'formaldehyde', 'O-Xyle', 'EthylB', 'CO', 'CO', '1-3butadiene');

mt = t(as.matrix(M));

# compute mean availability per sensor
nsensors=ncol(mt);
nms=vector(length=nsensors, mode='character');
av=vector(length=nsensors, mode='numeric');
for (i in 1:nsensors) {
    nms[i] = sprintf("%s %s", mt[1,i], mt[2,i]);
    av[i] = mean(as.numeric(mt[-(1:2), i]));
}

bysensor = data.frame(nms, av);

av.nonscada = av;
if(0) { # when analysing negatives
    av.nonscada[!nonscada] = -1e6;

    ranked = order(av.nonscada, decreasing=T);
} else {
    av.nonscada[!nonscada] = 1e6;

    ranked = order(av.nonscada, decreasing=F);
}

for(i in 1:length(ranked)) {
   print(sprintf("%s: %.1f", nms[ranked[i]], 100*av[ranked[i]]/24));
}


                                        # compute mean availability per day
                                        # prepare membership of each sensor in a group, plus one group for everything
pol = matrix(nrow=1+length(pol_list), ncol=nsensors);
for(i in 1:length(pol_list)) {
    pol[i,] = M$sensor == pol_list[i];
}
                                        # last item for everything
pol_list = c(pol_list, 'all');
pol[length(pol_list),] = TRUE;

ndays=nrow(mt)-2;
nms=names(mt[,1])[-(1:2)]
av=matrix(nrow=nrow(pol), ncol=ndays);
for (i in 1:ndays) {
    for(j in 1:nrow(pol)) {
        v = mt[i+2, pol[j, ]];
        a = mean(as.numeric(v), rm.na=T);
        av[j, i] = (a/24);
    }
}


if(0) {
# compute vector of big drops:
# 1. number of observations drops 2% day over day
drop1 = ((av[-1] - av[-ndays])/av[-ndays] < -0.02);
# 2. drop of over 2 points from the smoothed value
drop2 = bydate$nms[which(av - smooth(av) < -0.02)];
# 3. lower than 80% availability
drop3 = which(av < 0.8);
}

                                        # show availability by pollutant
for(i in 1:nrow(pol)) {
print(sprintf("%s %.1f", pol_list[i], 100*mean(av[i,], na.rm=T)));
}

                                        #charts
for(i in 0:3) {
    i1 = i*4+1; i2=i1+3;
    matplot(t(av[i1:i2,]), type='b', col=rainbow(4),lwd=2,pch='o',lty=1)
    legend(legend=pol_list[i1:i2],x="bottomright", col=rainbow(4),lwd=2)
    readline();
}
