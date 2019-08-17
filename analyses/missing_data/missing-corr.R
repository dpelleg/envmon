                                        # correlate amount of missing values with readings

# read missing values
M=read.csv('missing-hourly-2017.csv', sep="\t")

                                        # stations which are down for long periods of time (D. Carmel came online in late January, was up most of the year)
downstations_re = '^(Dor chemicals|Mobile_New|GADIV CTO)$';
downstations = grepl(M$station, pattern=downstations_re);

M = M[!downstations,]

nonscada_re = '^(SO2|NOX|TOLUEN|BENZN|PM10|PM2.5|NO2|NO|O3|formaldehyde|O-Xyle|EthylB|CO|1-3butadiene)$';
nonscada=grepl(M$sensor, pattern=nonscada_re);

M = M[nonscada,]
                                        # convert to a data frame
missing = data.frame();
for(i in 3:ncol(M)) {
    date = colnames(M)[i];
    ndate = substr(date, 2, nchar(date));
    d1=data.frame(station=M[,1], sensor=M[,2], availability=as.numeric(M[,i]), date=as.numeric(ndate));
    d1$low.avail <- ifelse(d1$availability < 22,1,0);
    missing = rbind(missing, d1);
    }

                                        # read sensor readings
D = read.csv('all-daily.tsv', header=T, sep="\t");

Um = unique(subset(missing, select=c(station, sensor)));
Ud = unique(subset(D[grepl(D$station, pattern='Flares'),], select=c(station, sensor)));
    
# correlate emmissions with missing values

cv = data.frame();
for(d in 1:nrow(Ud)) {

    dst = Ud$station[d];
    dse = Ud$sensor[d];
    Ds = subset(D, station == dst & sensor == dse);
                                        # join
    DM = merge(missing, Ds, by=c('date'));
    for(m in 1:nrow(Um)) {
        mst = as.character(Um$station[m]);
        mse = as.character(Um$sensor[m]);
        print(paste(mse, mst, dse, dst));
        DMss = subset(DM, station.x == mst & sensor.x == mse);
        c = cor(DMss$availability, DMss$val, method='spearman');
        ncv = data.frame(dstation=dst, dsensor=dse, mstation=mst, msensor=mse, cor=c);
        cv = rbind(cv, ncv);
    }
}

or = order(abs(cv$cor), decreasing=T);
