
#Observations, for date ranges 24:30 (last week of September 2016):
#    Checked for min.rho < -0.5 or so, and minmax.diff around 180
#
# When taking the angle (difference), no cosine:
#regavim: NO2, 137, NOX 149, TOLUEN 155, BENZN 126
#K.Binyamin: weak BENZN at 212 (-0.43)
# Igud: O3, 315
# K. Bialik: NO2, 111, NOX 114
# K. Atta: O3, 302
# Tivon: weak NO at 96 (-0.46), weak O3 at 316 (-0.44)
# Nesher, mobile/ein afek: no wind sensor
# Ahuza, weak PM2.5 at 92 (-0.45)
# Shprintzak: NO2 at 172, NOX at 172, NO at 175
#
#Observations, for date ranges 21:27
#    Checked for min.rho < -0.5 or so, 
# When taking the cosine of the angle:
# K.Hayim: NO2, 147; NOX, 154; TOL, 151; O.Xyle, 158. Under 0.5: EthylB, 162; PM10, 194; NO, 166; BENZN, 164 (0.4)
# Igud: PM10, 78; NOX, 78; NO2, 75; BENZEN, 92; NO, 83; 
# Bialik: NO2, 102; NOX, 119
# Atta: O3, 264
# Tivon: NO, 126; O3, 307
#Shprintzak: NO2, 163; NOX, 166; O3, 334
#D = read.csv("regavim-wind-sep.csv", header=TRUE);
#D = read.csv("igud-wind-sep.csv", header=TRUE)
#D = read.csv("hasidim-wind-sep.csv", header=TRUE)
#D = read.csv("bialik-wind-sep.csv", header=TRUE)
#D = read.csv("atta-wind-sep.csv", header=TRUE);
#D = read.csv("tivon-wind-sep.csv", header=TRUE);
#D = read.csv("ahuza-wind-sep.csv", header=TRUE);
D = read.csv("shprintzak-wind-sep.csv", header=TRUE);
# No wind sensor:
#D = read.csv("binyamin-wind-sep.csv", header=TRUE)
#D = read.csv("nesher-wind-sep.csv", header=TRUE);

ndeg=360;

# day of month
D.mday = as.numeric(sub("(\\d+)/(\\d+)/.*", "\\1", D$time));

match = is.element(D.mday, 16:21);

P.cand = colnames(D);
pollutants = c();
for(candpol in P.cand) {
    if(length(grep(candpol, c('station', 'TEMP', 'Filter', 'time', 'ITemp', 'RH', 'PREC', 'FILTER.2.5'))) == 0) {
        pollutants = c(pollutants, candpol);
    }
}

N = vector(length=length(pollutants));
Z = data.frame(ind=vector(length=length(pollutants)),
    min.cor = N, min.rho = N, min.p = N, max.cor = N, max.rho = N, max.p = N, minmax.diff = N
               );                #store the result

for(pol in 1:length(pollutants)) {
    col=pollutants[pol];
    Z$ind[pol] = col;
    print(col)
    if(mean(is.na(D[[col]][match])) < 0.2) {
        c=vector(length=ndeg);
        p=vector(length=ndeg);
        
        for(d in 1:ndeg) {
            angle = abs(D$WDD[match]-d);
            S=cor.test(cos(angle*pi/180), D[[col]][match], method='pearson');
            c[d] = S$estimate;
            p[d] = S$p.value;
        }
        Z$min.cor[pol] = which.min(c);;
        Z$min.rho[pol] = c[which.min(c)];
        Z$min.p[pol] = p[which.min(c)];
        
        Z$max.cor[pol] = which.max(c);;
        Z$max.rho[pol] = c[which.max(c)];
        Z$max.p[pol] = p[which.max(c)];
        
        Z$minmax.diff[pol] = abs(which.max(c) - which.min(c));
    }
}
