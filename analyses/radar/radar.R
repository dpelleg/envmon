
#p='BENZN';
#p='TOLUEN';
#p='NOX';
#p='O-Xyle';
p='PM10';
#p='SO2';
#p='EthylB';

DT = NULL;
stations = c('binyamin', 'igud', 'regavim');
st=1 ; D1 = read.csv(file=sprintf('pdf-%s-%s.csv', stations[st], p), header=FALSE);
st=2 ; D2 = read.csv(file=sprintf('pdf-%s-%s.csv', stations[st], p), header=FALSE);
st=3 ; D3 = read.csv(file=sprintf('pdf-%s-%s.csv', stations[st], p), header=FALSE);

if(mean(is.na(D1)) > 0.2) {
    D1 = 0;
} else {
    D1 = D1/sum(D1);
}
if(mean(is.na(D2)) > 0.2) {
    D2 = 0;
} else {
    D2 = D2/sum(D2);
}
if(mean(is.na(D3)) > 0.2) {
    D3 = 0;
} else {
    D3 = D3/sum(D3);
}

D=D1+D2+D3;
                                        #D=D3;
Dm = as.matrix(D);
Dm = t(Dm);

Np = dim(D)[1];                         #assume square

lonmin = 35.0
lonmax = 35.15
lon = lonmin + ((lonmax-lonmin)/Np)*(0:Np)
Ncol = 12;
colors = heat.colors(Ncol);
#colors = gray(0:Ncol / Ncol)
colors = colors[Ncol:1];

latmin = 32.74
latmax = 32.86
lat = latmin + ((latmax-latmin)/Np)*(0:Np)

image(lon, lat, (log(1+Dm)), col=colors);
title(p);

y=32.789167; x=35.040556; name = 'Igud';
points(x, y, type='p', pch=19);
text(y=y+.005, x=x, labels=name,cex=1.5)

y=32.831111; x=35.054444; name = 'K.Hayim'
points(x, y, type='p', pch=19);
text(y=y+.005, x=x, labels=name,cex=1.5)

y=32.78866; x=35.08511; name = 'K.Binyamin'
points(x, y, type='p', pch=19);
text(y=y+.005, x=x, labels=name,cex=1.5)

L = read.csv('zikuk.csv', header=FALSE, sep="\t"); lines(L[,2:1], lwd=3);
L = read.csv('tanks.csv', header=FALSE, sep="\t"); lines(L[,2:1], lwd=3);
L = read.csv('dor.csv', header=FALSE, sep="\t"); lines(L[,2:1], lwd=3);
