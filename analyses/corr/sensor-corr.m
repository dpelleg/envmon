% correlation between pairs of sensors
% TO crate data: 
% ./dump-daily-avg.pl > daily-avg.tsv
% ~/script/csv2m.pl daily-avg.tsv

[D, Dkey] = daily_avg_tsv();

% construct a matrix with sensors as columns, days as rows

C = sparse(1+D.epoch_days, D.sensor, D.val);

