#!/bin/sh

# Generate a dump of yesterday's data, and clean up the dump directory

DIR='/usr/local/www/shared/dump'
CURR="`date '+%Y%m%d'`.tsv"

if [ -d $DIR ]
   then
     find $DIR -maxdepth 1 \! -newermt "a week ago" -exec rm -f "{}" \; 2>&1 > /dev/null
     ./dump-yesterdays-data.pl > $DIR/$CURR
     ln -fs $DIR/$CURR $DIR/latest
fi
