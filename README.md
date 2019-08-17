# envmon
Download environemtal monitoring data, analyze and create alerts

fetch-envhaifa.pl : Scrape the public web site of the data provider and store data into the directory "envdata". This script should be run hourly
alert.pl: analyze stored data, and create alerts into db/pending_msgs. This should run hourly
send_msgs.pl: clean and format messages, manage recipient load, and send message over email and SMS. This should run hourly during business hours

Also, this housekeeping sequence should also be run monthly:
fetch-envhaifa.pl -o envdata_hist -d prevmonth ; mv db/weekstats db/weekstats.bak

run-daily-dump.sh is a helper script to generate a dump of alerts to be downloaded into other analysis tools.
