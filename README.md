# DATASETS

* gnome_all.csv.bz2 (uncompressed: 83.4 MB, bugs from 2002 to 2016, 555054 bugs )
* kde_all.csv.bz2 (uncompressed: 47.7 MB, bugs from 2002 to 2016, 339823 bugs )
* xfce_all.csv.bz2 (uncompressed: 1.3 MB, bugs from 2004 to 2016, 10199 bugs )

I got all the information using:

> https://SITE.bugzilla.org/buglist.cgi?bug_status=UNCONFIRMED&bug_status=CONFIRMED&bug_status=ASSIGNED&bug_status=REOPENED&bug_status=RESOLVED&bug_status=NEEDSINFO&bug_status=VERIFIED&bug_status=CLOSED&chfieldfrom=2003-06-29&chfieldto=2004-05-31&limit=20000&query_format=advanced&ctype=csv&human=1&order=Last+Changed

Slicing the time window you can download all the bugs, the maximum number of bugs retrieved by a single query is 10000 (also if limit has a value greater than it).

csv format: "Bug ID","Product","Component","Assignee","Status","Resolution","Summary","Changed"