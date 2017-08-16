init = load '/usr/local/JarFiles/WikiLinkFilt/part-r-00000' as (inLink:chararray, outLink:chararray);
tempGrp = GROUP init by inLink;
temp = Foreach tempGrp generate flatten (($0, $1.$1));
C = foreach temp generate $0 as inLink, 1 as pagerank, flatten($1) as url;
pagerank = foreach C generate pagerank / COUNT(url) as pagerank, flatten(url) as to_url;
cgrp = cogroup pagerank by to_url, C by url;
new_pagerank      = foreach cgrp generate group as url,
                      (1 - 0.85) + 0.85 * SUM (pagerank.pagerank)
                      as pagerank,
                      flatten(C.url) as links,
                      flatten(C.pagerank) AS C;
store new_pagerank into '/usr/local/JarFiles/PageRankTemp/';
nonulls           = filter new_pagerank by C is not null and pagerank is not null;
pagerank_diff     = foreach nonulls generate ABS (C - pagerank);
grpall            = group pagerank_diff all;
max_diff          = foreach grpall generate MAX (pagerank_diff);
store max_diff into '/usr/local/JarFiles/PageRank/';
