SET default_parallel 20;

A = load '/usr/local/JarFiles/twitter.txt' using PigStorage('\t') as (user:int, follower:int);
B = foreach A generate $0,$1;
C = Join A by ($0,$1), B by ($1, $0);
D = foreach C generate flatten (($0,$1));
E = foreach D generate Flatten(($0<$1 ? ($0,$1) : ($1,$0)));
F = distinct E;
dump F;
