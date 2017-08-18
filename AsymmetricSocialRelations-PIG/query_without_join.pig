SET default_parallel 20;

A = load '/usr/local/JarFiles/twitter.txt' using PigStorage('\t') as (user:int, follower:int);
B = foreach A generate Flatten(($0<$1 ? ($0,$1) : ($1,$0)));
C = distinct B;
C = group B by ($0,$1);
D = foreach C generate COUNT(B) as c, B;
E = filter D by c==2;
F = foreach E generate flatten(B);
G = distinct F;
dump F;
