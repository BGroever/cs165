-- Test for a clustered index select followed by a second predicate
--
-- Query in SQL:
-- SELECT sum(col1) FROM tbl4 WHERE (col3 >= 437 and col3 < 1437) AND (col2 >= 7494 and col2 < 9494);
--
s1=select(db1.tbl4.col3,437,1437)
f1=fetch(db1.tbl4.col2,s1)
s2=select(s1,f1,7494,9494)
f2=fetch(db1.tbl4.col1,s2)
print(f2)
a1=sum(f2)
print(a1)
