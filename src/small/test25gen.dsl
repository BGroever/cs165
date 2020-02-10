-- Test for a clustered index select followed by a second predicate
--
-- Query in SQL:
-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= 1178 and col2 < 1180);
-- SELECT sum(col1) FROM tbl4 WHERE (col2 >= 1059 and col2 < 1061);
--
s1=select(db1.tbl4.col2,1178,1180)
f1=fetch(db1.tbl4.col1,s1)
a1=sum(f1)
print(a1)
s2=select(db1.tbl4.col2,1059,1061)
f2=fetch(db1.tbl4.col1,s2)
a2=sum(f2)
print(a2)
