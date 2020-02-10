--
-- tbl3 has a secondary b-tree tree index on col2, and a clustered index on col3 with the form of a sorted column
-- testing for correctness
--
-- Query in SQL:
-- SELECT col1 FROM tbl4 WHERE col3 >= 787 and col3 < 789;
-- SELECT col1 FROM tbl4 WHERE col3 >= 546 and col3 < 550;
--
-- since col3 has a clustered index, the index is expected to be used by the select operator
s1=select(db1.tbl4.col3,787,789)
f1=fetch(db1.tbl4.col1,s1)
print(f1)
s2=select(db1.tbl4.col3,546,550)
f2=fetch(db1.tbl4.col1,s2)
print(f2)
