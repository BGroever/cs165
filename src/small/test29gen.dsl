--
-- Query in SQL:
--
-- tbl4_clustered_btree has a secondary sorted index on col2, and a clustered b-tree index on col3
-- testing for correctness
--
-- Query in SQL:
-- SELECT col1 FROM tbl4_clustered_btree WHERE col3 >= 723 and col3 < 725;
-- SELECT col1 FROM tbl4_clustered_btree WHERE col3 >= 1282 and col3 < 1286;
--
-- since col3 has a clustered index, the index is expected to be used by the select operator
s1=select(db1.tbl4_clustered_btree.col3,723,725)
f1=fetch(db1.tbl4_clustered_btree.col1,s1)
print(f1)
s2=select(db1.tbl4_clustered_btree.col3,1282,1286)
f2=fetch(db1.tbl4_clustered_btree.col1,s2)
print(f2)
