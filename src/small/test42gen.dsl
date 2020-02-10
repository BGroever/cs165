-- Correctness test: Delete values and run queries after inserts, updates, and deletes
--
-- DELETE FROM tbl5 WHERE col1 = -10;
-- DELETE FROM tbl5 WHERE col2 = -22;
-- DELETE FROM tbl5 WHERE col1 = -30;
-- DELETE FROM tbl5 WHERE col3 = -444;
-- DELETE FROM tbl5 WHERE col1 = -50;
-- SELECT col1 FROM tbl5 WHERE col2 >= -100 AND col2 < 20;
--
d1=select(db1.tbl5.col1,-10,-9)
relational_delete(db1.tbl5,d1)
d2=select(db1.tbl5.col2,-22,-21)
relational_delete(db1.tbl5,d2)
d3=select(db1.tbl5.col1,-30,-29)
relational_delete(db1.tbl5,d3)
d4=select(db1.tbl5.col3,-444,-443)
relational_delete(db1.tbl5,d4)
d5=select(db1.tbl5.col1,-50,-49)
relational_delete(db1.tbl5,d5)
s1=select(db1.tbl5.col2,-100,20)
f1=fetch(db1.tbl5.col1,s1)
print(f1)
