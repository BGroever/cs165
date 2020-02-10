-- Correctness test: Test for updates on columns with index
--
-- SELECT col1 FROM tbl5 WHERE col2 >= -12 AND col2 < 988;
--
s1=select(db1.tbl5.col2,-12,988)
f1=fetch(db1.tbl5.col1,s1)
print(f1)
--
-- SELECT col3 FROM tbl5 WHERE col1 >= -3 AND col1 < 997;
--
s2=select(db1.tbl5.col1,-3,997)
f2=fetch(db1.tbl5.col3,s2)
print(f2)
