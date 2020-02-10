-- Correctness test: Run query after inserts and updates
--
-- SELECT col1 FROM tbl5 WHERE col2 >= -155 AND col2 < 26;
--
s1=select(db1.tbl5.col2,-155,26)
f1=fetch(db1.tbl5.col1,s1)
print(f1)
