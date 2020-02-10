--
-- Testing for batching queries
-- Queries with full overlap (subsumption)
--
-- Query in SQL:
-- 10 Queries of the type:
-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;
--
--
batch_queries()
s0=select(db1.tbl3_batch.col4,6962,7022)
s1=select(db1.tbl3_batch.col4,6964,7020)
s2=select(db1.tbl3_batch.col4,6966,7018)
s3=select(db1.tbl3_batch.col4,6968,7016)
s4=select(db1.tbl3_batch.col4,6970,7014)
s5=select(db1.tbl3_batch.col4,6972,7012)
s6=select(db1.tbl3_batch.col4,6974,7010)
s7=select(db1.tbl3_batch.col4,6976,7008)
s8=select(db1.tbl3_batch.col4,6978,7006)
s9=select(db1.tbl3_batch.col4,6980,7004)
batch_execute()
f0=fetch(db1.tbl3_batch.col1,s0)
f1=fetch(db1.tbl3_batch.col1,s1)
f2=fetch(db1.tbl3_batch.col1,s2)
f3=fetch(db1.tbl3_batch.col1,s3)
f4=fetch(db1.tbl3_batch.col1,s4)
f5=fetch(db1.tbl3_batch.col1,s5)
f6=fetch(db1.tbl3_batch.col1,s6)
f7=fetch(db1.tbl3_batch.col1,s7)
f8=fetch(db1.tbl3_batch.col1,s8)
f9=fetch(db1.tbl3_batch.col1,s9)
print(f0)
print(f1)
print(f2)
print(f3)
print(f4)
print(f5)
print(f6)
print(f7)
print(f8)
print(f9)
