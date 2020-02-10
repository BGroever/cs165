-- Correctness test: Do inserts in tbl5.
--
-- Let table tbl5 have a secondary index (col2) and a clustered index (col3), so, all should be maintained when we insert new data.
-- This means that the table should be always sorted on col3 and the secondary indexes on col2 should be updated
--
-- Create Table
create(tbl,"tbl5",db1,4)
create(col,"col1",db1.tbl5)
create(col,"col2",db1.tbl5)
create(col,"col3",db1.tbl5)
create(col,"col4",db1.tbl5)
-- Create a clustered index on col1
create(idx,db1.tbl5.col1,sorted,clustered)
-- Create an unclustered btree index on col2
create(idx,db1.tbl5.col2,btree,unclustered)
--
--
-- Load data immediately in the form of a clustered index
load("./small/data5.csv")
--
-- INSERT INTO tbl5 VALUES (-1,-11,-111,-1111);
-- INSERT INTO tbl5 VALUES (-2,-22,-222,-2222);
-- INSERT INTO tbl5 VALUES (-3,-33,-333,-2222);
-- INSERT INTO tbl5 VALUES (-4,-44,-444,-2222);
-- INSERT INTO tbl5 VALUES (-5,-55,-555,-2222);
--
relational_insert(db1.tbl5,-1,-11,-111,-1111)
relational_insert(db1.tbl5,-2,-22,-222,-2222)
relational_insert(db1.tbl5,-3,-33,-333,-2222)
relational_insert(db1.tbl5,-4,-44,-444,-2222)
relational_insert(db1.tbl5,-5,-55,-555,-2222)
