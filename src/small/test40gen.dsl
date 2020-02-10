-- Correctness test: Update values
--
-- UPDATE tbl5 SET col1 = -10 WHERE col1 = -1;
-- UPDATE tbl5 SET col1 = -20 WHERE col2 = -22;
-- UPDATE tbl5 SET col1 = -30 WHERE col1 = -3;
-- UPDATE tbl5 SET col1 = -40 WHERE col3 = -444;
-- UPDATE tbl5 SET col1 = -50 WHERE col1 = -5;
--
u1=select(db1.tbl5.col1,-1,0)
relational_update(db1.tbl5.col1,u1,-10)
u2=select(db1.tbl5.col2,-22,-21)
relational_update(db1.tbl5.col1,u2,-20)
u3=select(db1.tbl5.col1,-3,-2)
relational_update(db1.tbl5.col1,u3,-30)
u4=select(db1.tbl5.col3,-444,-443)
relational_update(db1.tbl5.col1,u4,-40)
u5=select(db1.tbl5.col1,-5,-4)
relational_update(db1.tbl5.col1,u5,-50)
shutdown
