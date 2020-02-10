--
-- Query in SQL:
-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= 787 and col3 < 789;
-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= 546 and col3 < 550;
--
s1=select(db1.tbl4_ctrl.col3,787,789)
f1=fetch(db1.tbl4_ctrl.col1,s1)
print(f1)
s2=select(db1.tbl4_ctrl.col3,546,550)
f2=fetch(db1.tbl4_ctrl.col1,s2)
print(f2)
