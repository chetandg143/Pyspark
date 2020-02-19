import pandas as pd
import numpy as np
data =[1,2,3,4]
df = pd.DataFrame(data)
print(df)

data1 = [['alex',10] , ['sbc',23],['xyz',10]]
fd = pd.DataFrame(data1,columns=['Name','Age'],dtype=float)
print(fd)

d = {'Name':['tom' ,'abc','cdg'], 'Age':[21,25,26]}
g = pd.DataFrame(d)
print(g)

da = [{'a':2 ,'b':4 ,'c':6},{'a':3 , 'b':8}]
g1 = pd.DataFrame(da , index=['first','second'])
print(g1)


d = {'one' : pd.Series([1, 2, 3], index=['a', 'b', 'c']),
     'two' : pd.Series([1, 2, 3, 4], index=['a', 'b', 'c', 'd'])}

df = pd.DataFrame(d)

# Adding a new column to an existing DataFrame object with column label by passing new series

print ("Adding a new column by passing as Series:")
df['three']=pd.Series([10,20,30],index=['a','b','c'])
print(df)

print ("Adding a new column using the existing columns in DataFrame:")
df['four']=df['one']+df['three']

print (df)


d7 = {'one' : pd.Series([1, 2, 3], index=['a', 'b', 'c']),
     'two' : pd.Series([1, 2, 8, 4], index=['a', 'b', 'c', 'd'])}

d2= pd.DataFrame(d7)
print(d2.iloc[2])


df1 = pd.DataFrame([[1,2] , [3,4]] , columns=['a','b'])
df2 = pd.DataFrame([[5,6], [7,8]], columns=['a','b'])
df1 =df1.append(df2)
print(df1)
df1 = df1.drop(0)
print(df1)


P = {'Name':pd.Series(['Tom','James','Ricky','Vin','Steve','Smith','Jack']),
     'Age':pd.Series([25,26,25,23,30,29,23]),
     'Rating':pd.Series([4.23,3.24,3.98,2.56,3.20,4.6,3.8])}
p = pd.DataFrame(P)
print(p.T)
print(p.empty)




df = pd.DataFrame(np.random.randn(4,3),columns = ['col1','col2','col3'])
for row_index,row in df.iterrows():
    print (row_index,row)