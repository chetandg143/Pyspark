from functools import reduce

lis = [1,20,3,40,5,60,7,80]
m = list(map(lambda  a : a * 2 , lis ))
print(m)

a =list(map(lambda  a : a + 2 , lis))
print(a)

s  = list(map(lambda s : s -1  , lis))
print(s)

d = list(map(lambda d : d % 2  ,lis))
print(d)

df = list(filter(lambda  x : x!=5 , lis))
print(df)

re =reduce(lambda x,y : x + y , lis)
print(re)


def add(n):
    return n+n

nums = [2,4,6,8,10]
m1 = list(map(add, nums))
print(m1)

num1 = [2,4,6,8,10]
num2 = [1,3,5,7,9]
m2 = list(map(lambda x,y :x + y , num1, num2))
print(m2)


l = ['abc','xyz','tibil','123']
test = list(map(list ,l))
print(test)

names = ['apple' , 'all', 'cake','dog']
fun =[]
for i in names:
    fun.append(lambda i=i : print(i))

for f in fun:
    print(f())

up =list(map(lambda x : x.capitalize() ,names ))
print(up)
print(sorted(names))


fib = [0,1,1,2,3,5,8,13,21,34,55]
res =list(filter(lambda  x: x % 2 , fib))
print(res)