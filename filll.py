lis = [1,2,3,4,5,6,7,8,9,15,35,20,10]
b= [2]

ab =  list(map(lambda a : a + 2 ,lis))
print(ab)

def div (a):
    if a %  5 == 0:
        return a

abc = map(div,ab)
print(list(abc))



# def app(a):
#     return a + 5
#
# abc = list(map(app,li))
# print(abc)
#
# lisss = []
# def prr(li):
#     for i in li:
#         for j in i:
#
#             print( j)
# print(prr(li))
#
# a =  [j for i in li  for j in i]
# print(a)

