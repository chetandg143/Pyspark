import mysql.connector
import json

mydb = mysql.connector.connect(
       host = 'localhost',
       database = 'pysparkDB',
       user = 'root' ,
    password = 'MYSQL123',
)
mycur = mydb.cursor()
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Medical Incident' ")
# mi = mycur.fetchall()
# list1 = list(mi)
# print(list1)
# # li = list()
# # for l in list1:
# #     li.append(' '.join(list(l)))
# # li = ' '.join(li)
# # li = li.split()
# # print(li)
#
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Alarms' ")
# alm =mycur.fetchall()
# list2=list(alm)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Outside Fire' ")
# of =mycur.fetchall()
# list3 = list(of)
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Citizen Assist / Service Call' ")
# cs =mycur.fetchall()
# list4 = list(cs)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Other' ")
# othr =mycur.fetchall()
# list5 = list(othr)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Odor (Strange / Unknown)' ")
# od =mycur.fetchall()
# list6 = list(od)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Traffic Collision' ")
# Tc =mycur.fetchall()
# list7 = list(Tc)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Vehicle Fire' ")
# vf =mycur.fetchall()
# list8 =list(vf)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Electrical Hazard' ")
# eh =mycur.fetchall()
# list9 = list(eh)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Gas Leak (Natural and LP Gases)' ")
# gas =mycur.fetchall()
# list10 = list(gas)
#
# mycur.execute("select Incident_Number from Task_List where Call_Type ='Structure Fire' ")
# sf =mycur.fetchall()
# list11 = list(sf)
#
# d1 ={}
# d2 = {}
# d3 ={}
# d1['medical incident']=list1
# d1['Alarms'] =list2
# d1['Outside Fire'] =list3
# d1['Citizen Assist'] =list4
# d1['other'] = list5
# d1['odor'] =list6
# d1['Traffic collison'] = list7
# d1['Vehical fire'] = list8
# d1['Electrical Hazard'] =list9
# d1['Gas Leak'] = list10
# d1['Structure Fire'] =list11
# d2["Call Type"]  = d1
# d3['Attributes'] =d2
# print(d3.values())
mycur.execute("select  Incident_Number from Task_List where Call_Type ='Medical Incident' ")
res = mycur.fetchall()
print(res)
print(type(res))


def Convert(lst):
    res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
    print( res_dct.values())



# print(Convert(res))
# print()


mycur.execute("select Incident_Number from Task_List")
r = mycur.fetchall()
print(r)
out = sum(r,())
print(out)

tuple = [(1,2) , (3,4) ,(5,6) ,(45,67) , (9,7)]
out1 = [j for i in tuple  for j in i]
print(out1.sort())






























# with open("Attr.json", "w") as jsonFile:
# jsonFile.write(json.dumps(d3, indent = 4))

