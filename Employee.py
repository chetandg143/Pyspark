
class Employee:
    emp_count =0
    print("Name :", "salary :")

    def __init__(self, name ,salary):
        self.name = name
        self.salary= salary
        Employee.emp_count+=1

    def displaycount(self):
        print("total employees %d :" ,Employee.emp_count)

    def employeedisplay(self):

        print(self.name, " " ,self.salary)
e1 =Employee("abc",25000)
e2 =Employee("xyz" ,15000)
e3 =Employee("king",20000)
e4 =Employee("chetan",00000)

e1.employeedisplay()
e2.employeedisplay()
e3.employeedisplay()
e4.employeedisplay()

e1.displaycount()
