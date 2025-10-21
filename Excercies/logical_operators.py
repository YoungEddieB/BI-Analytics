#Logical operators
#and
#or
#not

#and

high_income = True
good_credit = True

if high_income and good_credit:
    print("You qualify for a Loan")
else:
    print("You do not qualify for a Loan")


#or

high_income = True
good_credit = True

if high_income or good_credit:
    print("You qualify for a Loan")
else:
    print("You do not qualify for a Loan")

#not

high_income = True
good_credit = True
student = False

if not student:
    print("You qualify for a Loan")
else:
    print("You do not qualify for a Loan")

#all together

high_income = True
good_credit = True
student = False

if high_income or good_credit and not student:
    print("You qualify for a Loan")
else:
    print("You do not qualify for a Loan")