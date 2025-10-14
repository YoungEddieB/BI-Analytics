course= "Python programming"
print(course.upper())
print(course.lower())
print(course.title())
print(course.strip()) ###-- like this it only removes spaces from the beginining and end of a string by using l(begining) or r(ending)
print(course.find('x')) ### -- if it returns -1 means that it did not find the index within the string value
print(course.replace('P','y'))
print("pro" in course) ## returns a boolean value meaning that if it contains or does not contain the value in the variable string
print("pro" not in course)