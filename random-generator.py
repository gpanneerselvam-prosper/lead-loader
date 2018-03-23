from faker import Faker
fake = Faker()

with open('random-input-data.csv', 'w') as outfile:
    i=1
    while(i <= 10000000):
        name = fake.name()
        ssn = fake.ssn()
        phone = fake.phone_number()
        line = str(i)+","+name+","+ssn+","+phone
        print(line)
        outfile.write(line+"\n")
        i = i + 1
