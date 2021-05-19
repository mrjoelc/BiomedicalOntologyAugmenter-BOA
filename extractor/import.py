import csv

def extract(keyword):
    list_match=[]
    csvfile = csv.reader(open('assets/OBIB.csv'))
    for row in csvfile:
        if keyword in row[1]:
            list_match.append(row[1])
    return list_match

print(extract('steroid'))