import sys
import os
import csv


def doit():
    csv_file = "sku_lookup_test.csv"
    headers = []
    with open(csv_file) as csvfile:
        propreader = csv.reader(csvfile)
        icnt = 0
        for row in propreader:
            if icnt == 0:
                headers = row
            else:
                print(f'{headers[0]}: {row[0]}, {headers[1]}: {row[1]}, {headers[2]}: {row[2]}')
            icnt += 1  

doit()