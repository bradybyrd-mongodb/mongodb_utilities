"""
    Get articles
    curl https://cdn.openai.com/API/examples/data/vector_database_wikipedia_articles_embedded.zip --output vector_database_wikipedia_articles_embedded.zip

"""
import csv
import os
# header: id,url,title,text,title_vector,content_vector,vector_id
base_path = "/Users/brady.byrd/data/vector"
fpath = "vector_database_wikipedia_articles_embedded_small.zip"
data = {}
headers = []
with open(f'{base_path}/{fpath}') as fil:
    data = csv.reader(fil, delimiter=',', quotechar='"')
    cnt = 0
    for row in data:
        if cnt == 0:
            headers = row
            print(f'Headers: {headers}')
        process_row(headers,row)
        cnt += 1
        print(f'')
