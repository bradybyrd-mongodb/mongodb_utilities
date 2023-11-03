#  Following: https://cookbook.openai.com/examples/question_answering_using_embeddings
# imports
import ast  # for converting embeddings saved as strings back to arrays
import openai  # for calling the OpenAI API
import pandas as pd  # for storing text and embeddings data
import tiktoken  # for counting tokens
from scipy import spatial  # for calculating vector similarities for search


# models
EMBEDDING_MODEL = "text-embedding-ada-002"
GPT_MODEL = "gpt-3.5-turbo"




def get_embedding(text, model="text-embedding-ada-002"):
   text = text.replace("\n", " ")
   return openai.Embedding.create(input = [text], model=model)['data'][0]['embedding']

df['ada_embedding'] = df.combined.apply(lambda x: get_embedding(x, model='text-embedding-ada-002'))
df.to_csv('output/embedded_1k_reviews.csv', index=False)



#  GPT Examples
# an example question about the 2022 Olympics
query = 'Which athletes won the gold medal in curling at the 2022 Winter Olympics?'

response = openai.ChatCompletion.create(
    messages=[
        {'role': 'system', 'content': 'You answer questions about the 2022 Winter Olympics.'},
        {'role': 'user', 'content': query},
    ],
    model=GPT_MODEL,
    temperature=0,
)

print(response['choices'][0]['message']['content'])

#  Embeddings Data Sets
embeddings_url = 'https://cdn.openai.com/API/examples/data/vector_database_wikipedia_articles_embedded.zip'

# The file is ~700 MB so this will take some time
wget.download(embeddings_url)

#  On the Mac:
#  667Mb
curl https://cdn.openai.com/API/examples/data/vector_database_wikipedia_articles_embedded.zip --output vector_database_wikipedia_articles_embedded.zip

in Python:

import csv
import os
# header: id,url,title,text,title_vector,content_vector,vector_id
base_path = "/Users/brady.byrd/data/vector"
fpath = "vector_database_wikipedia_articles_embedded_small.zip"
data = {}
with open(f'{base_path}/{fpath}') as fil:
    data = csv.reader(fil, delimiter=',', quotechar='"')
    cnt = 0
    for row in data:
