import os
import sys
base_dir = os.path.dirname(os.path.abspath(__file__))
# apppend parent folder to path
sys.path.append(os.path.dirname(base_dir))
from bbutil import Util
import requests
from io import BytesIO
from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams
from bs4 import BeautifulSoup
import csv
from pymongo import MongoClient
import nltk
nltk.download("punkt")  # Download the necessary data for tokenization

def parse_pdf(url):
    response = requests.get(url)
    response.raise_for_status()

    pdf_content = response.content

    pdf_file = BytesIO(pdf_content)

    text_file = BytesIO()

    # Extract the text from the PDF and store it in the file-like object
    extract_text_to_fp(pdf_file, text_file, laparams=LAParams())

    text_file.seek(0)

    extracted_text = text_file.read().decode("utf-8")

    # Split the extracted text into separate paragraphs
    paragraphs = extracted_text.split("\n\n")

    # Tokenize sentences from each paragraph
    docs = []
    for paragraph in paragraphs:
        sentences = []
        paragraph_sentences = nltk.sent_tokenize(paragraph)
        sentences.extend(paragraph_sentences)
        docs.append({"raw": paragraph, "sentences": sentences})

    return docs

def parse_html(url):
    response = requests.get(url)
    response.raise_for_status()

    html_content = response.content

    soup = BeautifulSoup(html_content, "html.parser")

    # Find all paragraphs in the HTML document
    paragraphs = [p.get_text() for p in soup.find_all("p")]

    # Tokenize sentences from each paragraph
    docs = []
    for paragraph in paragraphs:
        sentences = []
        paragraph_sentences = nltk.sent_tokenize(paragraph)
        sentences.extend(paragraph_sentences)
        docs.append({"raw": paragraph, "sentences": sentences})


    return docs

# ---------------------------------------------------------- #
#               MAIN
# ---------------------------------------------------------- #

csv_file_path = "aetna_plan_docs.csv"
print("Opening csv file")
with open(csv_file_path, "r") as file:
    reader = csv.reader(file)
    header = next(reader)
    print(f'Got header: {header}')
    client = MongoClient('mongodb+srv://main_admin:<secret>@hackathon.ughh2.mongodb.net')
    db = client['vector_search_demo']
    collection = db['mpsf_plan_docs']

    # Loop through each row in the CSV file
    for row in reader:
        row_dict = {header[i]: row[i] for i in range(len(header))}
        url = row_dict['Content Location']
        if url.endswith(".pdf"):
            parsed_par = parse_pdf(url)
            row_dict["paragraphs"] = parsed_par
        elif url.endswith((".html", ".htm")):
            parsed_par = parse_html(url)
            row_dict["paragraphs"] = parsed_par
        else:
            raise ValueError("Unsupported document type")

        collection.insert_one(row_dict)
    
    client.close()









