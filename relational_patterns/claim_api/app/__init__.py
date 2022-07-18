"""This is init module."""

from flask import Flask
from flask_cors import CORS

# Place where app is defined
app = Flask(__name__)
CORS(app)
from app import users_data
from app import claim_data
from app import claim_sql_data
