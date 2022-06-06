"""This is init module."""

from flask import Flask

# Place where app is defined
app = Flask(__name__)

from app import users_data
from app import claim_data
from app import claim_sql_data
