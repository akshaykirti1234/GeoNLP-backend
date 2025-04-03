from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import spacy
import psycopg2
from psycopg2 import pool
import re

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load NLP model
nlp = spacy.load("en_core_web_sm")

# Database connection parameters
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "GeoNLP",
    "user": "postgres",
    "password": "csmpl@123",
}

# Create a connection pool
try:
    connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **DB_CONFIG)
    if connection_pool:
        print("Connection pool created successfully")
except Exception as e:
    print(f"Error creating connection pool: {e}")

# Fetch table columns dynamically
def get_table_columns():
    conn = None
    try:
        conn = connection_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name FROM information_schema.columns WHERE table_name = 'new_plot_data';
            """)
            columns = {row[0].lower() for row in cursor.fetchall()}  
        return columns
    except Exception as e:
        print(f"Error fetching column names: {e}")
        return set()
    finally:
        if conn:
            connection_pool.putconn(conn)  

# Fetch distinct land use classifications once and store them in memory
def load_landuse_classes():
    conn = None
    try:
        conn = connection_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT landuse FROM new_plot_data;
            """)
            return {row[0].lower(): row[0] for row in cursor.fetchall()}  # Lowercase mapping
    except Exception as e:
        print(f"Error fetching land use classes: {e}")
        return {}
    finally:
        if conn:
            connection_pool.putconn(conn)  # Ensure the connection is returned

# Load land use classifications once at startup
landuse_classes = load_landuse_classes()

# Request Model
class QueryRequest(BaseModel):
    query: str

# Function to clean user query
def sentence_cleaner(user_input: str):
    doc = nlp(user_input)
    return [token.text for token in doc]

# Function to match user query with land use classifications
def get_matched_landuse(cleaned_words):
    sentence = " ".join(cleaned_words).lower()
    return [original for lower, original in landuse_classes.items() if lower in sentence]

@app.post("/process_query")
async def process_query(request: QueryRequest):
    cleaned_words = sentence_cleaner(request.query)
    table_columns = get_table_columns()  # Fetch dynamically

    # Check if any word matches a column name
    matched_columns = [word for word in cleaned_words if word.lower() in table_columns]

    # Extract land use classifications
    matched_landuse = get_matched_landuse(cleaned_words)

    # Detect numerical conditions (Fixed Regex)
    area_condition = None
    match = re.search(r"(<=|>=|=|less than|greater than|more than|below|above|at least|no more than|under)\s*(\d+)", 
                      request.query, re.IGNORECASE)

    if match:
        operator_map = {
            "less than": "<",
            "below": "<",
            "under": "<",
            "greater than": ">",
            "more than": ">",
            "above": ">",
            "at least": ">=",
            "no more than": "<=",
            "<=": "<=",
            ">=": ">=",
            "=": "="
        }
        operator = operator_map.get(match.group(1).lower(), "=")
        value = match.group(2)
        area_condition = f"area_sqm {operator} {value}"

    # Generate CQL filter with proper parentheses
    landuse_filter = f"({' OR '.join([f'landuse = \'{lu}\'' for lu in matched_landuse])})" if matched_landuse else None

    # Combine filters
    cql_filter_parts = []
    if landuse_filter:
        cql_filter_parts.append(landuse_filter)
    if area_condition:
        cql_filter_parts.append(area_condition)

    cql_filter = " AND ".join(cql_filter_parts) if cql_filter_parts else None

    return {
        "cleanedWords": cleaned_words,
        "matchedColumns": matched_columns,
        "matchedLanduse": matched_landuse,
        "cqlFilter": cql_filter,
    }
