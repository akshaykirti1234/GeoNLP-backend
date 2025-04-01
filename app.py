from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import spacy
import psycopg2
from psycopg2 import pool

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
    try:
        conn = connection_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name FROM information_schema.columns WHERE table_name = 'new_plot_data';
            """)
            columns = {row[0].lower() for row in cursor.fetchall()}  # Store in a set for quick lookup
        connection_pool.putconn(conn)
        return columns
    except Exception as e:
        print(f"Error fetching column names: {e}")
        return set()

# Fetch distinct land use classifications only when needed
def get_landuse_classes():
    try:
        conn = connection_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT landuse FROM new_plot_data;
            """)
            landuse_classes = {row[0].lower(): row[0] for row in cursor.fetchall()}
        connection_pool.putconn(conn)
        return landuse_classes
    except Exception as e:
        print(f"Error fetching land use classes: {e}")
        return {}

# Request Model
class QueryRequest(BaseModel):
    query: str

# Function to clean user query
def sentence_cleaner(user_input: str):
    doc = nlp(user_input)
    return [token.text for token in doc if not token.is_stop and not token.is_punct]

# Function to match user query with land use classifications
def get_matched_landuse(cleaned_words):
    landuse_classes = get_landuse_classes()  # Fetch only when required
    sentence = " ".join(cleaned_words).lower()
    return [original for lower, original in landuse_classes.items() if lower in sentence]

@app.post("/process_query")
async def process_query(request: QueryRequest):
    cleaned_words = sentence_cleaner(request.query)
    table_columns = get_table_columns()  # Fetch dynamically

    # Check if "landuse" is mentioned in the query
    matched_columns = [word for word in cleaned_words if word.lower() in table_columns]
    matched_landuse = get_matched_landuse(cleaned_words) if "landuse" in matched_columns else []

    # Generate CQL filter
    cql_filter = " OR ".join([f"landuse = '{lu}'" for lu in matched_landuse]) if matched_landuse else None

    return {
        "cleanedWords": cleaned_words,
        "matchedColumns": matched_columns,
        "matchedLanduse": matched_landuse,
        "cqlFilter": cql_filter,
    }
