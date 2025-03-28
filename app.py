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
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "GeoNLP"
DB_USER = "postgres"
DB_PASSWORD = "csmpl@123"

# Create a connection pool
try:
    connection_pool = psycopg2.pool.SimpleConnectionPool(
        1, 20,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    if connection_pool:
        print("Connection pool created successfully")
except Exception as e:
    print(f"Error creating connection pool: {e}")

# Fetch column names dynamically
def get_table_columns():
    try:
        conn = connection_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name FROM information_schema.columns WHERE table_name = 'new_plot_data';
        """)
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection_pool.putconn(conn)
        return columns
    except Exception as e:
        print(f"Error fetching column names: {e}")
        return []

# Get columns from the table
table_columns = get_table_columns()

# Request Model
class QueryRequest(BaseModel):
    query: str

def sentence_cleaner(user_input: str) -> list:
    doc = nlp(user_input)
    
    for ent in doc.ents:
        print(f"{ent.text} --> {ent.label_}")

    # Removing stopwords and punctuation
    cleaned_words = [token.text for token in doc if not token.is_stop and not token.is_punct]
    return cleaned_words

@app.post("/process_query")
async def process_query(request: QueryRequest):
    cleaned_words = sentence_cleaner(request.query)
    
    # Check if any word matches a column name
    matched_columns = [word for word in cleaned_words if word in table_columns]

    return {
        "cleanedWords": cleaned_words,
        "matchedColumns": matched_columns
    }
