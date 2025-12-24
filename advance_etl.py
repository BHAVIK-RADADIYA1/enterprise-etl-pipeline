import pandas as pd
import sqlite3
import logging
from datetime import datetime

# --- CONFIGURATION (Good practice to separate config from logic) ---
DB_NAME = "enterprise_warehouse.db"
LOG_FILE = "pipeline_logs.log"
RAW_FILE = "daily_sales_raw.csv"
QUARANTINE_FILE = "quarantine_data.csv"

# --- UPGRADE 1: PROFESSIONAL LOGGING ---
# Real engineers log events to a file so they can debug crashes later.
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def generate_dummy_data():
    """Generates messy data for testing."""
    raw_data = {
        'order_id': [101, 102, 103, 104, 105, 106],
        'customer_name': ['Alice', 'Bob', 'Charlie', 'David', None, 'Frank'], # Null value
        'product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Mouse', 'Laptop'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Accessories', 'Electronics'],
        'price': [1200, 25, -50, 300, 25, 1200], # Negative price (Dirty data)
        'quantity': [1, 2, 1, 2, 5, 1],
        'transaction_date': ['2023-10-01', '2023-10-01', '2023-10-01', '2023-10-02', '2023-10-02', '2023-10-02']
    }
    df = pd.DataFrame(raw_data)
    df.to_csv(RAW_FILE, index=False)
    logging.info("✅ Generated dummy raw data file.")

def extract_data():
    logging.info("Starting Extraction phase...")
    try:
        df = pd.read_csv(RAW_FILE)
        logging.info(f"-> Extracted {len(df)} rows from source.")
        return df
    except Exception as e:
        logging.error(f"Extraction Failed: {e}")
        return None

def transform_and_model_data(df):
    logging.info("Starting Transformation & Modeling phase...")
    
    # --- UPGRADE 2: DATA QUARANTINE ---
    # Identify bad data
    bad_data_mask = (df['customer_name'].isnull()) | (df['price'] <= 0)
    
    # Split into Good and Bad dataframes
    df_quarantine = df[bad_data_mask]
    df_clean = df[~bad_data_mask].copy() # Use .copy() to avoid SettingWithCopyWarning
    
    # Save bad data for review (Don't just delete it!)
    if not df_quarantine.empty:
        df_quarantine.to_csv(QUARANTINE_FILE, index=False)
        logging.warning(f"⚠️ Quarantined {len(df_quarantine)} bad rows to {QUARANTINE_FILE}")
    
    # Add Total Amount
    df_clean['total_amount'] = df_clean['price'] * df_clean['quantity']
    
    # --- UPGRADE 3: STAR SCHEMA MODELING ---
    # We will split data into two tables: Dim_Product and Fact_Sales
    
    # 1. Create Dimension Table (Unique Products)
    dim_product = df_clean[['product', 'category', 'price']].drop_duplicates().reset_index(drop=True)
    # Assign a surrogate key (product_id)
    dim_product['product_id'] = dim_product.index + 1
    
    # 2. Create Fact Table (Transactions linked to Product ID)
    # Join clean data with dimension to get product_id
    fact_sales = df_clean.merge(dim_product, on=['product', 'category', 'price'], how='left')
    
    # Select only relevant columns for Fact Table (Schema Design)
    fact_sales = fact_sales[['order_id', 'transaction_date', 'customer_name', 'product_id', 'quantity', 'total_amount']]
    
    logging.info(f"-> Transformed data. {len(fact_sales)} clean rows ready for loading.")
    return dim_product, fact_sales

def load_data(dim_df, fact_df):
    logging.info("Starting Loading phase...")
    try:
        conn = sqlite3.connect(DB_NAME)
        
        # Load Dimension Table
        dim_df.to_sql('dim_product', conn, if_exists='replace', index=False)
        logging.info("-> Loaded 'dim_product' table.")
        
        # Load Fact Table
        fact_df.to_sql('fact_sales', conn, if_exists='replace', index=False)
        logging.info("-> Loaded 'fact_sales' table.")
        
        conn.close()
        logging.info("✅ ETL Pipeline Completed Successfully.")
    except Exception as e:
        logging.error(f"Loading Failed: {e}")

# --- EXECUTION ---
if __name__ == "__main__":
    generate_dummy_data()
    
    df_raw = extract_data()
    
    if df_raw is not None:
        dim, fact = transform_and_model_data(df_raw)
        load_data(dim, fact)
        
        # Verify the Join works (Simulating an Analytical Query)
        print("\n--- ANALYTICS REPORT: Revenue by Category (Joined Data) ---")
        conn = sqlite3.connect(DB_NAME)
        query = """
        SELECT p.category, SUM(f.total_amount) as total_revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.category
        """
        print(pd.read_sql(query, conn))
        conn.close()