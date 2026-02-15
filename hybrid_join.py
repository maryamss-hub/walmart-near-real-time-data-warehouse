import pandas as pd
import mysql.connector
from collections import deque, defaultdict
from datetime import datetime
import time
import sys
from typing import Dict, List, Tuple



FILE_PATHS = {
    'customer': 'D:/DW_proj/customer_master_data.csv',
    'product': 'D:/DW_proj/product_master_data.csv',
    'transaction': 'D:/DW_proj/transactional_data/transactional_data.csv'
}

# HYBRIDJOIN Parameters
HASH_TABLE_SIZE = 10000  
DISK_PARTITION_SIZE = 500  
BATCH_INSERT_SIZE = 5000  

# #
# DATABASE CONNECTION
# #

def get_db_connection():
    """Prompt user for database credentials and establish connection."""
    print("\n" + "="*70)
    print("DATABASE CONNECTION SETUP")
    print("="*70)
    
    host = input("Enter MySQL Host (default: localhost): ").strip() or "localhost"
    user = input("Enter MySQL Username (default: root): ").strip() or "root"
    password = input("Enter MySQL Password: ").strip()
    database = input("Enter Database Name (default: walmart_dw): ").strip() or "walmart_dw"
    
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
             password=password,
            database=database,
            autocommit=False,
            use_pure=True,
            connection_timeout=300
        )
        print(f"- Connected to database '{database}' successfully!\n")
        return conn
    except mysql.connector.Error as err:
        print(f"✗ Database connection failed: {err}")
        sys.exit(1)

##
# DATA LOADING & PREPROCESSING
# #

def load_master_data():
    """Load and preprocess master data (customer and product)."""
    print("="*70)
    print("PHASE 1: LOADING MASTER DATA (MD)")
    print("="*70)
    
    # Load Customer Master Data
    print("\n[1/2] Loading Customer Master Data...")
    customer_df = pd.read_csv(FILE_PATHS['customer'])
    customer_df = customer_df[['Customer_ID', 'Gender', 'Age', 'Occupation', 
                                'City_Category', 'Stay_In_Current_City_Years', 'Marital_Status']]
    print(f"  - Loaded {len(customer_df):,} customer records")
    
    # Load Product Master Data
    print("\n[2/2] Loading Product Master Data...")
    product_df = pd.read_csv(FILE_PATHS['product'])
    product_df = product_df[['Product_ID', 'Product_Category', 'price$', 
                              'storeID', 'supplierID', 'storeName', 'supplierName']]
    product_df.rename(columns={'price$': 'Price', 'storeID': 'Store_ID', 
                                'supplierID': 'Supplier_ID'}, inplace=True)
    print(f"  - Loaded {len(product_df):,} product records")
    
    # Create lookup dictionaries for O(1) access
    customer_dict = customer_df.set_index('Customer_ID').to_dict('index')
    product_dict = product_df.set_index('Product_ID').to_dict('index')
    
    print(f"\n- Master Data loaded successfully!")
    return customer_dict, product_dict, product_df

def load_dimension_tables(conn, customer_dict, product_df):
    """Populate dimension tables with master data."""
    print("\n" + "="*70)
    print("PHASE 2: POPULATING DIMENSION TABLES")
    print("="*70)
    
    cursor = conn.cursor()
    
    # DimCustomer
    print("\n[1/5] Populating DimCustomer...")
    customer_data = [
        (int(cid), str(v['Gender']), str(v['Age']), int(v['Occupation']), 
         str(v['City_Category']), int(v['Stay_In_Current_City_Years']), int(v['Marital_Status']))
        for cid, v in customer_dict.items()
    ]
    cursor.executemany("""
        INSERT INTO DimCustomer (Customer_ID, Gender, Age, Occupation, 
                                 City_Category, Stay_In_Current_City_Years, Marital_Status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, customer_data)
    print(f"  - Inserted {len(customer_data):,} records")
    
    # DimStore
    print("\n[2/5] Populating DimStore...")
    stores = product_df[['Store_ID', 'storeName']].drop_duplicates()
    store_data = [(int(row['Store_ID']), str(row['storeName'])) for _, row in stores.iterrows()]
    cursor.executemany("""
        INSERT INTO DimStore (Store_ID, Store_Name) VALUES (%s, %s)
    """, store_data)
    print(f"  - Inserted {len(store_data):,} records")
    
    # DimSupplier
    print("\n[3/5] Populating DimSupplier...")
    suppliers = product_df[['Supplier_ID', 'supplierName']].drop_duplicates()
    supplier_data = [(int(row['Supplier_ID']), str(row['supplierName'])) for _, row in suppliers.iterrows()]
    cursor.executemany("""
        INSERT INTO DimSupplier (Supplier_ID, Supplier_Name) VALUES (%s, %s)
    """, supplier_data)
    print(f"  - Inserted {len(supplier_data):,} records")
    
    # DimProduct
    print("\n[4/5] Populating DimProduct...")
    product_data = [
        (str(row['Product_ID']), str(row['Product_Category']), float(row['Price']), 
         int(row['Store_ID']), int(row['Supplier_ID']))
        for _, row in product_df.iterrows()
    ]
    cursor.executemany("""
        INSERT INTO DimProduct (Product_ID, Product_Category, Price, Store_ID, Supplier_ID)
        VALUES (%s, %s, %s, %s, %s)
    """, product_data)
    print(f"  - Inserted {len(product_data):,} records")
    
    # DimDate (Generate date dimension for 2015-2021)
    print("\n[5/5] Populating DimDate...")
    date_range = pd.date_range(start='2015-01-01', end='2021-12-31', freq='D')
    date_data = []
    
    for date in date_range:
        day_of_week = date.dayofweek
        is_weekend = 1 if day_of_week >= 5 else 0
        month = date.month
        
        if month in [3, 4, 5]:
            season = 'Spring'
        elif month in [6, 7, 8]:
            season = 'Summer'
        elif month in [9, 10, 11]:
            season = 'Fall'
        else:
            season = 'Winter'
        
        half_year = 1 if month <= 6 else 2
        
        date_data.append((
            date.date(), int(date.day), int(date.month), str(date.strftime('%B')),
            int((date.month - 1) // 3 + 1), int(date.year), int(day_of_week),
            str(date.strftime('%A')), int(is_weekend), int(date.isocalendar()[1]),
            str(season), int(half_year)
        ))
    
    cursor.executemany("""
        INSERT INTO DimDate (Full_Date, Day, Month, Month_Name, Quarter, Year, 
                             Day_Of_Week, Day_Name, Is_Weekend, Week_Of_Year, Season, Half_Year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, date_data)
    print(f"   Inserted {len(date_data):,} date records")
    
    conn.commit()
    cursor.close()
    print("\n All dimension tables populated successfully!")

def fetch_dimension_keys(conn):
    print("\n[INFO] Loading dimension keys into memory...")
    cursor = conn.cursor()
    
    # Customer keys
    cursor.execute("SELECT Customer_ID, Customer_Key FROM DimCustomer")
    customer_keys = {int(cid): int(key) for cid, key in cursor.fetchall()}
    
    # Product keys
    cursor.execute("SELECT Product_ID, Product_Key FROM DimProduct")
    product_keys = {str(pid): int(key) for pid, key in cursor.fetchall()}
    
    # Store keys
    cursor.execute("SELECT Store_ID, Store_Key FROM DimStore")
    store_keys = {int(sid): int(key) for sid, key in cursor.fetchall()}
    
    # Supplier keys
    cursor.execute("SELECT Supplier_ID, Supplier_Key FROM DimSupplier")
    supplier_keys = {int(sid): int(key) for sid, key in cursor.fetchall()}
    
    # Date keys
    cursor.execute("SELECT Full_Date, Date_Key FROM DimDate")
    date_keys = {str(date): int(key) for date, key in cursor.fetchall()}
    
    cursor.close()
    print(f"  Loaded surrogate keys for all dimensions")
    return customer_keys, product_keys, store_keys, supplier_keys, date_keys


# HYBRIDJOIN ALGORITHM IMPLEMENTATION

class HybridJoin:
    
    def __init__(self, product_dict: Dict):
        """Initialize HYBRIDJOIN components."""
        self.hash_table = defaultdict(list)  
        self.queue = deque()  
        self.stream_buffer = deque()  
        self.disk_buffer = []  
        self.product_dict = product_dict
        self.w = HASH_TABLE_SIZE  
        self.hash_table_size = HASH_TABLE_SIZE
        self.partition_size = DISK_PARTITION_SIZE
        
        # Statistics
        self.tuples_processed = 0
        self.join_matches = 0
        self.partitions_loaded = 0
    
    def hash_function(self, key: str) -> int:
        return hash(key) % self.hash_table_size
    
    def load_stream_tuples(self, tuples: List[Dict]):
        self.stream_buffer.extend(tuples)
    
    def process_stream_buffer(self):
        loaded = 0
        while self.stream_buffer and loaded < self.w:
            tuple_data = self.stream_buffer.popleft()
            product_id = str(tuple_data['Product_ID'])
            
            hash_idx = self.hash_function(product_id)
            self.hash_table[hash_idx].append(tuple_data)
            
            self.queue.append((product_id, hash_idx))
            
            loaded += 1
            self.tuples_processed += 1
        
        self.w = 0  
    
    def load_disk_partition(self, join_key: str) -> List[Dict]:
       
        self.partitions_loaded += 1
        
        # Lookup in product dictionary
        if join_key in self.product_dict:
            return [self.product_dict[join_key]]
        return []
    
    def probe_and_join(self, enriched_results: List[Dict]):
    
        if not self.queue:
            return
        
        join_key, hash_idx = self.queue.popleft()
        
        self.disk_buffer = self.load_disk_partition(join_key)
        
        if hash_idx in self.hash_table and self.disk_buffer:
            tuples_at_slot = self.hash_table[hash_idx][:]
            
            for stream_tuple in tuples_at_slot:
                if str(stream_tuple['Product_ID']) == join_key:
                    for disk_tuple in self.disk_buffer:
                        enriched = stream_tuple.copy()
                        enriched.update(disk_tuple)
                        enriched_results.append(enriched)
                        
                        self.join_matches += 1
                        self.w += 1  
                    
                    self.hash_table[hash_idx].remove(stream_tuple)
            
            if not self.hash_table[hash_idx]:
                del self.hash_table[hash_idx]
    
    def run(self, stream_data: pd.DataFrame, customer_dict: Dict) -> pd.DataFrame:
        print("\n" + "="*70)
        print("PHASE 3: HYBRIDJOIN ALGORITHM EXECUTION")
        print("="*70)
        
        print(f"\n[CONFIG] Hash Table Size: {self.hash_table_size:,} slots")
        print(f"[CONFIG] Disk Partition Size: {self.partition_size} tuples")
        print(f"[CONFIG] Total Stream Tuples: {len(stream_data):,}")
        
        enriched_results = []
        batch_size = 20000  # Process larger batches for speed
        
        start_time = time.time()
        
        # Convert stream data to list of dicts
        stream_tuples = stream_data.to_dict('records')
        total_tuples = len(stream_tuples)
        
        print(f"\n[START] Processing {total_tuples:,} transactions...")
        
        # Process in batches
        for i in range(0, total_tuples, batch_size):
            batch = stream_tuples[i:i+batch_size]
            self.load_stream_tuples(batch)
            
            # HYBRIDJOIN main loop - Process ALL keys in queue
            while self.stream_buffer or self.queue:
                # Step 2: Load stream tuples into hash table
                if self.stream_buffer:
                    self.process_stream_buffer()
                
                # Step 3 & 4: Probe and join ALL keys in queue
                while self.queue:
                    self.probe_and_join(enriched_results)
                
                # If stream buffer still has data and w > 0, continue
                if not self.stream_buffer:
                    break
            
            # Progress logging
            if (i + batch_size) % 100000 == 0 or i + batch_size >= total_tuples:
                elapsed = time.time() - start_time
                progress = min(i + batch_size, total_tuples)
                rate = progress / elapsed if elapsed > 0 else 0
                print(f"  [{progress:,}/{total_tuples:,}] tuples | "
                      f"{rate:.0f} tuples/sec | Matches: {self.join_matches:,}")
        
        elapsed_time = time.time() - start_time
        
        print(f"\n HYBRIDJOIN completed in {elapsed_time:.2f} seconds")
        print(f"  • Total tuples processed: {self.tuples_processed:,}")
        print(f"  • Successful joins: {self.join_matches:,}")
        print(f"  • Partitions loaded: {self.partitions_loaded:,}")
        print(f"  • Processing rate: {total_tuples/elapsed_time:.0f} tuples/sec")
        
        # Enrich with customer data
        print("\n[INFO] Enriching transactions with customer data...")
        for record in enriched_results:
            cid = int(record['Customer_ID'])
            if cid in customer_dict:
                record.update(customer_dict[cid])
        
        return pd.DataFrame(enriched_results)

# #
# DATA LOADING INTO FACT TABLE
# #

def load_fact_table(conn, enriched_df: pd.DataFrame, dimension_keys: Tuple):
    print("\n" + "="*70)
    print("PHASE 4: LOADING FACT TABLE")
    print("="*70)
    
    customer_keys, product_keys, store_keys, supplier_keys, date_keys = dimension_keys
    
    cursor = conn.cursor()
    fact_data = []
    skipped = 0
    
    print(f"\n[INFO] Preparing {len(enriched_df):,} records for insertion...")
    
    for _, row in enriched_df.iterrows():
        try:
            # Lookup surrogate keys with explicit type conversion
            customer_key = customer_keys.get(int(row['Customer_ID']))
            product_key = product_keys.get(str(row['Product_ID']))
            store_key = store_keys.get(int(row['Store_ID']))
            supplier_key = supplier_keys.get(int(row['Supplier_ID']))
            date_key = date_keys.get(str(row['date']))
            
            # Skip if any key is missing
            if None in [customer_key, product_key, store_key, supplier_key, date_key]:
                skipped += 1
                continue
            
            # Calculate amounts with explicit type conversion
            quantity = int(row['quantity'])
            unit_price = float(row['Price'])
            total_amount = float(quantity * unit_price)
            
            fact_data.append((
                int(row['orderID']), 
                int(customer_key), 
                int(product_key), 
                int(store_key),
                int(supplier_key), 
                int(date_key), 
                int(quantity), 
                float(unit_price), 
                float(total_amount)
            ))
            
        except (KeyError, ValueError, TypeError) as e:
            skipped += 1
            continue
    
    print(f"[INFO] Valid records: {len(fact_data):,} | Skipped: {skipped:,}")
    
    # Batch insert for performance
    print(f"\n[INFO] Inserting into FactSales in batches of {BATCH_INSERT_SIZE:,}...")
    
    insert_query = """
        INSERT INTO FactSales (Order_ID, Customer_Key, Product_Key, Store_Key,
                               Supplier_Key, Date_Key, Quantity, Unit_Price, Total_Amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    start_time = time.time()
    total_inserted = 0
    
    for i in range(0, len(fact_data), BATCH_INSERT_SIZE):
        batch = fact_data[i:i+BATCH_INSERT_SIZE]
        cursor.executemany(insert_query, batch)
        conn.commit()
        total_inserted += len(batch)
        
        if i % 50000 == 0 or i + BATCH_INSERT_SIZE >= len(fact_data):
            elapsed = time.time() - start_time
            rate = total_inserted / elapsed if elapsed > 0 else 0
            print(f"  [{total_inserted:,}/{len(fact_data):,}] inserted | {rate:.0f} records/sec")
    
    elapsed_time = time.time() - start_time
    
    cursor.close()
    print(f"\n- FactSales loaded successfully in {elapsed_time:.2f} seconds!")
    print(f"  • Total records inserted: {total_inserted:,}")
    print(f"  • Insertion rate: {total_inserted/elapsed_time:.0f} records/sec")

# #
# MAIN ETL EXECUTION
# #

def main():
    """Main ETL orchestration function."""
    print("\n" + "="*70)
    print("WALMART DATA WAREHOUSE - HYBRIDJOIN ETL PIPELINE")
    print("="*70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    overall_start = time.time()
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        # PHASE 1: Load Master Data
        customer_dict, product_dict, product_df = load_master_data()
        
        # PHASE 2: Populate Dimension Tables
        load_dimension_tables(conn, customer_dict, product_df)
        dimension_keys = fetch_dimension_keys(conn)
        
        # PHASE 3: Load Transaction Stream
        print("\n" + "="*70)
        print("LOADING TRANSACTION STREAM")
        print("="*70)
        print("\n[INFO] Reading transactional data...")
        
        transaction_df = pd.read_csv(FILE_PATHS['transaction'])
        transaction_df = transaction_df[['orderID', 'Customer_ID', 'Product_ID', 'quantity', 'date']]
        transaction_df = transaction_df.head(1000)

        
        # Convert data types explicitly
        transaction_df['orderID'] = transaction_df['orderID'].astype('int64')
        transaction_df['Customer_ID'] = transaction_df['Customer_ID'].astype('int64')
        transaction_df['Product_ID'] = transaction_df['Product_ID'].astype(str)
        transaction_df['quantity'] = transaction_df['quantity'].astype('int64')
        transaction_df['date'] = transaction_df['date'].astype(str)
        
        print(f"  - Loaded {len(transaction_df):,} transaction records")
        
        # PHASE 3: Execute HYBRIDJOIN
        hybrid_join = HybridJoin(product_dict)
        enriched_df = hybrid_join.run(transaction_df, customer_dict)
        
        # PHASE 4: Load Fact Table
        load_fact_table(conn, enriched_df, dimension_keys)
        
        # Final statistics
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM FactSales")
        final_count = cursor.fetchone()[0]
        cursor.close()
        
        overall_time = time.time() - overall_start
        
        print("\n" + "="*70)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total Execution Time: {overall_time:.2f} seconds ({overall_time/60:.2f} minutes)")
        print(f"Final FactSales Record Count: {final_count:,}")
        print("="*70)
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()
        print("\n[INFO] Database connection closed.")

if __name__ == "__main__":
    main()