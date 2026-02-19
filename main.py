import pandas as pd
import re
import io
import os
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from supabase import create_client, Client
import math 

# --- CONFIGURATION ---

## Configuration for Google Drive
RAW_FOLDER_ID = raw folder key
ARCHIVE_FOLDER_ID = archive folder key

## Configuration for Supabase
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

# Initialize Supabase
supabase: Client = create_client(url, key)

# --- FUNCTIONS ---

def load_to_supabase(df, table_name):

    try:
        # 1. Sanitize Column Names
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('/', '_').str.replace('-', '_')
        
        # 2. Convert to List of Dictionaries (Raw Data)
        raw_records = df.to_dict(orient='records')
        
        # 3. Manually clean every value
        clean_records = []
        
        for record in raw_records:
            new_record = {}
            for key, value in record.items():
                # Check if it is a float AND if it is infinite or NaN
                if isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
                    new_record[key] = None
                else:
                    new_record[key] = value
            clean_records.append(new_record)

        # 4. Insert into Supabase
        print(f"Uploading {len(clean_records)} rows to Supabase...")
        response = supabase.table(table_name).insert(clean_records).execute()
        
        print(f"✅ Success! Inserted {len(clean_records)} rows into {table_name}.")
        return response

    except Exception as e:
        print(f"❌ Error uploading to Supabase: {e}")
        # This will print the exact record causing the issues with infinite/NaN values
        # print("First record:", clean_records[0]) 
        raise e

def raw_report_transform(file_content, filename):
    # 1. Load Data
    try:
        df = pd.read_excel(file_content, sheet_name='Paid order list')
    except Exception as e:
        print(f"Skipping {filename}: Could not read Excel. Error: {e}")
        return None

    # 2. Clean Column Names
    df.columns = df.columns.str.strip()

    # 3. Explode the Product List
    df['product_list'] = df['Products'].astype(str).str.split(',')
    df_exploded = df.explode('product_list')
    df_exploded = df_exploded[df_exploded['product_list'] != '']

    # 4. Extract Size
    size_pattern = r'(Solo|Duo|Medio|Familia)'
    df_exploded['Size'] = df_exploded['product_list'].str.extract(size_pattern, flags=re.I, expand=False).str.title()

    # 5. Extract Variation (Hot/Cold)
    hot_cold_pattern = r'(Hot|Cold)'
    df_exploded['Variation'] = df_exploded['product_list'].str.extract(hot_cold_pattern, flags=re.I, expand=False).str.title()

    # 6. Extract Flavor (Fries/Lemonade)
    target_ff_p = r'(Fries|Lemonade)'
    is_target1 = df_exploded['product_list'].str.contains(target_ff_p, case=False, na=False)
    fries_flavor_pattern = r'(Cheese|BBQ|Sour Cream|Plain|Mango)'
    df_exploded.loc[is_target1, 'Flavor'] = df_exploded.loc[is_target1, 'product_list'].str.extract(fries_flavor_pattern, flags=re.I, expand=False).str.title()

    # 7. Extract Sugar Level
    sugar_pattern = r'(Sugar 20%|Sugar 50%|Sugar 75%|Sugar 100%)'
    df_exploded['Sugar Level'] = df_exploded['product_list'].str.extract(sugar_pattern, flags=re.I, expand=False).str.title()

    # 8. Extract Spice Level
    spicy_pattern = r'(Mild \(1/4\)|Regular \(2/4\)|Spicy \(3/4\))'
    df_exploded['Spice Level'] = df_exploded['product_list'].str.extract(spicy_pattern, flags=re.I, expand=False).str.title()

    # 9. Extract Quantity
    df_exploded['Quantity'] = df_exploded['product_list'].str.extract(r'x\s*(\d+)').astype(float).fillna(1)

    # 10. Complex Extraction - this code block will edit the names for the target items (Croissant, Croffle, Cookies, Cookie)
    # The code will extract the item name and the flavor, put it in two different columns, then concatenate in Clean_Item, and drop the temporary columns


    target_categories = ['Croissant', 'Croffle', 'Cookies', 'Cookie']
    target_mask_pattern = r'(' + '|'.join(target_categories) + r')'

    flavors_list = [
        'Chip and Chunk Walnut', 'Nutella Pecan Cookie', 'Red Velvet Cookie', 
        'Smores Cookie', 'Almond Nutella', 'Biscoff Cookie', 'Strawberry Cream', 
        'Spam and Egg', 'Chip and Chunk', 'Biscoff', 'Caramel', 'Chocolate', 
        'Matcha', 'Oreo', 'Plain', 'Smores', 'Red Velvet', 'Dubai'
    ]

    flavor_pattern = r'(' + '|'.join(map(re.escape, flavors_list)) + r')'
    is_target = df_exploded['product_list'].str.contains(target_mask_pattern, case=False, na=False)

    df_exploded.loc[is_target, 'Temp_Flavor'] = df_exploded.loc[is_target, 'product_list'].str.extract(flavor_pattern, flags=re.I, expand=False)
    df_exploded.loc[is_target, 'Temp_Flavor'] = df_exploded.loc[is_target, 'Temp_Flavor'].str.replace(r'\s*Cookie', '', regex=True, flags=re.I).str.strip()

    df_exploded.loc[is_target, 'Category_Name'] = df_exploded.loc[is_target, 'product_list'].str.extract(target_mask_pattern, flags=re.I, expand=False).str.title()
    df_exploded.loc[is_target & (df_exploded['Category_Name'] == 'Cookie'), 'Category_Name'] = 'Cookies'

    df_exploded.loc[is_target, 'Clean_Item'] = (
        df_exploded.loc[is_target, 'Category_Name'] + " - " + df_exploded.loc[is_target, 'Temp_Flavor']
    )

    # 11. Handle Non-Targets
    df_exploded.loc[~is_target, 'Clean_Item'] = df_exploded.loc[~is_target, 'product_list'].str.replace(r'x\s*\d+', '', regex=True)
    df_exploded.loc[~is_target, 'Clean_Item'] = df_exploded.loc[~is_target, 'Clean_Item'].str.replace(r'\s*\(.*\)', '', regex=True).str.strip()

    # 12. Manual Corrections - (Made a dictionary for future corrections)
    product_corrections = {
        'Fruit Lemonade w/Popping Pearls': 'Fruit Lemonade w/ Popping Pearls'
    }

    df_exploded['Clean_Item'] = df_exploded['Clean_Item'].replace(product_corrections)

    # 13. Map all the products to their respective sub-categories 

    product_to_sub_category = {
        # Add-Ons
        "Candle - Big": "Add-Ons (Cake)",
        "Candle": "Add-Ons (Cake)",
        "Candle - Small": "Add-Ons (Cake)",
        "Candle + Topper Set": "Add-Ons (Cake)",
        "Candle + Topper Set - Big Candle + Big Bday T.": "Add-Ons (Cake)",
        "Candle + Topper Set - Small Candle+ Small Bday T.": "Add-Ons (Cake)",
        "Extra Aioli Dip": "Food Add-Ons",
        "Extra Cheese Sauce": "Food Add-Ons",
        "Extra Egg": "Food Add-Ons",
        "Extra Rice": "Food Add-Ons",

        # Food
        "Bacon with Rice and Egg": "All Day Breakfast",
        "Corned Beef with Rice and Egg": "All Day Breakfast",
        "Spam with Rice and Egg": "All Day Breakfast",
        "Sunrise Breakfast Plate": "All Day Breakfast",
        "Cheese Sticks": "Appetizers",
        "Chicken Fingers": "Appetizers",
        "French Fries": "Appetizers",
        "French Fries Overload": "Appetizers",
        "French Fries Platter": "Appetizers",
        "Mojos": "Appetizers",
        "Nachos": "Appetizers",
        "Spring Rolls": "Appetizers",
        "Carbonara": "Pasta",
        "Chicken Aglio Olio": "Pasta",
        "Chicken Pesto": "Pasta",
        "Creamy Lasagna": "Pasta",
        "Shrimp Aglio Olio": "Pasta",
        "Spaghetti Meatballs": "Pasta",
        "Spicy Tuna Pasta": "Pasta",
        "Chicken Salpicao": "Rice Meals",
        "Pad Kra Pao": "Rice Meals",
        "Spicy Pork Stir Fry": "Rice Meals",
        "Bacon and Egg Sandwich": "Sandwiches",
        "Clubhouse": "Sandwiches",
        "Crispy Chicken Sandwich": "Sandwiches",
        "Spam and Egg Sandwich": "Sandwiches",

        # Beverages
        "Coffee Jelly Blended": "Blended Coffee",
        "Hazelnut Blended": "Blended Coffee",
        "Java Chip Blended": "Blended Coffee",
        "Mocha Blended": "Blended Coffee",
        "White Mocha Blended": "Blended Coffee",
        "Biscoff Blended": "Blended Cream",
        "Biscoff Cream": "Blended Cream",
        "Caramel Blended": "Blended Cream",
        "Caramel Cream": "Blended Cream",
        "Chocolate Chip Cream": "Blended Cream",
        "Chocolate Cream": "Blended Cream",
        "Matcha Cream": "Blended Cream",
        "Nutella Blended": "Blended Cream",
        "Nutella Cream": "Blended Cream",
        "Oreo Cream": "Blended Cream",
        "Strawberry Cream": "Blended Cream",
        "Vanilla Cream": "Blended Cream",
        "White Chocolate Cream": "Blended Cream",
        "Amantes": "Coffee Based",
        "Americano": "Coffee Based",
        "Biscoff Latte": "Coffee Based",
        "Cappuccino": "Coffee Based",
        "Caramel Macchiato": "Coffee Based",
        "Flavored Latte": "Coffee Based",
        "Latte": "Coffee Based",
        "Matcha Espresso": "Coffee Based",
        "Mocha": "Coffee Based",
        "Nutella Latte": "Coffee Based",
        "Salted Caramel Latte": "Coffee Based",
        "Spanish Latte": "Coffee Based",
        "Vietnamese": "Coffee Based",
        "White Mocha": "Coffee Based",
        "White Mocha Hazelnut": "Coffee Based",
        "Blueberry Yakult": "Fruit Based",
        "Fruit Lemonade w/ Popping Pearls": "Fruit Based",
        "Green Apple Fruit Tea": "Fruit Based",
        "Mango Yakult": "Fruit Based",
        "Passion Fruit": "Fruit Based",
        "Passion Fruit Cooler": "Fruit Based",
        "Strawberry Yakult": "Fruit Based",
        "Chamomile": "Hot Tea",
        "Peppermint": "Hot Tea",
        "Biscoff Milk": "Milk Based",
        "Blueberry Milk": "Milk Based",
        "Chocolate": "Milk Based",
        "Matcha": "Milk Based",
        "Nutella Milk": "Milk Based",
        "Oreo Matcha": "Milk Based",
        "Oreo Milk": "Milk Based",
        "Strawberry Matcha": "Milk Based",
        "Strawberry Milk": "Milk Based",
        "White Chocolate": "Milk Based",
        "White Chocolate Chip": "Pastries", 

        # Desserts
        "Biscoff Cheesecake": "Cheesecakes",
        "Blueberry Cheesecake": "Cheesecakes",
        "Mango Cheesecake": "Cheesecakes",
        "New York Cheesecake": "Cheesecakes",
        "Nutella Cheesecake": "Cheesecakes",
        "Oreo Cheesecake": "Cheesecakes",
        "Strawberry Cheesecake": "Cheesecakes",
        "Ube Cheesecake": "Cheesecakes",
        "Biscoff tiramisu": "Cheesecakes",
        "Choco Almond": "Moist Cakes",
        "Choco Caramel": "Moist Cakes",
        "Garnet Velvet": "Moist Cakes",
        "Pecan Walnut Carrot": "Moist Cakes",
        "Signature Chocolate": "Moist Cakes",
        "Banana Bread": "Pastries",
	"Crookie": "Pastries',
        "Cookies - Biscoff": "Pastries",
        "Cookies - Chip and Chunk": "Pastries",
        "Cookies - Chip and Chunk Walnut": "Pastries",
        "Cookies - Nutella Pecan": "Pastries",
        "Cookies - Red Velvet": "Pastries",
        "Cookies - Smores": "Pastries",
        "Cookies - Dubai": "Pastries",
        "Crinkles": "Pastries",
        "Croffle - Almond Nutella": "Pastries",
        "Croffle - Biscoff": "Pastries",
        "Croffle - Caramel": "Pastries",
        "Croffle - Chocolate": "Pastries",
        "Croffle - Matcha": "Pastries",
        "Croffle - Oreo": "Pastries",
        "Croffle - Plain": "Pastries",
        "Croffle - Smores": "Pastries",
        "Croffle - Strawberry Cream": "Pastries",
        "Croissant - Almond Nutella": "Pastries",
        "Croissant - Biscoff": "Pastries",
        "Croissant - Caramel": "Pastries",
        "Croissant - Chocolate": "Pastries",
        "Croissant - Oreo": "Pastries",
        "Croissant - Plain": "Pastries",
        "Croissant - Spam and Egg": "Pastries",

        # Others
        "Bottled Water": "Others",
        "Coke in Can": "Others"
    }


    # Map all the sub-categories to their respective categories
    
    sub_category_to_category = {
        "Add-Ons (Cake)": "Add-Ons",
        "Food Add-Ons": "Add-Ons",
        "All Day Breakfast": "Food",
        "Appetizers": "Food",
        "Pasta": "Food",
        "Rice Meals": "Food",
        "Sandwiches": "Food",
        "Blended Coffee": "Beverages",
        "Blended Cream": "Beverages",
        "Coffee Based": "Beverages",
        "Fruit Based": "Beverages",
        "Hot Tea": "Beverages",
        "Milk Based": "Beverages",
        "Pastries": "Desserts",
        "Cheesecakes": "Desserts",
        "Moist Cakes": "Desserts",
        "Others": "Others"
    }

    df_exploded['Sub-Category'] = df_exploded['Clean_Item'].map(product_to_sub_category)
    df_exploded['Category'] = df_exploded['Sub-Category'].map(sub_category_to_category)
    
    # 15. Payment Type Function

    def get_payment_type(row):
        val_cash = str(row.get('Cash', 0)) # Safer .get
        
        if val_cash == '0.00' or val_cash == '0':
            return 'Free/Voucher/Discounted'
        elif val_cash != '-':
            return 'Cash'
        elif str(row.get('Gcash', '-')) != '-':
            return 'Gcash'
        else:
            return 'Credit / Debit'

    df_exploded['Payment Type'] = df_exploded.apply(get_payment_type, axis=1)

    # 16. Final Cleanup
    cols_to_use = [
        'Order ID', 'Clean_Item', 'Sub-Category', 'Category', 'Flavor', 
        'Variation', 'Size', 'Quantity', 'Spice Level', 'Sugar Level', 
        'Product amount', 'Received amount', 'Payment time', 'Payment Type', 'Type/Channel'
    ]
    
    # Ensure columns exist before selecting
    existing_cols = [c for c in cols_to_use if c in df_exploded.columns]
    df_exploded = df_exploded[existing_cols]
    
    df_exploded = df_exploded[df_exploded['Clean_Item'].astype(str) != 'nan']
    df_exploded['Clean_Item'] = df_exploded['Clean_Item'].str.title()


    # Make sure all numeric columns are fit to JSON

    columns_to_numeric = ['Received amount', 'Product amount']

    for col in columns_to_numeric:
        df_exploded[col] = df_exploded[col].astype(str).str.replace(',', '')
        df_exploded[col] = pd.to_numeric(df_exploded[col], errors='coerce')

    
    df_exploded.rename(columns={
        'Clean_Item': 'Items', 
        'Type/Channel': 'Order Type', 
        'Product amount': 'Total Order Amount'
    }, inplace=True)

    df_exploded = df_exploded.iloc[:-1] # Remove footer row

    return df_exploded


def main():
    print("--- STARTING AUTOMATION ---")
    
    # 1. Connect to Drive
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = service_account.Credentials.from_service_account_file('service_account.json', scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)

    # 2. Check for New Files
    query = f"'{RAW_FOLDER_ID}' in parents and trashed=false"
    results = drive_service.files().list(q=query).execute()
    files = results.get('files', [])

    if not files:
        print("No new reports found. Exiting.")
        return

    # 3. Process Each New File
    processed_dfs = []
    
    for file in files:
        print(f"Processing: {file['name']}")
        
        request = drive_service.files().get_media(fileId=file['id'])
        file_content = io.BytesIO(request.execute())
        
        clean_df = raw_report_transform(file_content, file['name'])
        
        if clean_df is not None and not clean_df.empty:
            processed_dfs.append(clean_df)
            
            # Archive file
            drive_service.files().update(
                fileId=file['id'],
                addParents=ARCHIVE_FOLDER_ID,
                removeParents=RAW_FOLDER_ID
            ).execute()
            print(f"   -> Archived {file['name']}")

    # 4. Upload to Supabase (The Fix)
    if processed_dfs:
        print("Concatenating all files...")
        master_df = pd.concat(processed_dfs, ignore_index=True)
        
        # Call the function (now defined correctly outside main)
        load_to_supabase(master_df, "fact_sales2026")
    else:
        print("No valid data processed.")

if __name__ == "__main__":
    main()
