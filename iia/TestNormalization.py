# import pandas as pd
# from sqlalchemy import create_engine

# # Step 1: Load Source Data from CSV
# # Assuming 'source3.csv' is your actual data file
# source3_df = pd.read_csv('data/source 3/Real Estate Data V21.csv')

# # Step 2: Normalize Data
# # Step 2.1: Create Property_Details Table
# property_details_df = source3_df[['Name', 'Property Title', 'Price', 'Location', 'Total_Area', 'Description']].copy()
# property_details_df.reset_index(inplace=True)  # Reset index to create a unique identifier for each property
# property_details_df.rename(columns={'index': 'property_id', 'Name': 'property_name', 'Property Title': 'property_title', 'Total_Area': 'total_area'}, inplace=True)  # Rename index to property_id and other columns

# # Step 2.2: Create Property_Features Table
# property_features_df = source3_df[['Baths', 'Balcony']].copy()
# property_features_df.reset_index(inplace=True)  # Reset index to create a unique identifier
# property_features_df.rename(columns={'index': 'property_id'}, inplace=True)  # Use property_id as a reference

# # Step 3: Store Normalized Data in PostgreSQL
# # Database connection configuration
# DATABASE_URL = 'postgresql://postgres:admin@localhost:5432/property_db'

# # Create a connection to the PostgreSQL database
# engine = create_engine(DATABASE_URL)

# # Store Property_Details Table in PostgreSQL
# property_details_df.to_sql('property_details', engine, if_exists='replace', index=False)

# # Store Property_Features Table in PostgreSQL
# property_features_df.to_sql('property_features', engine, if_exists='replace', index=False)

# print("Actual source data has been normalized and stored in the PostgreSQL database.")


import pandas as pd
from sqlalchemy import create_engine

# Step 1: Load Source Data from CSV
source3_df = pd.read_csv('data/source 3/Real Estate Data V21.csv')


# Step 2: Normalize Data

# Step 2.1: Create Property_Details Table (Remove derived Price_per_SQFT)
property_details_df = source3_df[['Name', 'Property Title', 'Price', 'Location', 'Total_Area', 'Description']].copy()
property_details_df.reset_index(inplace=True)  # Reset index to create a unique identifier for each property
property_details_df.rename(columns={
    'index': 'property_id', 
    'Name': 'property_name', 
    'Property Title': 'property_title', 
    'Total_Area': 'total_area'
}, inplace=True)

# Step 2.2: Create Property_Features Table
property_features_df = source3_df[['Baths', 'Balcony']].copy()
property_features_df.reset_index(inplace=True)
property_features_df.rename(columns={'index': 'property_id'}, inplace=True)  # Use property_id as a reference

# Step 3: Store Normalized Data in PostgreSQL
# Database connection configuration
DATABASE_URL = 'postgresql://postgres:admin@localhost:5432/property_db'

# Create a connection to the PostgreSQL database
engine = create_engine(DATABASE_URL)

# Store Property_Details Table in PostgreSQL
property_details_df.to_sql('property_details', engine, if_exists='replace', index=False)

# Store Property_Features Table in PostgreSQL
property_features_df.to_sql('property_features', engine, if_exists='replace', index=False)

print("Source 3 data has been normalized and stored in the PostgreSQL database.")
