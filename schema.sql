-- Dimension Tables

-- Property Features Dimension
CREATE TABLE dim_property_features (
    feature_key SERIAL PRIMARY KEY,
    feature_category VARCHAR(50),
    feature_type VARCHAR(50),
    feature_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Property Age Dimension
CREATE TABLE dim_property_age (
    age_key SERIAL PRIMARY KEY,
    age_range VARCHAR(50),
    age_description VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Property Status Dimension
CREATE TABLE dim_property_status (
    status_key SERIAL PRIMARY KEY,
    ownership_type VARCHAR(50),
    furnishing_status VARCHAR(50),
    availability_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Time Dimension
CREATE TABLE dim_time (
    time_key SERIAL PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Fact Tables

-- Property Listing Fact Table
CREATE TABLE fact_property_listings (
    listing_key SERIAL PRIMARY KEY,
    property_key INTEGER REFERENCES fact_property_master(property_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    feature_key INTEGER REFERENCES dim_property_features(feature_key),
    age_key INTEGER REFERENCES dim_property_age(age_key),
    status_key INTEGER REFERENCES dim_property_status(status_key),
    price_per_sqft DECIMAL(10,2),
    maintenance_cost DECIMAL(10,2),
    total_floors INTEGER,
    floor_number INTEGER,
    facing_direction VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);