-- Dimension Tables

-- Location Dimension
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    locality_name VARCHAR(100) NOT NULL,
    pin_code VARCHAR(6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Property Type Dimension
CREATE TABLE dim_property_type (
    property_type_key SERIAL PRIMARY KEY,
    category VARCHAR(50) NOT NULL, -- Residential, Commercial
    sub_category VARCHAR(50) NOT NULL, -- Apartment, Villa
    configuration VARCHAR(50), -- 1BHK, 2BHK, 3BHK, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Builder Dimension
CREATE TABLE dim_builder (
    builder_key SERIAL PRIMARY KEY,
    builder_name VARCHAR(200) NOT NULL,
    rera_id VARCHAR(50), -- RERA registration number
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Tables

-- Property Master Fact
CREATE TABLE fact_property_master (
    property_key SERIAL PRIMARY KEY,
    location_key INTEGER REFERENCES dim_location(location_key),
    property_type_key INTEGER REFERENCES dim_property_type(property_type_key),
    builder_key INTEGER REFERENCES dim_builder(builder_key),
    property_name VARCHAR(200),
    total_area_sqft DECIMAL(10,2),
    price DECIMAL(15,2),
    rera_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);