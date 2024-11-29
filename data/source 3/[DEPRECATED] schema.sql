CREATE TABLE features (
    FeatureID SERIAL PRIMARY KEY,
    Baths INT,
    Balcony BOOLEAN,
    PropertyID INT,
    FOREIGN KEY (PropertyID) REFERENCES properties(PropertyID)
);

CREATE TABLE location (
    LocationID SERIAL PRIMARY KEY,
    Location VARCHAR(255)
);

CREATE TABLE pricing (
    PriceID SERIAL PRIMARY KEY,
    Price NUMERIC,
    Price_per_SQFT NUMERIC,
    PropertyID INT,
    FOREIGN KEY (PropertyID) REFERENCES properties(PropertyID)
);

CREATE TABLE properties (
    PropertyID SERIAL PRIMARY KEY,
    Name VARCHAR(255),
    Title VARCHAR(255),
    Description TEXT,
    LocationID INT,
    Total_Area NUMERIC,
    FOREIGN KEY (LocationID) REFERENCES location(LocationID)
);