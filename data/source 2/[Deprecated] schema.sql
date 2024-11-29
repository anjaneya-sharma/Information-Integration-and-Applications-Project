CREATE TABLE cities (
    city VARCHAR(255),
    city_id SERIAL PRIMARY KEY
);

CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    Location VARCHAR(255),
    city_id INT,
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

CREATE TABLE properties (
    property_id SERIAL PRIMARY KEY,
    Property_Name VARCHAR(255),
    Price NUMERIC,
    Total_Area_SQFT NUMERIC,
    Price_per_SQFT NUMERIC,
    property_type_id INT,
    location_id INT,
    room_config_id INT,
    Location VARCHAR(255),
    Balcony BOOLEAN,
    FOREIGN KEY (property_type_id) REFERENCES property_types(property_type_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id),
    FOREIGN KEY (room_config_id) REFERENCES rooms(room_config_id)
);

CREATE TABLE property_types (
    property_type_id SERIAL PRIMARY KEY,
    property_type VARCHAR(255)
);

CREATE TABLE rooms (
    room_config_id SERIAL PRIMARY KEY,
    Total_Rooms INT,
    BHK INT
);