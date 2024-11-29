# System A: Flask API to Expose Source 2 Data

from flask import Flask, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Database connection parameters
CONNECTION_PARAMS = {
    'dbname': 'real_estate_db_source_2',
    'user': 'postgres',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432'
}

@app.route('/get_properties', methods=['GET'])
def get_properties():
    conditions = request.args.get('conditions', '1=1')  # Default to return all if no conditions
    try:
        # Connect to the database
        conn = psycopg2.connect(**CONNECTION_PARAMS)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Define your query here (using source 2 schema)
        query = f"""
            SELECT 
                COALESCE(p.property_name, 'Unknown') AS Property_Name,
                p.property_title AS Property_Title,
                COALESCE(pt.property_type, 'Not Specified') AS Property_Type,
                COALESCE(p.price, 0) AS Price,
                COALESCE(p.total_area_sqft, 0) AS Total_Area,
                COALESCE(c.city, 'Unknown') AS City,
                COALESCE(l.location, 'Unknown') AS Location,
                COALESCE(p.price_per_sqft, 0) AS Price_per_SQFT,
                COALESCE(p.description, 'No description available') AS Description,
                COALESCE(r.total_rooms, 0) AS Number_Of_Rooms,
                COALESCE(p.balcony, false) AS Number_Of_Balconies,
                'source_2' as source
            FROM properties p
            LEFT JOIN locations l ON p.location_id = l.location_id
            LEFT JOIN cities c ON l.city_id = c.city_id
            LEFT JOIN property_types pt ON p.property_type_id = pt.property_type_id
            LEFT JOIN rooms r ON p.room_config_id = r.room_config_id
            WHERE {conditions}
            LIMIT 100
        """
        cur.execute(query)
        results = cur.fetchall()

        print(results)

        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
