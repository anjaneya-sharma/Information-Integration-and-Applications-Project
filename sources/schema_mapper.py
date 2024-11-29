from difflib import SequenceMatcher
import json
from pathlib import Path

class SchemaMapper:
    def __init__(self):
        self.schema_file = Path('sources/schema_store.json')
        self.schema = self._load_schema()
        self.mapping_cache = {}

    def _load_schema(self):
        try:
            if self.schema_file.exists():
                with open(self.schema_file) as f:
                    return json.load(f)
            return {}
        except Exception as e:
            print(f"Error loading schema: {e}")
            return {}

    def _save_schema(self):
        try:
            with open(self.schema_file, 'w') as f:
                json.dump(self.schema, f, indent=4)
            print("Schema saved successfully")
            self.mapping_cache.clear()
        except Exception as e:
            print(f"Error saving schema: {e}")

    def get_column(self, source, std_column_name):
        """Get mapped column name from schema"""
        cache_key = f"{source}:{std_column_name}"
        if cache_key in self.mapping_cache:
            return self.mapping_cache[cache_key]

        if source in self.schema and 'columns' in self.schema[source]:
            mapped_name = self.schema[source]['columns'].get(std_column_name)
            if mapped_name:
                self.mapping_cache[cache_key] = mapped_name
                return mapped_name
        return std_column_name

    def find_matching_column(self, source, failed_column, actual_columns):
        """Find best matching column and update schema"""
        print(f"\n=== Finding match for {failed_column} ===")
        print(f"Available columns: {actual_columns}")
        
        # Try fuzzy matching
        best_match = None
        best_score = 0
        
        for col in actual_columns:
            score = SequenceMatcher(None, failed_column.lower(), col.lower()).ratio()
            print(f"Comparing {failed_column} with {col}: score {score}")
            if score > best_score and score >= 0.8:
                best_score = score
                best_match = col
                print(f"New best match: {best_match}")

        if best_match:
            print(f"Found match: {failed_column} -> {best_match}")
            # Update schema mapping
            if source not in self.schema:
                self.schema[source] = {'columns': {}}
            
            # Find standard name that uses failed column
            for std_name, current_col in self.schema[source]['columns'].items():
                if current_col == failed_column:
                    print(f"Updating mapping for {std_name}: {failed_column} -> {best_match}")
                    self.schema[source]['columns'][std_name] = best_match
                    self._save_schema()
                    return best_match

            # If no existing standard name found, add new mapping
            print(f"Adding new mapping: {failed_column} -> {best_match}")
            for std_name in ['Property_Name', 'Property_Title', 'Price', 'Total_Area']:
                if std_name not in self.schema[source]['columns']:
                    self.schema[source]['columns'][std_name] = best_match
                    break
            self._save_schema()
            return best_match

        return failed_column