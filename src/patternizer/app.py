from data_preparation import import_and_fix_csv_column_names
from pattern_analysis import patternize_index

# Load and prepare data
df = import_and_fix_csv_column_names("data.csv", index_obj)

# Perform pattern analysis
results = patternize_index(df, 0.5, "target_index", index_obj)

# Pass results to database_operations for storage