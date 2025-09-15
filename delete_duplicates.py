import pandas as pd


input_file = "data/properties.csv"
output_file = "data/properties_noduplicates.csv"

# Load CSV into DataFrame
df = pd.read_csv(input_file)

# Remove duplicates based on 'links' column (keep the first occurrence)
df_cleaned = df.drop_duplicates(subset=["link"], keep="first")

# Save the cleaned DataFrame to a new CSV
df_cleaned.to_csv(output_file, index=False)

print(f"✅ Duplicates removed. Cleaned file saved as: {output_file}")
