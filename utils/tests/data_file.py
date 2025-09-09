def init_csv(file_path):
    """Create a new CSV file with headers if it doesn't exist."""
    if not os.path.exists(file_path):
        fieldnames = ["link", "postal_code", "locality", "price", "scraped_at", "property_type"]
        with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        print(f"✅ Created new CSV with headers at {file_path}")
        
init_csv()