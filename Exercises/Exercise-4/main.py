import os
import glob
import json
import csv

def flatten_json(nested_json, parent_key='', sep='_'):
    """Flatten nested JSON (dict and list) into a single-level dictionary."""
    items = []
    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.extend(flatten_json({f"{i}": item}, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def json_to_csv(json_path, output_dir='csv_output'):
    """Convert a JSON file to CSV, handling both single objects and lists."""
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        print(f"[✘] Error reading JSON file {json_path}: {e}")
        return
    except FileNotFoundError:
        print(f"[✘] File not found: {json_path}")
        return

    # Handle both single JSON objects and lists
    if isinstance(data, list):
        flat_data = [flatten_json(item) for item in data]
    else:
        flat_data = [flatten_json(data)]

    if not flat_data:
        print(f"[!] No valid data to write from: {json_path}")
        return

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, os.path.basename(json_path).replace('.json', '.csv'))

    try:
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=flat_data[0].keys())
            writer.writeheader()
            for row in flat_data:
                writer.writerow(row)
        print(f"[✔] Converted: {json_path} → {csv_path}")
    except Exception as e:
        print(f"[✘] Error writing CSV file {csv_path}: {e}")

def create_sample_json_if_empty(data_dir):
    """Create a sample JSON file if the data directory is empty."""
    json_files = glob.glob(os.path.join(data_dir, '**/*.json'), recursive=True)
    if not json_files:
        sample = {
            "name": "John Doe",
            "location": {
                "type": "Point",
                "coordinates": [-99.9, 16.88333]
            },
            "age": 30
        }
        sample_path = os.path.join(data_dir, "sample.json")
        os.makedirs(data_dir, exist_ok=True)
        try:
            with open(sample_path, 'w', encoding='utf-8') as f:
                json.dump(sample, f, indent=2)
            print(f"[+] Created sample file: {sample_path}")
            return [sample_path]
        except Exception as e:
            print(f"[✘] Error creating sample file {sample_path}: {e}")
            return []
    return json_files

def main():
    data_dir = "data"
    output_dir = "csv_output"

    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    print(f"[+] Ensured directory exists: {data_dir}")

    # Collect JSON files or create a sample if none exist
    json_files = create_sample_json_if_empty(data_dir)

    if not json_files:
        print("[!] No JSON files found and unable to create sample.")
        return

    # Convert each JSON file to CSV
    for path in json_files:
        json_to_csv(path, output_dir)

if __name__ == "__main__":
    main()
