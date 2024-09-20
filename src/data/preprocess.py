import pandas as pd

def preprocess_building_permits(input_file, output_file):
    all_data = []
    chunk_size = 500
    total_chunks = 0

    # Create an iterator to read the CSV
    reader = pd.read_csv(input_file, iterator=True, low_memory=False)
    
    while True:
        try:
            chunk = reader.get_chunk(chunk_size)  # Read in chunks
            total_chunks += chunk_size
            # print(f"Processing chunk of {len(chunk):,} records...")

            # Standardize column names to lowercase
            chunk.columns = [col.lower() for col in chunk.columns]
            
            # Remove duplicate rows
            chunk.drop_duplicates(inplace=True)
            
            # Fill missing values with NaN
            chunk.fillna(pd.NA, inplace=True)
            
            # Convert Unix epoch (in milliseconds) to date for relevant columns
            unix_columns = ['date entered',]
            for col in unix_columns:
                if col in chunk.columns:
                    # Convert to datetime from milliseconds
                    chunk[col] = pd.to_datetime(chunk[col] / 1000, unit='s', errors='coerce').dt.date
            
            all_data.append(chunk)
            print(f"Processed {total_chunks:,} records so far...")

        except StopIteration:
            print("No more chunks to read.")
            break

    # Concatenate all processed chunks into a single DataFrame
    df = pd.concat(all_data, ignore_index=True)
    
    # Save the cleaned data to a new CSV file
    df.to_csv(output_file, index=False)

    # Print the total number of records processed for verification
    print(f"Total records processed: {len(df):,}.")

if __name__ == "__main__":
    input_file = './data/raw/building_permits.csv'
    output_file = './data/processed/building_permits_cleaned.csv'
    preprocess_building_permits(input_file, output_file)
