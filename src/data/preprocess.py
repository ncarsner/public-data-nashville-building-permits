import pandas as pd

def preprocess_building_permits(input_file, output_file):
    all_data = []
    chunk_size = 1_000

    # Use chunksize to read the CSV file
    try:
        df_iter = pd.read_csv(input_file, chunksize=chunk_size, low_memory=False, encoding='utf-8', encoding_errors='replace')

        # Define a mapping for renaming columns
        column_mapping = {
            'Permit__': 'Permit',
            'Permit_Type_Description': 'Permit Type',
            'Permit_Subtype_Description': 'Permit Subtype',
            'Parcel': 'Parcel',
            'Date_Entered': 'Date Entered',
            'Date_Issued': 'Date Issues',
            'Const_Cost': 'Cost',
            'Address': 'Address',
            'City': 'City',
            'State': 'State',
            'Subdivision_Lot': 'Subvision',
            'Contact': 'Contact',
            'Per_Ty': 'Permit Type',
            'Per_SubTy': 'Permit Subtype',
            'IVR_Trk_': 'IVR Tracking',
            'Purpose': 'Purpose',
            'Council_Dist': 'District',
            'Lon': 'Longitude',
            'Lat': 'Latitude',
            'ObjectId': 'Object ID',
            'ZIP': 'Zip Code'
        }

        # Iterate through each chunk
        for chunk in df_iter:
            print(f"Processing chunk of {len(chunk):,} records...")

            # Rename columns according to the mapping
            chunk.rename(columns=column_mapping, inplace=True)
            
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

        # Concatenate all processed chunks into a single DataFrame
        if all_data:  # Ensure there's data to concatenate
            df = pd.concat(all_data, ignore_index=True)

            # Save the cleaned data to a new CSV file
            df.to_csv(output_file, index=False)

            # Print the total number of records processed for verification
            print(f"{len(df):,} records processed.")
        else:
            print("No data processed.")

    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    input_file = './data/raw/building_permits.csv'
    output_file = './data/processed/building_permits_cleaned.csv'
    preprocess_building_permits(input_file, output_file)
