import json
import pandas as pd

# Function to preprocess data
def preprocess_data(input_file, output_file, batch_size=100000):
    with open(input_file, 'r') as f_in:
        data_generator = pd.read_json(f_in, chunksize=batch_size, lines=True)
        with open(output_file, 'w') as f_out:
            for chunk in data_generator:
                # Perform preprocessing steps
                preprocessed_chunk = preprocess_chunk(chunk)
                # Convert preprocessed chunk to JSON format and write to output file
                for _, row in preprocessed_chunk.iterrows():
                    row_json = row.to_dict()
                    # Handle date format conversion
                    try:
                        # Check if 'date' column is in the correct format for strftime
                        row_json['date'] = row_json['date'].strftime('%Y-%m-%d %H:%M:%S')
                    except AttributeError:
                        # If 'date' column is not in datetime format, skip conversion
                        pass
                    # Normalize data in each column (convert to lowercase)
                    for key, value in row_json.items():
                        if isinstance(value, str):
                            row_json[key] = value.lower()
                    
                    json.dump(row_json, f_out)
                    f_out.write('\n')

def preprocess_chunk(chunk):

    chunk = chunk.dropna()
    return chunk

input_file = 'Sampled_Amazon_Meta.json'
output_file = 'preprocessed_data3.json'
preprocess_data(input_file, output_file)













