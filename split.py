import pandas as pd
import os

def split_csv_file(filename, row_limit=1000000):
    """
    Split a CSV file into smaller files.

    Parameters:
    - filename (str): The name of the CSV file to split.
    - row_limit (int): The maximum number of rows for each split file.

    Returns:
    - None
    """

    # Read the CSV file
    data = pd.read_csv(filename)

    # Determine the number of chunks
    num_chunks = len(data) // row_limit + (1 if len(data) % row_limit != 0 else 0)

    # Split and save
    for i in range(num_chunks):
        chunk = data.iloc[i * row_limit: (i + 1) * row_limit]
        base, ext = os.path.splitext(filename)
        chunk_filename = f"{base}_part{i + 1}{ext}"
        chunk.to_csv(chunk_filename, index=False)
        print(f"Saved chunk to {chunk_filename}")

    return True

# Example usage
filename = "all_in_one_12L.csv"
split_csv_file(filename)
