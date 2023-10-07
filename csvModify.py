import pandas as pd

def remove_emails_from_csv(input_csv, output_csv, emails_to_remove):
    """
    Remove specific emails from a CSV file.

    Args:
    - input_csv (str): path to the input CSV file.
    - output_csv (str): path to save the output CSV file after removal.
    - emails_to_remove (list): list of email addresses to be removed.
    """

    # Load CSV into a pandas DataFrame
    df = pd.read_csv(input_csv)

    # Identify the email column
    email_column = None
    for col in df.columns:
        if 'email' in col.lower():
            email_column = col
            break

    if not email_column:
        print("Email column not found in the CSV.")
        return

    # Remove rows with specified emails
    initial_len = len(df)
    df = df[~df[email_column].isin(emails_to_remove)]
    removed_count = initial_len - len(df)

    # Save the updated DataFrame to a new CSV
    df.to_csv(output_csv, index=False)

    print(f"Removed {removed_count} emails. Updated CSV saved to {output_csv}.")


def append_emails_to_csv(input_csv, output_csv, emails_to_append):
    """
    Append new emails to a CSV file.

    Args:
    - input_csv (str): path to the input CSV file.
    - output_csv (str): path to save the output CSV file after appending.
    - emails_to_append (list): list of email addresses to be appended.
    """

    # Load CSV into a pandas DataFrame
    df = pd.read_csv(input_csv)

    # Identify the email column
    email_column = None
    for col in df.columns:
        if 'email' in col.lower():
            email_column = col
            break

    if not email_column:
        print("Email column not found in the CSV.")
        return

    # Create a new DataFrame with emails to append
    df_append = pd.DataFrame({email_column: emails_to_append})

    # Concatenate the original and new DataFrames
    df = pd.concat([df, df_append], ignore_index=True)

    # Save the updated DataFrame to a new CSV
    df.to_csv(output_csv, index=False)

    print(f"Appended {len(emails_to_append)} emails. Updated CSV saved to {output_csv}.")

def length(input_csv):
    df = pd.read_csv(input_csv)
    print(f"Length of {input_csv} is {len(df)}")


if __name__ == "__main__":
    # ["output/swathy_50L.csv", "output/all_in_one_10L.csv", "output/adithya_50L.csv"]
    INPUT_CSV = ["CSV/all_in_one_10L.csv"]
    OUTPUT_CSV = ["output/swathy_50L.csv", "output/all_in_one_10L.csv", "output/adithya_50L.csv"]
    COUTPUT_CSV = ["adithya_50L.csv"]
    EMAILS_TO_REMOVE = ["VINODA.SHETTY@HDFCLIFE.COM"]  # Replace with the list of emails you want to remove
    # length(INPUT_CSV)
    # remove_emails_from_csv(INPUT_CSV, OUTPUT_CSV, EMAILS_TO_REMOVE)
    # length(OUTPUT_CSV)


    # for i in range(len(INPUT_CSV)):
    #     print(f"Removing emails from {INPUT_CSV[i]}...")
    #     remove_emails_from_csv(INPUT_CSV[i], OUTPUT_CSV[i], EMAILS_TO_REMOVE)
    
    # length of output csv
    for i in range(len(OUTPUT_CSV)):
        length(OUTPUT_CSV[i])


    # EMAILS_TO_APPEND = ["j4usr1b3@gmail.com"]
    # # # append_emails_to_csv(OUTPUT_CSV, OUTPUT_CSV, EMAILS_TO_APPEND)

    # for i in range(len(INPUT_CSV)):
    #     print(f"Appending emails to {OUTPUT_CSV[i]}...")
    #     append_emails_to_csv(OUTPUT_CSV[i], COUTPUT_CSV[i], EMAILS_TO_APPEND)
        

    # for i in range(len(COUTPUT_CSV)):
    #     length(COUTPUT_CSV[i])



