import os
import pandas as pd
import re
import openpyxl

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}")
MAX_TOTAL_EMAILS = 12500  # Cap on total emails to extract

DEFAULT_EMAILS = [
    "checkemail.hdfclife@gmail.com",
    "psamant@hdfclife.com",
    "clintond@hdfclife.com",
    "vijayn@hdfclife.com",
    "vaibhav.b59@hdfclife.com",
    "rajeev.kanal@hdfclife.com",
    "S_SHITAL@HDFCLIFE.COM",
    "SUNILSUNDARM@HDFCLIFE.COM",
    "S_SANDESH@HDFCLIFE.COM",
    "VINODA.SHETTY@HDFCLIFE.COM",
    "R.SURESH2@HDFCLIFE.COM",
    "SOUMIYA.IYER@HDFCLIFE.COM",
    "JAIBINL@HDFCLIFE.COM",
    "SHASHANKSHRINIVASD@HDFCLIFE.COM",
    "bichudas@gmail.com",
    "bichudas5@gmail.com",
    "sunsumon@gmail.com",
    "rohitvinod92@gmail.com"
]
def identify_email_column(df):
    """
    Identify the column that contains email addresses.
    Return the column name or None if not found.
    """
    for col in df.columns:
        # Check headers
        headers_to_check = [
            "email", 
            "customer_email_id", 
            "emailid", 
            "email address"
            # ... (any other headers you want to check)
        ]
        
        if any(header in col.lower() for header in headers_to_check):
            print(f"Found email column: {col}")
            return col
        
        # Check the first 10 rows for email pattern
        if df[col].head(10).astype(str).str.contains(EMAIL_REGEX).mean() > 0.5:
            return col
    print("No email column found")
    return None

def verify_emails(emails):
    """
    Verify the validity of email addresses using regex.
    Filter out emails to only those ending with gmail.com or in DEFAULT_EMAILS.
    Return the email addresses if valid, None if not.
    """
    valid_emails = [email for email in emails if (isinstance(email, str) and EMAIL_REGEX.match(email)) or email in DEFAULT_EMAILS]
    
    if not valid_emails:
        print(f"No valid emails found in {len(emails)} emails")
        return None
    return valid_emails

def extract_emails_from_file(file_path):
    """
    Extract email addresses from an Excel or CSV file.
    Return a list of email addresses.
    """
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, engine='python')
    else:
        df = pd.read_excel(file_path, engine='openpyxl')
    
    email_col = identify_email_column(df)
    if email_col:
        print_status(f"Found email column: {email_col}")
        return df[email_col].dropna().tolist()
    return []

def print_status(status):
    print(status)

def save_to_csv(emails, base_path):
    """
    Save emails to a CSV file.
    If there are more than 5,000 emails, multiple files will be created.
    """
    MAX_EMAILS_PER_FILE = 5500 - len(DEFAULT_EMAILS)  # Adjusting for default emails
    num_files = len(emails) // MAX_EMAILS_PER_FILE + (len(emails) % MAX_EMAILS_PER_FILE > 0)
    
    for i in range(num_files):
        start_idx = i * MAX_EMAILS_PER_FILE
        end_idx = start_idx + MAX_EMAILS_PER_FILE
        
        combined_emails = DEFAULT_EMAILS + emails[start_idx:end_idx]  # Adding default emails at the start
        file_name = f"{base_path}_{i + 1}.csv"
        pd.DataFrame(combined_emails).to_csv(file_name, header=False, index=False)
        print(f"Saved {file_name}")

def main(input_directory, output_directory):
    """..."""
    all_emails_set = set()

    for root, dirs, files in os.walk(input_directory):
        for file in files:
            print_status(f"Checking {file}...")
            if file.endswith((".xlsx", ".xls", ".csv")):
                file_path = os.path.join(root, file)
                try:
                    print_status(f"Processing {file_path}...")
                    emails = extract_emails_from_file(file_path)
                    emails_len = len(emails)
                    print_status(f"Found {emails_len} emails in {file_path}")
                    verified_emails = verify_emails(emails)
                    if verified_emails:
                        all_emails_set.update(verified_emails)  # using a set to remove duplicates
                    # Stop extracting if we've reached the total limit
                    if len(all_emails_set) >= MAX_TOTAL_EMAILS:
                        break
                except Exception as e:
                    print(f"Error processing {file}: {e}")
        if len(all_emails_set) >= MAX_TOTAL_EMAILS:
            break

    all_emails_list = list(all_emails_set)[:MAX_TOTAL_EMAILS]  # Convert the set back to a list and apply limit

    print("Total unique emails:", len(all_emails_list))
    # Ensure the output directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    # Save emails in batches to CSV files
    output_base_path = os.path.join(output_directory, "output")
    save_to_csv(all_emails_list, output_base_path)

if __name__ == '__main__':
    INPUT_DIRECTORY = 'TestEmails'
    OUTPUT_DIRECTORY = 'output'
    main(INPUT_DIRECTORY, OUTPUT_DIRECTORY)