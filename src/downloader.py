import os
import re
import pandas as pd
import threading
import time

from threading import Lock

import requests

# Define a lock for thread safety when accessing/modifying metadata_df
metadata_lock = Lock()

def is_valid_url(url):
    """
    Check if the provided URL is valid.

    Parameters:
        url (str): The URL to validate.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    url_pattern = re.compile(
        r'^(http|https)://'  # Scheme
        r'([0-9a-z\.\-]+)\.([a-z]{2,})(:[0-9]+)?'  # Domain name and optional port
        r'(\/[^\s]*)?'  # Path
        r'$', re.IGNORECASE)
    return bool(url_pattern.match(url))

def sanitize_filename(filename):
    """
    Sanitize a filename by removing any potentially dangerous characters.

    Parameters:
        filename (str): The filename to sanitize.

    Returns:
        str: The sanitized filename.
    """
    # Remove potentially dangerous characters from the filename
    return re.sub(r'[^\w\-.]', '_', filename)

def download_report(url, br_number, output_folder, metadata_df, metadata_excel_file, skip_existing=True):
    try:
        filename = sanitize_filename(f"{br_number}.pdf")  # Sanitize filename
        if not is_valid_url(url):
            raise ValueError("Invalid URL")

        # Check if report already exists
        with metadata_lock:
            if skip_existing and br_number in metadata_df['Brnum'].values:
                return False

        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()

        # Check if response is okay (status code 2xx)
        if not response.ok:
            update_metadata_with_status(metadata_df, br_number, 'no', metadata_excel_file)  # Update metadata with status 'no'
            return False

        # Check if file size is non-zero
        if not response.headers.get('content-length'):
            update_metadata_with_status(metadata_df, br_number, 'no', metadata_excel_file)  # Update metadata with status 'no'
            return False

        # Save file to disk
        with open(os.path.join(output_folder, filename), 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

        # Check if file was actually downloaded and has non-zero size
        if not os.path.isfile(os.path.join(output_folder, filename)) or os.path.getsize(
                os.path.join(output_folder, filename)) == 0:
            update_metadata_with_status(metadata_df, br_number, 'no', metadata_excel_file)  # Update metadata with status 'no'
            return False

        # Update metadata if download is successful
        with metadata_lock:
            metadata_df.loc[metadata_df['Brnum'] == br_number, 'pdf_downloaded'] = 'yes'

        return True
    except Exception as e:
        update_metadata_with_status(metadata_df, br_number, 'no', metadata_excel_file)  # Update metadata with status 'no'
        return False

def update_metadata_with_status(metadata_df, br_number, status, metadata_excel_file):
    with metadata_lock:
        metadata_df.loc[metadata_df['Brnum'] == br_number, 'pdf_downloaded'] = status
        try:
            metadata_df.to_excel(metadata_excel_file, index=False)  # Update metadata in the Excel file
        except Exception as e:
            pass

def estimate_time_per_report(df, url_column, br_number_column, output_folder, metadata_df, sample_size=100):
    try:
        # Select a sample of reports from your dataset
        sample_reports = df.sample(sample_size)

        # Start time for sample time estimation
        sample_start_time = time.time()

        # Create threads for downloading each report in the sample
        threads = []
        for _, row in sample_reports.iterrows():
            url = row[url_column]
            br_number = row[br_number_column]
            if pd.notnull(url):
                thread = threading.Thread(target=download_report, args=(url, br_number, output_folder, metadata_df, True))
                threads.append(thread)
                thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

        # End time for sample time estimation
        sample_end_time = time.time()

        # Calculate total time for sample
        sample_total_time = sample_end_time - sample_start_time

        # Calculate average time per report for sample
        average_time_per_report = sample_total_time / sample_size

        # Calculate estimated total download time for all reports
        total_reports = len(df)  # Total number of reports in the dataset
        estimated_total_time = average_time_per_report * total_reports

        # Print results
        print(f"Average time per report for sample: {average_time_per_report:.2f} seconds")
        print(f"Estimated total download time for all reports: {estimated_total_time:.2f} seconds")
    except Exception as e:
        pass

def download_reports_from_excel(excel_file, url_column, br_number_column, output_folder, metadata_excel_file, limit=30, skip_existing=True):
    try:
        if not os.path.isfile(excel_file):
            raise FileNotFoundError("Excel file not found")

        df = pd.read_excel(excel_file)
        metadata_df = pd.read_excel(metadata_excel_file)

        estimate_time_per_report(df, url_column, br_number_column, output_folder, metadata_df, sample_size=100)

        count = 0
        threads = []
        for _, row in df.iterrows():
            if count >= limit:
                break
            url = row[url_column]
            br_number = row[br_number_column]
            if pd.notnull(url):
                thread = threading.Thread(target=download_report, args=(url, br_number, output_folder, metadata_df, metadata_excel_file, skip_existing))
                threads.append(thread)
                thread.start()
                count += 1

        for thread in threads:
            thread.join()

        # Write metadata to Excel file
        write_to_excel(metadata_df, metadata_excel_file)

    except Exception as e:
        pass

    
def write_to_excel(dataframe, excel_file):
    try:
        # Read existing Excel file into a DataFrame
        existing_df = pd.read_excel(excel_file)

        # Concatenate the existing DataFrame with the new DataFrame
        combined_df = pd.concat([existing_df, dataframe], ignore_index=True)

        # Write the combined DataFrame back to the Excel file
        combined_df.to_excel(excel_file, index=False)
        print(f"Data successfully written to {excel_file}.")

    except Exception as e:
        print(f"Failed to write data to {excel_file}: {e}")

        # Print the DataFrame read from Excel after writing
        read_df = pd.read_excel(excel_file)
        print("DataFrame read from Excel after writing:")
        print(read_df)
