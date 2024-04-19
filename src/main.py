from downloader import download_reports_from_excel#, write_example_data_to_excel
import pandas as pd
import time

def main():
    # Define the paths and columns
    excel_file = "data/GRI_2017_2020.xlsx"
    url_column = "Pdf_URL"
    br_number_column = "BRnum"
    output_folder = "reports"
    metadata_excel_file = "data/metadata2.xlsx"
    limit = 10
    
    #Write example data to the metadata Excel file
    #write_example_data_to_excel(metadata_excel_file)
    
    # Call the function to download reports
    download_reports_from_excel(excel_file, url_column, br_number_column, output_folder, metadata_excel_file, limit)
    

if __name__ == "__main__":
    main()
