import os
import csv
from bs4 import BeautifulSoup
import pandas as pd

# Your setup
parent_folder = 'd:\RA Tasks\Data\Incentives master report'
specific_id = 'ctl00_ContentPlaceHolder1_divSCAndST'

common_details = {
    "ctl00_ContentPlaceHolder1_lblApplicationNo" : "Application No",
    "ctl00_ContentPlaceHolder1_lblapplicationdate" : "Application Date",
    "ctl00_ContentPlaceHolder1_txtPanNumber1" : "PAN Number",
    "ctl00_ContentPlaceHolder1_ddltypeofOrg1" : "Type of Organisation",
    "ctl00_ContentPlaceHolder1_ddlCategory1" : "Category",
    "ctl00_ContentPlaceHolder1_ddlindustryStatus1" : "Industry Status",
    "ctl00_ContentPlaceHolder1_rblCaste1" : "Caste",
    "ctl00_ContentPlaceHolder1_ddldistrictunit1" : "Unit District",
    "ctl00_ContentPlaceHolder1_ddlUnitMandal1" : "Unit Mandal",
    "ctl00_ContentPlaceHolder1_ddldistrictoffc1" : "District Office",
    "ctl00_ContentPlaceHolder1_ddloffcmandal1" : "Mandal Office",
}

#ANNEXURE: XVI - Claiming Advance Subsidy for SC / ST Entrepreneurs

annexure_xvi_id = "ctl00_ContentPlaceHolder1_divSCAndST"

# Adding the Approved/Estimated projected cost, term loan sanctioned and released, assets acquired etc because it might be relavent
land_asset_details = {
    "ctl00_ContentPlaceHolder1_txtLand2" : "Land Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtLand3" : "Land Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtLand4" : "Land Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtLand5" : "Land Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtLand6" : "Land Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtLand7" : "Land Value certified by Chartered Accoutant (Rs.)"
}

building_asset_details = {
    "ctl00_ContentPlaceHolder1_txtBuilding2" : "Building Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtBuilding3" : "Building Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtBuilding4" : "Building Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtBuilding5" : "Building Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtBuilding6" : "Building Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtBuilding7" : "Building Value certified by Chartered Accoutant (Rs.)"
}

plant_machinery_asset_details = {
    "ctl00_ContentPlaceHolder1_txtPM2" : "Plant & Machinery Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtPM3" : "Plant & Machinery Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtPM4" : "Plant & Machinery Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtPM5" : "Plant & Machinery Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtPM6" : "Plant & Machinery Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtPM7" : "Plant & Machinery Value certified by Chartered Accoutant (Rs.)"
}

machinary_contingency_asset_details = {
    "ctl00_ContentPlaceHolder1_txtMCont2" : "Machinery Contingency Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtMCont3" : "Machinery Contingency Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtMCont4" : "Machinery Contingency Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtMCont5" : "Machinery Contingency Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtMCont6" : "Machinery Contingency Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtMCont7" : "Machinery Contingency Value certified by Chartered Accoutant (Rs.)"
}

erection_asset_details = {
    "ctl00_ContentPlaceHolder1_txtErec2" : "Erection Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtErec3" : "Erection Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtErec4" : "Erection Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtErec5" : "Erection Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtErec6" : "Erection Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtErec7" : "Erection Value certified by Chartered Accoutant (Rs.)"
}

feasibility_study_asset_details = {
    "ctl00_ContentPlaceHolder1_txtTFS2" : "Technical Feasibility Study Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtTFS3" : "Technical Feasibility Study Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtTFS4" : "Technical Feasibility Study Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtTFS5" : "Technical Feasibility Study Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtTFS6" : "Technical Feasibility Study Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtTFS7" : "Technical Feasibility Study Value certified by Chartered Accoutant (Rs.)"
}

working_capital_asset_details = {
    "ctl00_ContentPlaceHolder1_txtWC2" : "Working Capital Approved Projected Cost (Rs.)",
    "ctl00_ContentPlaceHolder1_txtWC3" : "Working Capital Loan Sanctioned (Rs.)",
    "ctl00_ContentPlaceHolder1_txtWC4" : "Working Capital Equity from the promoters (Rs.)",
    "ctl00_ContentPlaceHolder1_txtWC5" : "Working Capital Loan Amount Released (Rs.)",
    "ctl00_ContentPlaceHolder1_txtWC6" : "Working Capital Value (as certified by financial institution) (Rs.)",
    "ctl00_ContentPlaceHolder1_txtWC7" : "Working Capital Value certified by Chartered Accoutant (Rs.)"
}

#annexure XVI - Claiming Advance Subsidy for SC / ST Entrepreneurs ID's
annexure_xvi = {
    "ctl00_ContentPlaceHolder1_txtTotalEquity" : "Total equity from promotors / share holders / partners to be brought in Rs",
    "ctl00_ContentPlaceHolder1_txtOwnCapital" : "Own Capital (Rs.)",
    "ctl00_ContentPlaceHolder1_txtBorrowed" : "Borrowed from outside (Rs.)",
    "ctl00_ContentPlaceHolder1_txtAdvSubClaimed" : "Advance Subsidy claimed (Rs.)"
}

#Final Anexure XVI Combined Dictionary
id_to_column_mapping = {**common_details, **land_asset_details, **building_asset_details, **plant_machinery_asset_details, **machinary_contingency_asset_details, **erection_asset_details, **feasibility_study_asset_details, **working_capital_asset_details, **annexure_xvi}

# Function to process each HTML file
def process_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')
        
        # Check for the specific id
        if soup.find(id=specific_id):
            extracted_data = {}
            # Extract data based on the provided id to column mapping
            for id_, column_name in id_to_column_mapping.items():
                element = soup.find(id=id_)
                extracted_data[column_name] = element.text.strip() if element else ''
            return extracted_data
    return None

# Function to count the total number of HTML files
def count_html_files(parent_folder):
    total_files = 0
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            if file.endswith('.htm') or file.endswith('.html'):
                total_files += 1
    return total_files

# Updated function to process HTML files and track progress
def iterate_through_directories_and_write_csv(parent_folder):
    total_files = count_html_files(parent_folder)
    processed_files = 0
    extracted_info = []

    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            if file.endswith('.htm') or file.endswith('.html'):
                file_path = os.path.join(root, file)
                processed_files += 1
                extracted_data = process_html_file(file_path)
                if extracted_data:
                    extracted_info.append(extracted_data)
                
                # Print progress in a single line
                progress = (processed_files / total_files) * 100  # Calculate progress percentage
                print(f"\rProcessed {processed_files}/{total_files} files - {progress:.2f}%", end='')

    print()  # Ensure the next print statement appears on a new line after the progress updates
    
    # Write the extracted info to a CSV file
    if extracted_info:
        with open('New folder\Extracted Annexure Data\extracted_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(id_to_column_mapping.values()))
            writer.writeheader()
            for data in extracted_info:
                writer.writerow(data)

# Execute the main function
iterate_through_directories_and_write_csv(parent_folder)

