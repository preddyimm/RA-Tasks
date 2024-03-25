import os
import csv
from bs4 import BeautifulSoup
import pandas as pd
from multiprocessing import Pool, Manager, cpu_count

# Define your mappings and specific ID as provided earlier
parent_folder = '/Volumes/New Volume/RA Tasks/Incentives master report/'
specific_id = 'ctl00_ContentPlaceHolder1_divPowerCost'

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

#ANNEXURE: VII - Reimbursement of Power Cost

annexure_vii_id = "ctl00_ContentPlaceHolder1_divPowerCost"

power_map = {
    "ctl00_ContentPlaceHolder1_ddlPowerStatus" : "POWER TYPE",
    "ctl00_ContentPlaceHolder1_txtNewPowerReleaseDate" : "New Power Released Date",
    "ctl00_ContentPlaceHolder1_txtServiceConnectionNumberNew" : "New Service Connection Number",
    "ctl00_ContentPlaceHolder1_txtNewContractedLoad" : "New Contracted Load (in HP)",
    "ctl00_ContentPlaceHolder1_txtNewConnectedLoad" : "New Connected Load (in HP)",
    "ctl00_ContentPlaceHolder1_txtExistPowerReleaseDate" : "Existing Power Released Date",
    "ctl00_ContentPlaceHolder1_txtServiceConnectionNumberExist" : "Existing Service Connection Number",
    "ctl00_ContentPlaceHolder1_txtExistContractedLoad" : "Existing Contracted Load (in HP)",
    "ctl00_ContentPlaceHolder1_txtExistConnectedLoad" : "Existing Connected Load (in HP)",
    "ctl00_ContentPlaceHolder1_txtExpanPowerReleaseDate" : "Expansion Power Released Date",
    "ctl00_ContentPlaceHolder1_txtServiceConnectionNumberExpan" : "Expansion Service Connection Number",
    "ctl00_ContentPlaceHolder1_txtExpanContractedLoad" : "Expansion Contracted Load (in HP)",
    "ctl00_ContentPlaceHolder1_txtExpanConnectedLoad" : "Expansion Connected Load (in HP)"
}


annexure_vii_total_claim_power = {
    "ctl00_ContentPlaceHolder1_txtClaimedAmount" : "Claim Applied for (Amount in Rs.) for Reimbursement of Power Cost"
}

id_to_column_mapping = {**common_details, **power_map, **annexure_vii_total_claim_power}


# Function to process each HTML file
def process_html_file(args):
    file_path, id_to_column_mapping, specific_id, counter, lock = args
    data_found = False
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            soup = BeautifulSoup(file, 'html.parser')
        
        # Check for the specific id and extract data if found
        if soup.find(id=specific_id):
            extracted_data = {}
            for id_, column_name in id_to_column_mapping.items():
                element = soup.find(id=id_)
                extracted_data[column_name] = element.text.strip() if element else ''
            data_found = True
        else:
            extracted_data = {}
    except Exception as e:
        extracted_data = {}
        print(f"Error processing file {file_path}: {e}")
    
    # Update progress
    with lock:
        counter.value += 1
        print(f"\rProcessed {counter.value} files.", end='', flush=True)
    
    return extracted_data if data_found else None

# Main function to orchestrate the multiprocessing and data extraction
def main(parent_folder, id_to_column_mapping):
    # Find all HTML files
    file_paths = [os.path.join(dp, f) for dp, dn, filenames in os.walk(parent_folder) for f in filenames if f.endswith('.html') or f.endswith('.htm')]
    
    # Setup multiprocessing
    with Manager() as manager:
        counter = manager.Value('i', 0)
        lock = manager.Lock()
        pool_args = [(file_path, id_to_column_mapping, specific_id, counter, lock) for file_path in file_paths]
        
        with Pool(cpu_count(), maxtasksperchild=1000) as pool:
            results = pool.map(process_html_file, pool_args)
        
        print("\nProcessing complete.")
        
        # Filter out None results and prepare for CSV writing
        extracted_info = [data for data in results if data is not None]

    # Write the extracted info to a CSV file
    if extracted_info:
        output_csv_path = 'ANNEXURE: VII - Reimbursement of Power Cost.csv'  # Adjust path as needed
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=list(id_to_column_mapping.values()))
            writer.writeheader()
            for data in extracted_info:
                writer.writerow(data)

if __name__ == "__main__":
    # Example usage
    main(parent_folder, id_to_column_mapping)
