import os
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count, Manager

def process_html_file(file_path, id_to_column_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()    

    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'id': "ctl00_ContentPlaceHolder1_gvCertificate"})
    
    # Extracting values for given IDs and mapping them to specified column names
    extracted_data = {}
    for id, column_name in id_to_column_name.items():
        element = soup.find(id=id)
        extracted_data[column_name] = element.get_text(strip=True) if element else "Data Not Found"
    
    table_data = []
    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:
            cols = row.find_all('td')
            row_data = [ele.text.strip() for ele in cols]
            if any(row_data):
                table_data.append(row_data)
    
    df = pd.DataFrame(table_data)
    for column_name, value in extracted_data.items():
        df.insert(0, column_name, value)
    return df

def update_progress(result, processed_files):
    processed_files.value += 1
    print(f"\rProcessed {processed_files.value} files.", end='')

def process_files_in_folder_parallel(parent_folder_path, id_to_column_name):
    manager = Manager()
    processed_files = manager.Value('i', 0)

    files = []
    for root, dirs, files_in_dir in os.walk(parent_folder_path):
        for file in files_in_dir:
            if file.endswith('.htm'):
                files.append(os.path.join(root, file))
    
    results = []
    with Pool(processes=cpu_count() - 2) as pool:
        for file in files:
            result = pool.apply_async(process_html_file, args=(file, id_to_column_name), callback=lambda _: update_progress(_, processed_files))
            results.append(result)

        pool.close()
        pool.join()

    combined_df = pd.concat([res.get() for res in results], ignore_index=True)
    return combined_df

if __name__ == '__main__':
    parent_folder_path = 'Incentives master report'
    # Define your ID to column name mapping here
    id_to_column_name = {
    "ctl00_ContentPlaceHolder1_lblApplicationNo" : "Application No",
    "ctl00_ContentPlaceHolder1_lblapplicationdate" : "Application Date",
    "ctl00_ContentPlaceHolder1_lblheadTPRIDE" : "TPRIDE or not",
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
    
    combined_df = process_files_in_folder_parallel(parent_folder_path, id_to_column_name)

    combined_csv_path = 'Table for ANNEXURE: XIII - Reimbursement on equipment purchased for cleaner production measures, equipment purchased.csv'
    combined_df.to_csv(combined_csv_path, index=False)

    print(f"\nCombined CSV file saved at {combined_csv_path}")
