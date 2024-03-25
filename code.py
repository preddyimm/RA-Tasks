import os
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count, Manager

def process_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()    

    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'id': "ctl00_ContentPlaceHolder1_gvInterestDCP"})
    uid_element = soup.find(id="ctl00_ContentPlaceHolder1_lblApplicationNo")
    uid = uid_element.get_text(strip=True) if uid_element else "UID Not Found"
    table_data = []
    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:
            cols = row.find_all('td')
            row_data = [ele.text.strip() for ele in cols]
            if any(row_data):
                table_data.append(row_data)

    df = pd.DataFrame(table_data)
    df.insert(0, 'UID', uid)
    return df

def update_progress(result, processed_files):
    processed_files.value += 1
    print(f"\rProcessed {processed_files.value} files.", end='')

def process_files_in_folder_parallel(parent_folder_path):
    manager = Manager()
    processed_files = manager.Value('i', 0)

    # New method to recursively find .htm files
    files = []
    for root, dirs, files_in_dir in os.walk(parent_folder_path):
        for file in files_in_dir:
            if file.endswith('.htm'):
                files.append(os.path.join(root, file))
    
    results = []
    with Pool(processes=cpu_count() - 2) as pool:
        for file in files:
            result = pool.apply_async(process_html_file, args=(file,), callback=lambda _: update_progress(_, processed_files))
            results.append(result)

        pool.close()
        pool.join()

    combined_df = pd.concat([res.get() for res in results], ignore_index=True)
    return combined_df

if __name__ == '__main__':
    parent_folder_path = 'Incentives master report'  # Adjusted to your parent folder path
    combined_df = process_files_in_folder_parallel(parent_folder_path)

    combined_csv_path = 'Table for ANNEXURE: IX - Reimbursement of Interest Subsidy under Pavala Vaddi Scheme.csv'
    combined_df.to_csv(combined_csv_path, index=False)

    print(f"\nCombined CSV file saved at {combined_csv_path}")
