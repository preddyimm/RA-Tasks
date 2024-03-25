import os
import pandas as pd
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count, Manager

def process_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()    

    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the table and UID element
    table = soup.find('table', {'id': "GridView3"})
    uid_element = soup.find(id="lblUidNo")
    uid = uid_element.get_text(strip=True) if uid_element else "UID Not Found"

    # Extract table data
    table_data = []
    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:  # Skipping the header row
            cols = row.find_all('td')
            row_data = [ele.text.strip() for ele in cols]
            if any(row_data):  # Skip rows that are completely empty
                table_data.append(row_data)

    # Convert to DataFrame and add UID column
    df = pd.DataFrame(table_data)
    df.insert(0, 'UID', uid)  # Insert UID as the first column

    return df

def update_progress(result, processed_files):
    processed_files.value += 1
    print(f"\rProcessed {processed_files.value} files.", end='')

def process_files_in_folder_parallel(folder_path):
    manager = Manager()
    processed_files = manager.Value('i', 0)

    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.htm') and os.path.isfile(os.path.join(folder_path, f))]
    
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
    folder_path = 'Data/CFE Approval stage/Print'  # Replace with your folder path
    combined_df = process_files_in_folder_parallel(folder_path)

    # Save the combined DataFrame as a CSV file
    combined_csv_path = 'combined_data_CFE_print_ Sewage_data.csv'  # Replace with your desired path
    combined_df.to_csv(combined_csv_path, index=False)

    print(f"\nCombined CSV file saved at {combined_csv_path}")
