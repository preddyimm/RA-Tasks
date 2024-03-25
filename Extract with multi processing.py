import os
from bs4 import BeautifulSoup
import csv
from multiprocessing import Pool, Manager

def extract_data_from_html(html_file_path, ids):
    with open(html_file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    soup = BeautifulSoup(html_content, "html.parser")
    extracted_data = {}
    for id_name in ids:
        element = soup.find(id=id_name)
        extracted_data[id_name] = element.get_text(strip=True) if element else "Not Found"
    
    return extracted_data


def extract_and_process_file(file_path, id_to_name_map, counter, lock):
    try:
        # Extract data from the HTML file
        data = extract_data_from_html(file_path, id_to_name_map.keys())
        
        # Update the counter and print the progress
        with lock:
            counter.value += 1
            print(f"Files processed: {counter.value}", end='\r')
        
        return data
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return {}

def process_html_folder(folder_path, output_csv_path, id_to_name_map, num_processes, counter, lock):
    file_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".htm") or f.endswith(".html")]

    with Pool(num_processes) as pool:
        all_data = pool.starmap(extract_and_process_file, [(path, id_to_name_map, counter, lock) for path in file_paths])

    # Create a list of fieldnames from id_to_name_map values in the order you provided
    fieldnames = list(id_to_name_map.values())

    # Write to CSV
    with open(output_csv_path, "w", newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for data in all_data:
            # Map keys in data to corresponding fieldnames
            row = {id_to_name_map.get(key, key): value for key, value in data.items()}
            writer.writerow(row)


def process_multiple_folders(parent_folder_path, id_to_name_map, num_processes):
    with Manager() as manager:
        counter = manager.Value('i', 0)
        lock = manager.Lock()

        for folder_name in os.listdir(parent_folder_path):
            folder_path = os.path.join(parent_folder_path, folder_name)
            if os.path.isdir(folder_path):
                print(f"Processing folder: {folder_name}")
                output_csv_path = os.path.join(parent_folder_path, f"{folder_name}.csv")
                process_html_folder(folder_path, output_csv_path, id_to_name_map, num_processes, counter, lock)
                print(f"\nCompleted processing for folder: {folder_name}")

    # Define individual dictionaries (Only a few examples provided here. Add all others similarly)
    
common_details_map = {
    "lblUidNo" : "UID No",
    "lblNameOfUndertaker" : "NAME OF INDUSTRIAL UNDERTAKING	",
    "lblNameOfPromoter" : "NAME OF PROMOTER",
    "lblSonOf" : "Promoters Father's Name",
}

proposed_location_map = {
    "lblProposedLocation" : "Location of Factory",
    "lblSurveyNo" : "Survey No",
    "lblNameofGp" : "Nameof District Grampanchayat/IE/IDA/SEZ",
    "lblvillage0" : "Village/Town",
    "lblMandal0" : "Mandal",
    "lblDistrict0" : "District",
    "lblPincode0" : "PinCode",
    "lblEmail0" : "Email-ID",
    "lblTelephone0" : "Telephone",
    "lblExtentofSightArea" : "Total extent of site area as per document(in Sq.mts)",
    "lblProposedArea" : "Proposed area for development(in Sq mts)",
    "lblBultupArea" : "Total built-up area(in Sq.mts)",
    "lbWidthToRoad" : "Existing width of approach road(in feet)",
    "lblTypeofRoad" : "Type of Approach Road",
    "lblLandComesUnder" : "Land comes under",
    "lblCaseType" : "Case type",
    "lblCategoryOfIndustry" : "Category of Industry",
    "lblCastes" : "Caste",
    "lblBuildingApproval" : "Building Approval",
    "lbldiffabled" : "Differently Abled"
}

entreprenuer_adress_map = {
    "lblDoorNo" : "Door No",
    "lblStreetName" : "Street",
    "lblvillage" : "Village",
    "lblMandal" : "Mandal",
    "lblDistrict" : "District",
    "lblState"  : "State",
    "lblPincode" : "Pincode",
    "lblEmail" : "Email",
    "lblTelephone" : "Telephone",
    "lblMobileNo" : "Mobile No"
}
proposal_map_expansion = {
    "lblLandCost" : "Existing Enterprise Land (In Rs.)",
    "lblExpInvestLandValue" : "Under Expansion/Diversification Project Land (In Rs.)",
    "lblTotalLandValue" : "Land Total Value (In Rs.)",
    "lblBuldingCost" : "Existing Enterprise Building (In Rs.)",
    "lblExpInvestBuildingValue" : "Under Expansion/Diversification Project Building (In Rs.)",
    "lblTotalBuilding" : "Building Total Value (In Rs.)",
    "lblPlantCost" : "Existing Enterprise Plant & Machinery (In Rs.)",
    "lblExpInvestPlantlValue" : "Under Expansion/Diversification Project Plant & Machinery (In Rs.)",
    "lblToalPlantValue" : "Plant & Machinery Total Value (In Rs.)",
    "lblTotExistingInvest" : "Total Existing Investment (In Rs.)",
    "lblExpInvestTotalValue" : "Total Under Expansion/Diversification Project Investment (In Rs.)",
    "lblTotalInvestment" : "Total Investment (In Rs.)",
    "lblturnOver" : "Existing Enterprise Turnover (In Rs.)",
    "lblTurnoverExp" : "Under Expansion/Diversification Project Turnover (In Rs.)"
}

proposal_map_new = {
    "ctl00_ContentPlaceHolder1_txtlandexistingNew": "Existing_new Enterprise Land (In Rs.)",
    "ctl00_ContentPlaceHolder1_txtbuildingexistingNew": "Existing_new Enterprise Building (In Rs.)",
    "ctl00_ContentPlaceHolder1_txtplantexistingNew": "Existing_new Enterprise Plant & Machinery (In Rs.)",
}

probable_employment_potentiL_map = {
    "lblMaleDirect" : "Direct Male Employment",
    "lblMaleIndirect" : "Indirect Male Employment",
    "lblMaleTotal" : "Total Male Employment",
    "lblFemaleDirect" : "Direct Female Employment",
    "lblFemaleIndirect"  : "Indirect Female Employment",
    "lblFemaleTotal"  : "Total Female Employment",
}

Registration_map = {
    "lblRegistration" : "Registration No.",
    "lblDate" : "Registration Date"
}

line_of_activity_map = {
    "lblLineofActiivity" : "Line of Activity"
}

#LOA_tables_map = {
#    "GridView1" : "LINE OF MANUFACTURE",
#    "GridView2" : "RAW MATERIALS USED IN PROCESS"
#}

Power_map = {
    "lblMaxDemand" : "Contracted maximum demand in KVA",
    "lblConnectedLoad" : "Connected load in KVA",
    "lblTransformerCapacity" : "Aggregate Installed Capacity OF The  transformer to be installed by the Entreprenuer",
    "lblRequiredVoltage" : "Required Voltage"
}

services_map = {
    "lblOtherServiceExisting"  : "Any other services existing in the same premises"
}

proposed_maximum_working_hours_map = {
    "lblHoursPerDay" : "Maximum working hours per day",
    "lblHoursPerMonth" : "Maximum working hours per month",
    "lblTrailProduction" : "Expected month and year of trial production",
    "lblPowerSupplyPerDate" : "Probable date of requirement of power supply"
}

water_supply_map = {
    "lblWaterSupplyDate" : "Water supply from",
    "lblWaterRequirement" : "Water requirement",
    "lblDrinkingwater" : "Drinking water ( in KL/Day )",
    "lblProcessingWater" : "Processing water ( in KL/Day )",
    "lblSourseOfWater" : "Source of water",
    "lblRequirementOfWaterInKLPerDay" : "Requirement of water in KL/Day",
    "lblConsumptiveWater" : "Quantity of water required for  consumptive use (in KL/Day)",
    "lblNonConsumptiveWater" : "Quantity of water required for  non-consumptive use (in KL/Day)"
}

fire_safety_map = {
    "lblHeightOfBulding" : "Height of the building",
    "lblHeightOfEachFloor" : "Height of each floor in mtrs",
    "lblInternaiStaircase" : "Internal staircase",
    "lblExternalStairCase" : "External staircase",
    "lblWidthOfStairCase" : "Width of staircase",
    "lblNoOfExits" : "No. of exits",
    "lblWidthOfEachExists" : "Width of each exit",
    "lblWidthOfStairCase15" : "Width of staircase",
    "lblSpaceInEast" : "Open space in East",
    "lblSpaceInWest" : "Open space in West",
    "lblSpaceInNorth" : "Open space in North",
    "lblSpaceInSouth" : "Open space in South",
    "lblLevelOfGround" : "Level of ground",
    "lblFireDetectionSystem" : "Fire detection system",
    "lblFireAlarmSystem" : "Fire alarm system",
    "lblSprinkler" : "Sprinkler",
    "lblFoam" : "Foam",
    "lblCO2" : "CO2",
    "lblDCP" : "DCP",
    "lblFireServiceInlet" : "Fire service inlet",
    "lblUnderGrounDtank" : "Underground tank",
    "lblNoOfCouryYards" : "No. of courtyards",
    "lblFirePumpElectricity" : "Fire pump electricity 15 mtrs. To 30 mtrs. Ht.",
    "lblDiesel" : "Fire pump diesel",
    "lblCO7" : "CO7"
}

Labour_Department_map = {
    "lblCategory" : "Category of Establishment",
    "lblMangerName" : "Name of the Manager",
    "lblFatherName" : "Manegers Father's Name",
    "lblDesignation" : "Designation",
    "lblAddressofManger" : "Address of the Manager",
    "lblnatureofwork" : "Nature of Work",
    "lblEstimatedComm" : "Estimated date of commencement of building or other construction work",
    "lblMaximumWorkers" : "Maximum number of building workers to be employed on any day",
    "lblconstructiondate" : "Date of completion of construction work",
    "lblPriName" : "Name of the Principal Employer",
    "lblPriPGName" : "Name of the Principal Employer's Parent/Guardian",
    "lblPriDesgn" : "Principal Employer Designation",
    "lblPriAge" : "Principal Employer Age",
    "lblPriEmail" : "Principal Employer Email",
    "lblPriMobileNo" : "Principal Employer Mobile No",
    "lblPriAddress" : "Principal Employer Address",
    "lblDirName" : "Name of the Director",
    "lblDirDoorNo" : "Door No",
    "lblDirDistrict" : "District",
    "lblDirMandal" : "Mandal",
    "lblDirVillage" : "Village"
}

transformer_protection_measures_map = {
    "lblTrolly" : "45 Ltrs. From Trolley",
    "lblFencing" : "Fencing",
    "lblSoakPit" : "Soak Pit",
    "lbllighteningProtectin" : "Lightening Protection",
    "lblControlRoom" : "Control Room",
    "lblHydralicPlatform" : "Whether the Hydraulic Platform can be moved all around the bldg"
}

PCB_map = {
    "lblProcess" : "Waste water generation in KLD Process",
    "lblWashings" : "Waste water generation in KLD Washings",
    "lblBoilerBlowDown" : "Waste water generation in KLD Boiler Blow Down",
    "lblCoolingTowerBleed" : "Waste water generation in KLD Cooling Tower Bleed",
    "lblDomestic" : "Waste water generation in KLD Domestic",
    "lblTotalWasteWater" : "Total Waste Water Generation in KLD"
}

air_pollution_map = {
    "lblCapacityOfDGSet" : "Capacity of DG Set",
    "lblFuelConsumptionPerDay" : "Fuel Consumption Per Day",
    "lblFuelStorageDetails" : "Fuel Storage Details",
    "lblStackHeight" : "Stack Height",
    "lblAirPolutionControlEquipement" : "Air Pollution Control Equipement"
}

process_waste_map = {
    "lblEmissionCharacteristtics" : "Emission Characteristtics",
    "lblQuwntityOfEmission" : "Quwntity Of Emission",
    "lblControlSystem" : "Control System",
    "lblGeneratorRequred" : "Is the Project requires Generator"
}

#Waste_table_map = {
#    "GridView3" : "Solid and hazardous waste"
#}

# Combined dictionary of all IDs and their descriptive names
combined_id_map = {
    **common_details_map,
    **proposed_location_map,
    **entreprenuer_adress_map,
    **proposal_map_expansion,
    **proposal_map_new,
    **probable_employment_potentiL_map,
    **Registration_map,
    **line_of_activity_map,
    **Power_map,
    **services_map,
    **proposed_maximum_working_hours_map,
    **water_supply_map,
    **fire_safety_map,
    **Labour_Department_map,
    **transformer_protection_measures_map,
    **PCB_map,
    **air_pollution_map,
    **process_waste_map   
}


if __name__ == "__main__":

# Parent folder path containing all subfolders
    parent_folder_path = "Data/CFE Reports/New folder"

# Number of processes to use (you can adjust this number)
    num_processes = 10

# Process each subfolder in the parent folder using multiprocessing
    process_multiple_folders(parent_folder_path, combined_id_map, num_processes)
