import csv
import random
from faker import Faker
import datetime
import re
import gradio as gr

# Initialize Faker for UK data
fake = Faker('en_GB')

# --- Configuration ---
OUTPUT_FILE = 'synthetic_autoglass_repairs.csv'

# --- Data Generation Options ---

# Geographically correct city-to-region mapping
CITY_REGION_MAP = {
    'London': ['London'],
    'West Midlands': ['Birmingham', 'Coventry', 'Wolverhampton', 'Stoke-on-Trent'],
    'North West': ['Manchester', 'Liverpool', 'Bolton'],
    'Yorkshire and the Humber': ['Sheffield', 'Leeds', 'Bradford'],
    'Scotland': ['Glasgow', 'Edinburgh', 'Aberdeen', 'Dundee'],
    'South West': ['Bristol', 'Plymouth', 'Swindon'],
    'South East': ['Southampton', 'Portsmouth', 'Brighton', 'Reading', 'Milton Keynes', 'Oxford'],
    'East Midlands': ['Leicester', 'Nottingham', 'Derby', 'Northampton'],
    'East of England': ['Luton', 'Norwich', 'Cambridge'],
    'North East': ['Newcastle upon Tyne'],
    'Wales': ['Cardiff', 'Swansea'],
    'Northern Ireland': ['Belfast']
}

# Hierarchical data structure for realistic vehicle models
VEHICLE_DATA = {
    'Ford': { 'Car': ['Fiesta', 'Focus', 'Mondeo'], 'SUV': ['Kuga', 'Puma'], 'Van': ['Transit'], 'Truck': ['Ranger']},
    'Vauxhall': { 'Car': ['Corsa', 'Astra'], 'SUV': ['Mokka', 'Grandland'], 'Van': ['Vivaro']},
    'Volkswagen': { 'Car': ['Golf', 'Polo', 'Passat'], 'SUV': ['T-Roc', 'Tiguan'], 'Van': ['Transporter', 'Caddy']},
    'BMW': { 'Car': ['1 Series', '3 Series', '5 Series'], 'SUV': ['X1', 'X3', 'X5'], 'Motorcycle': ['R 1250 GS']},
    'Mercedes-Benz': { 'Car': ['A-Class', 'C-Class', 'E-Class'], 'SUV': ['GLA', 'GLC'], 'Van': ['Sprinter', 'Vito']},
    'Audi': { 'Car': ['A1', 'A3', 'A4', 'A6'], 'SUV': ['Q2', 'Q3', 'Q5']},
    'Nissan': { 'Car': ['Micra', 'Leaf'], 'SUV': ['Juke', 'Qashqai', 'X-Trail'], 'Truck': ['Navara']},
    'Toyota': { 'Car': ['Yaris', 'Corolla', 'Prius'], 'SUV': ['C-HR', 'RAV4'], 'Truck': ['Hilux']},
    'Land Rover': { 'SUV': ['Defender', 'Discovery', 'Range Rover Evoque', 'Range Rover']},
    'Jaguar': { 'Car': ['XE', 'XF'], 'SUV': ['E-PACE', 'F-PACE']}
}

# Pre-generate a fixed list of 20 garage names
GARAGE_NAMES = [f"{fake.company()} Autoworks" for _ in range(20)]

# Expanded lists and options
GARAGE_TYPES = ['Independent', 'Franchsie', 'Chain', 'Mobile']
GLASS_TYPES = ['Tempered', 'Acoustic', 'Heated', 'Privacy', 'Tinted', 'Laminated', 'OEM', 'Solar-Control', 'Hydrophobic-coated']
WINDOW_POSITIONS = ['Front Windshield', 'windshield', 'wind_screen', 'Rear Window', 'Front Left Door', 'Front Right Door', 'Rear Left Door', 'Rear Right Door', 'Quarter Glass', 'Vent Glass', 'Sunroof', 'Tailgate Glass', 'Side Panel Glass']
DAMAGE_TYPES = ['Chip', 'Crack', 'Shatter', 'Scratch']
REPAIR_TYPES = ['Repair', 'Replacement', 'Chip Repair', 'Crack Repair', 'Reseal', 'ADAS Calibration']
WEATHER_CONDITIONS = ['Clear', 'Rain', 'Snow', 'Fog', 'Windy']
TRAFFIC_LEVELS = ['Low', 'Medium', 'High', 'Heavy']
INSURANCE_CLAIMED_OPTIONS = [True, False, 1, 0, 'Yes', 'No', 'Y', 'N', None]
CUSTOMER_RATING_OPTIONS = [1, 2, 3, 4, 5, 0, 6, 'five', 'one', None, 'N/A']

# --- Helper Functions for Messy Data ---

def generate_messy_text(options):
    if random.random() < 0.03: return random.choice([None, "N/A"])
    choice = random.choice(options)
    if random.random() < 0.2:
        case_map = {'upper': str.upper, 'lower': str.lower, 'title': str.title}
        choice = case_map[random.choice(list(case_map.keys()))](choice)
    if random.random() < 0.1: choice = f" {choice} "
    return choice

def generate_messy_date():
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date.today()
    random_date = start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))
    return random_date.strftime(random.choice(['%d/%m/%Y', '%m/%d/%Y', '%d-%b-%y']))

def generate_messy_cost(base_cost):
    if random.random() < 0.15: return random.choice(["unknown", "N/A", None])
    cost = base_cost + random.uniform(-20, 20)
    cost_str = f"{cost:.4f}" if random.random() < 0.15 else f"{cost:.2f}"
    cost_format = random.choice(['plain', 'gbp', 'symbol', 'comma'])
    if cost_format == 'gbp': return f"{cost_str} GBP"
    if cost_format == 'symbol': return f"£{cost_str}"
    if cost_format == 'comma': return f"{int(cost):,}.{cost_str.split('.')[1]}"
    return cost_str

def generate_malformed_email(email):
    return email.replace('@', '', 1) if random.random() < 0.1 else email

def generate_messy_postcode():
    if random.random() < 0.05: return random.choice([None, "N/A"])
    postcode = fake.postcode()
    mess_type = random.random()
    if mess_type < 0.1: return postcode.split(' ')[0]
    if mess_type < 0.2: return postcode.lower()
    return postcode

def generate_messy_mobile():
    if random.random() < 0.1: return random.choice([None, "N/A"])
    base_num = f"07{random.randint(100000000, 999999999)}"
    if random.random() < 0.6: return base_num
    if random.random() < 0.8: return f"{base_num[:5]} {base_num[5:]}"
    return f"+44 {base_num[1:]}"

# --- Pre-generation of Technicians ---
TECHNICIANS = []
used_tech_ids = set()
while len(TECHNICIANS) < 550:
    tech_id = random.randint(1000, 5999)
    if tech_id not in used_tech_ids:
        used_tech_ids.add(tech_id)
        TECHNICIANS.append((tech_id, fake.name(), generate_messy_mobile()))

# --- Main Data Generation Logic ---

def create_dataset(num_rows):
    dataset = []
    
    # --- MODIFICATION START ---
    # Pre-generate unique, shuffled lists for Job IDs and Customer IDs
    job_ids = list(range(100000, 100000 + num_rows))
    random.shuffle(job_ids)

    customer_ids = list(range(1000, 1000 + num_rows))
    random.shuffle(customer_ids)
    # --- MODIFICATION END ---
    
    for i in range(num_rows):
        # --- MODIFICATION START ---
        # Assign a unique ID from the pre-generated lists for each row
        job_id = job_ids[i]
        customer_id = customer_ids[i]
        # --- MODIFICATION END ---
        
        selected_region = random.choice(list(CITY_REGION_MAP.keys()))
        selected_city = random.choice(CITY_REGION_MAP[selected_region])
        
        selected_brand = random.choice(list(VEHICLE_DATA.keys()))
        selected_type = random.choice(list(VEHICLE_DATA[selected_brand].keys()))
        selected_model = random.choice(VEHICLE_DATA[selected_brand][selected_type])
        
        base_repair_cost = random.uniform(100, 2500)
        repair_cost = generate_messy_cost(base_repair_cost)
        glass_cost = generate_messy_cost(base_repair_cost * random.uniform(0.3, 1.1))
        
        profit = None
        if isinstance(repair_cost, str) and isinstance(glass_cost, str):
            try:
                clean_repair = float(re.sub(r'[^\d.]', '', repair_cost))
                clean_glass = float(re.sub(r'[^\d.]', '', glass_cost))
                profit = f"{(clean_repair - clean_glass):.2f}"
            except (ValueError, TypeError):
                profit = "Error"
        
        customer_name = fake.name()
        email_name = customer_name.lower().replace(' ', random.choice(['.', '_']))
        customer_email = f"{email_name}@{fake.free_email_domain()}" if random.random() < 0.9 else fake.email()

        technician_id, technician_name, technician_mobile = random.choice(TECHNICIANS)
        
        dataset.append([
            job_id, generate_messy_date(), generate_messy_text([selected_region]), generate_messy_text([selected_city]),
            random.choice(GARAGE_NAMES), generate_messy_text(GARAGE_TYPES),
            generate_messy_text([selected_brand]), generate_messy_text([selected_type]), generate_messy_text([selected_model]),
            generate_messy_text(GLASS_TYPES), generate_messy_text(WINDOW_POSITIONS), generate_messy_text(DAMAGE_TYPES),
            generate_messy_text(REPAIR_TYPES), repair_cost, glass_cost, profit,
            random.choice(CUSTOMER_RATING_OPTIONS), customer_name, generate_malformed_email(customer_email),
            generate_messy_mobile(), generate_messy_postcode(), random.choice(INSURANCE_CLAIMED_OPTIONS),
            technician_id, technician_name, technician_mobile, 
            # --- MODIFICATION START ---
            customer_id, # Use the unique customer_id
            # --- MODIFICATION END ---
            random.randint(0, 20), generate_messy_text(WEATHER_CONDITIONS), generate_messy_text(TRAFFIC_LEVELS),
            round(random.uniform(1, 5), 1)
        ])
        
    num_duplicates = int(num_rows * 0.01)
    if dataset:
        dataset.extend(random.choices(dataset, k=num_duplicates))
        random.shuffle(dataset)
        
    return dataset

def write_to_csv(dataset, filename):
    headers = [
        'Job_ID', 'Repair_Date', 'Region', 'City', 'Garage_Name', 'Garage_Type',
        'Vehicle_Brand', 'Vehicle_Type', 'Vehicle_Model', 'Glass_Type', 'Window_Position',
        'Damage_Type', 'Repair_Type', 'Repair_Cost', 'Glass_Cost', 'Profit',
        'Customer_Rating', 'Customer_Name', 'Customer_Email', 'Customer_Mobile', 'Customer_Postcode', 
        'Insurance_Claimed', 'Technician_ID', 'Technician_Name', 'Technician_Mobile', 'Customer_ID', 
        'Vehicle_Age_in_Years', 'Weather_Condition', 'Traffic_Level', 'Job_Duration_in_Hours'
    ]
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(dataset)

def generate_and_get_file(num_rows):
    num_rows = int(num_rows)
    print(f"Generating {num_rows} base rows of synthetic data...")
    synthetic_data = create_dataset(num_rows)
    print(f"Total rows including duplicates: {len(synthetic_data)}")
    write_to_csv(synthetic_data, OUTPUT_FILE)
    print(f"Successfully created {OUTPUT_FILE}")
    return OUTPUT_FILE

iface = gr.Interface(
    fn=generate_and_get_file,
    inputs=gr.Number(label="Number of Base Rows to Generate", value=15000, step=1000),
    outputs=gr.File(label="Download Generated CSV"),
    title="Synthetic Autoglass Repair Data Generator",
    description="Specify the number of rows for the dataset and click 'Generate' to create and download the messy, realistic CSV file."
)

if __name__ == "__main__":
    iface.launch()