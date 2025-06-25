import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from directories import Directories
from openpyxl import load_workbook
import shutil

# ---------------------------------------------
# Configuration
# ---------------------------------------------
STATES = True           # Split results by state?
SECTORAL_PLANS = False  # Include sectoral plan mapping?
WIDE_FORMAT = True      # True for wide ("w"), False for long ("l")

# Input filenames
INPUT_FILES = {
    'transport': 'FE_transport.csv',
    'commercial': 'FE_commercial.csv',
    'residential': 'FE_residential.csv',
    'industry': 'FE_industry.csv',
    'power': 'FE_power.csv',
    'emissions': 'CO2 emissions.csv',
    'elec_cap_gen': 'Elec capacity and generation.csv',
    'eneff_ind': 'EnEff Industry.csv',
    'eneff_bld': 'EnEff Buildings.csv',
    'h2': 'H2 capacity and generation.csv'
}

# Directories
dirs = Directories()
INPUT_PATH = Path(dirs.INPUT_PATH)
OUTPUT_PATH = Path(dirs.OUTPUT_PATH)
MAPPING_PATH = Path(dirs.MAPPING_PATH)

# Timestamp
melb_tz = pytz.timezone('Australia/Melbourne')
TIMESTAMP = datetime.now(melb_tz).strftime('%Y-%m-%d_%H-%M')

# Suppress pandas performance warnings
warnings.simplefilter('ignore', category=pd.errors.PerformanceWarning)

# ---------------------------------------------
# Helper Functions
# ---------------------------------------------

def read_mapping(filename, key_col, val_col):
    df = pd.read_csv(MAPPING_PATH / filename)
    return dict(zip(df[key_col], df[val_col]))

# Linear gap-fill of columns (assumes numeric year columns)
def gap_fill(df):
    """Expands df with missing years and interpolates linearly."""
    df = df.copy()

    # Only include columns that are integers (i.e., years)
    year_cols = [col for col in df.columns if isinstance(col, int)]

    if not year_cols:
        # Check if any column names look like years (string but numeric)
        str_years = [col for col in df.columns if str(col).isdigit()]
        df.columns = [int(col) if str(col).isdigit() else col for col in df.columns]
        year_cols = [col for col in df.columns if isinstance(col, int)]

    if not year_cols:
        return df  # Still nothing, exit safely

    year_cols = sorted(year_cols)
    full_years = list(range(year_cols[0], year_cols[-1] + 1))

    for year in full_years:
        if year not in df.columns:
            df[year] = pd.NA

    df = df.sort_index(axis=1)
    df[full_years] = df[full_years].apply(pd.to_numeric, errors='coerce')
    df[full_years] = df[full_years].interpolate(axis=1, limit_direction='both')
    return df

# Sectoral plan mapping logic
def add_sectoral_plan(df):
    def map_row(r):
        s = r['sector']
        sub = r.get('subsector', None)
        subd = r.get('subsector_detail', None)
        if s in ['Power', 'Hydrogen']:
            return 'Electricity and Energy'
        if s in ['Residential buildings', 'Commercial buildings']:
            return 'Built environment'
        if s == 'Transport':
            return 'Electricity and Energy' if sub == 'Other transport' else 'Transport'
        if s == 'Industry':
            if sub in ['Gas Mining', 'Mining']:
                return 'Resources'
            if sub in ['Agriculture', 'Forestry and logging']:
                return 'Agriculture and Land'
            if subd in ['Construction services', 'Construction', 'Refrigeration and AirCon', 'Water supply, sewerage and drainage services']:
                return 'Built environment'
            return 'Industry and Waste'
        if s == 'Carbon dioxide removal':
            return 'Carbon dioxide removal'
        if s == 'Land use sequestration':
            return 'Agriculture and Land'
        return '-'

    df['sectoral_plan_sector'] = df.apply(map_row, axis=1)
    # reorder columns: insert after half
    cols = list(df.columns)
    mid = len(cols) // 2
    new_cols = cols[:mid] + ['sectoral_plan_sector'] + cols[mid:]
    return df[new_cols]

# Convert wide DataFrame (with year columns) to long format
def wide_to_long(df, id_vars):
    return df.reset_index().melt(id_vars=id_vars, var_name='year', value_name='value').dropna()

# Summarize by grouping and pivot
def summarize(df, group_cols, val_col):
    pivot = pd.pivot_table(df, values=val_col, index=group_cols, columns='year', aggfunc='sum', fill_value=0)
    return gap_fill(pivot)

# Create fuel switching summary DataFrame
def summarize_fuel_switching(df, group_cols, value_col):
    switching_df = df.copy()

    # Set start and end fuel from raw columns if not already set
    if 'start_fuel' not in switching_df.columns and 'fuel' in switching_df.columns:
        switching_df['start_fuel'] = switching_df['fuel']
    if 'end_fuel' not in switching_df.columns and 'fuel_override' in switching_df.columns:
        switching_df['end_fuel'] = switching_df['fuel_override']

    # Default to start_fuel where end_fuel is blank or '-'
    switching_df['end_fuel'] = switching_df.apply(
        lambda row: row['start_fuel'] if pd.isna(row['end_fuel']) or row['end_fuel'] == '-' else row['end_fuel'],
        axis=1
    )

    # Always keep all rows regardless of whether switching occurred
    switching_group_cols = group_cols + ['start_fuel', 'end_fuel']
    pivot = pd.pivot_table(
        switching_df,
        values=value_col,
        index=switching_group_cols,
        columns='year',
        aggfunc='sum',
        fill_value=0
    )
    return gap_fill(pivot).reset_index()

# ---------------------------------------------
# Load Mapping Dictionaries
# ---------------------------------------------
map_enduse = read_mapping('enduse_to_subsector_detail_mapping.csv', 'enduse', 'subsector_detail')
map_sd2s = read_mapping('subsector_detail_to_subsector_mapping_v2.csv', 'subsector_detail', 'subsector')
map_sp2s = read_mapping('subsector_p_to_subsector_mapping.csv', 'subsector_p', 'subsector')
map_spc2sd = read_mapping('subsector_p_cca_to_subsector_detail_mapping.csv','subsector_p_cca','subsector_detail')
map_com2et = read_mapping('commodity_to_emission_type_mapping.csv', 'commodity', 'emis_type')
map_t2tech = read_mapping('tech_to_technology_mapping.csv', 'tech', 'technology')
map_pc2td = read_mapping('process_code_to_tech_detail_mapping.csv', 'process_code', 'tech_detail')
map_h2tech = read_mapping('h2_mapping.csv','process','subsector_detail')
map_h2sector = read_mapping('h2_sector_mapping.csv','subsector_detail','subsector')
map_emis = read_mapping('emis_mapping.csv','emission_type','emis_type')
map_emsector = read_mapping('emis_sector_mapping.csv','sector0_process','sector')

# Common grouping columns
common_cols = ['scenario'] + (['state'] if STATES else [])

# ---------------------------------------------
# Processing Modules
# ---------------------------------------------

#Transport
def process_energy_transport():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['transport'])
    df = df.rename(columns={'sector_p': 'sector', 'fuel': 'start_fuel'})
    # For Transport, no switching: end_fuel = start_fuel
    df['end_fuel'] = df['start_fuel']
    df['subsector_detail'] = df['enduse'].map(map_enduse)
    df['subsector'] = df['subsector_detail'].map(map_sd2s)

    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'start_fuel', 'end_fuel', 'unit']
    df_summary = summarize(df, group_cols, 'val')
    df_summary = gap_fill(df_summary).reset_index()

    return df_summary

#Residential
def process_energy_residential():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['residential'])
    df = df.rename(columns={'sector_p': 'sector', 'fuel_switched': 'start_fuel', 'fuel': 'end_fuel'})

    # Default to end_fuel if start_fuel is missing
    df['start_fuel'] = df.apply(
        lambda row: row['end_fuel'] if pd.isna(row['start_fuel']) or row['start_fuel'] == '-' else row['start_fuel'],
        axis=1
    )
    df['subsector_detail'] = df['enduse'].map(map_enduse)
    df['subsector'] = df['subsector_p'].map(map_sp2s)

    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'start_fuel', 'end_fuel', 'unit']
    df_summary = summarize(df, group_cols, 'val')
    df_summary = gap_fill(df_summary).reset_index()

    return df_summary

#Power
def process_energy_power():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['power'])
    df = df.rename(columns={'sector_p': 'sector', 'fuel': 'start_fuel', 'fuel_override': 'end_fuel'})

    # Omit records where fuel is Renewable, Solar, Wind, or Electricity
    fuels_to_exclude = ['Renewable', 'Solar', 'Wind', 'Electricity']
    df = df[~df['start_fuel'].isin(fuels_to_exclude)]

    # Set default end_fuel = start_fuel if not overridden
    df['end_fuel'] = df.apply(
        lambda row: row['start_fuel'] if pd.isna(row['end_fuel']) or row['end_fuel'] == '-' else row['end_fuel'],
        axis=1
    )

    df['subsector'] = df['technology0_process']
    df['subsector_detail'] = df['tech'].map(map_t2tech)

    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'start_fuel', 'end_fuel', 'unit']
    df_summary = summarize(df, group_cols, 'val')
    df_summary = gap_fill(df_summary).reset_index()

    return df_summary

#Commercial
def process_energy_commercial():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['commercial'])
    df = df.rename(columns={'sector_p': 'sector', 'fuel': 'start_fuel', 'fuel_override': 'end_fuel'})
    # Set default end_fuel if blank
    df['end_fuel'] = df.apply(
        lambda row: row['start_fuel'] if pd.isna(row['end_fuel']) or row['end_fuel'] == '-' else row['end_fuel'],
        axis=1
    )
    # Calculate energy intensity (EInt) and energy demand
    df = df.sort_values(by=['scenario', 'state', 'sector', 'subsector_p', 'enduse', 'start_fuel', 'end_fuel', 'unit', 'year'])
    value = 0
    eint_list = []
    for _, row in df.iterrows():
        if row['varbl'] == 'IESTCS_EnInt':
            value = row['val'] / row['val~den'] if row['val~den'] != 0 else 0
            eint_list.append(None)
        elif row['varbl'] == 'IESTCS_Out':
            eint_list.append(value)
        else:
            eint_list.append(None)
    df['EInt'] = eint_list
    df = df[df['varbl'] == 'IESTCS_Out']
    df['energy_demand'] = df['val'] * df['EInt']
    # Map subsector and detail
    df['subsector_detail'] = df['enduse'].map(map_enduse)
    df['subsector'] = df['buildingtype'].map(map_sp2s)
    # Group by start_fuel and end_fuel as per updated schema
    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'start_fuel', 'end_fuel', 'unit']
    df_summary = summarize(df, group_cols, 'energy_demand')
    df_summary = gap_fill(df_summary).reset_index()
    return df_summary

#Industry
def process_energy_industry():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['industry'])
    df = df.rename(columns={'sector_p': 'sector', 'fuel': 'start_fuel', 'fuel_override': 'end_fuel'})
    # Set default end_fuel if blank
    df['end_fuel'] = df.apply(
        lambda row: row['start_fuel'] if pd.isna(row['end_fuel']) or row['end_fuel'] == '-' else row['end_fuel'],
        axis=1
    )
    # Calculate energy intensity (EInt) and energy demand
    df = df.sort_values(by=['scenario', 'state', 'sector', 'subsector_p', 'subsector_c', 'start_fuel', 'end_fuel', 'unit', 'year'])
    value = 0
    eint_list = []
    for _, row in df.iterrows():
        if row['varbl'] == 'IESTCS_EnInt':
            value = row['val'] / row['val~den'] if row['val~den'] != 0 else 0
            eint_list.append(None)
        elif row['varbl'] == 'IESTCS_Out':
            eint_list.append(value)
        else:
            eint_list.append(None)
    df['EInt'] = eint_list
    df = df[df['varbl'] == 'IESTCS_Out']
    df['energy_demand'] = df['val'] * df['EInt']
    # Map subsector and detail
    df['subsector_detail'] = df['subsector_c']
    df['subsector'] = df['subsector_c'].map(map_sd2s)
    # Group by start_fuel and end_fuel as per updated schema
    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'start_fuel', 'end_fuel', 'unit']
    df_summary = summarize(df, group_cols, 'energy_demand')
    df_summary = gap_fill(df_summary).reset_index()
    return df_summary

# ----------------------------------------------
# Generation Modules
# ----------------------------------------------

#Elec capacity and generation
def process_elec_cap_gen():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['elec_cap_gen'])
    df = df.rename(columns={'sector_p':'sector'})
    df['subsector_detail'] = df['tech'].map(map_t2tech)
    df['subsector'] = df['technology0_process']
    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'unit']
    df_summary = summarize(df, group_cols, 'val')
    df_summary = gap_fill(df_summary).reset_index()

    return df_summary

#H2 capacity and generation
def process_h2():
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['h2'])
    df = df.rename(columns={'fuel': 'sector'})
    df['subsector_detail'] = df['process'].map(map_h2tech)
    df['subsector'] = df['subsector_detail'].map(map_h2sector)

    group_cols = common_cols + ['sector', 'subsector', 'subsector_detail', 'unit']
    df_summary = summarize(df, group_cols, 'val')
    df_summary = gap_fill(df_summary).reset_index()
    return df_summary


# ----------------------------------------------
# Emissions Module
# ----------------------------------------------
def process_emis():
    # 1. Load raw data
    df = pd.read_csv(INPUT_PATH / INPUT_FILES['emissions'])

    # 2. Top-level sector mapping
    df['sector'] = df['sector_p'].map(map_emsector)

    # 3. subsector_detail for Industry
    mask_ind = df['sector']=='Industry'

    # 3a. Try subsector_c
    df.loc[mask_ind, 'subsector_detail'] = df.loc[mask_ind, 'subsector_c']

    # 3b. Fallback to subsector_p_cca via your mapping file
    missing = mask_ind & df['subsector_detail'].isin(['-', None, pd.NA])
    df.loc[missing, 'subsector_detail'] = (
    df.loc[missing, 'subsector_p_cca']
      .map(map_spc2sd)                             # use mapping to change name
      .fillna(df.loc[missing, 'subsector_p_cca'])  # if not in mapping, retain name from VEDA
    )

    # 3c. Final fallback label
    still_blank = mask_ind & df['subsector_detail'].isin(['-', None, pd.NA])
    df.loc[still_blank, 'subsector_detail'] = 'Unassigned emissions'

    # 4. subsector for Industry
    df.loc[mask_ind, 'subsector'] = (
    df.loc[mask_ind, 'subsector_detail']
      .map(map_sd2s)
      .fillna('Unassigned emissions')
    )
    
    # 5. Transport & Buildings
    mask_bt = df['sector'].isin(['Transport','Residential buildings','Commercial buildings'])
    df.loc[mask_bt, 'subsector_detail'] = df.loc[mask_bt, 'enduse'].map(map_enduse)
    df.loc[mask_bt, 'subsector']        = df.loc[mask_bt, 'subsector_p'].map(map_sp2s)

    # 6. Carbon removal
    mask_cdr = df['sector']=='Carbon dioxide removal'
    df.loc[mask_cdr & (df['sector_p']=='LU_CO2seq'),
           'subsector_detail'] = 'Land use sequestration'
    df.loc[mask_cdr & (df['sector_p']=='DAC'),
           'subsector_detail'] = 'Direct air capture'
    df.loc[mask_cdr & (df['subsector_detail']=='Land use sequestration'),
           'subsector'] = 'Land'
    df.loc[mask_cdr & (df['subsector_detail']=='Direct air capture'),
           'subsector'] = 'Engineered'
    df.loc[mask_cdr & ~df['subsector_detail'].isin(['Land use sequestration','Direct air capture']),
           'subsector'] = '-'

    # 7. Power & Hydrogen
    df.loc[df['sector']=='Power',    'subsector'] = df['tech']
    df.loc[df['sector']=='Hydrogen', 'subsector'] = df['tech']

    # 8. Emission types
    df['emis_type'] = df['commodity'].map(map_com2et).fillna('-')
    ind = df['sector']=='Industry'
    df.loc[ind & (df['varbl']=='Emi_CO2') & (df['commodity']=='INDCO2N'),
           'emis_type'] = 'Energy'
    df.loc[ind & (df['varbl']=='Emi_CO2') & (df['commodity']=='INDCO2P'),
           'emis_type'] = 'Process'
    df.loc[ind & (df['varbl']=='Cap_CO2'),
           'emis_type'] = 'Capture'
    df.loc[ind & (df['varbl']=='Emi_IndCO2_energy'),
           'emis_type'] = 'Energy'

    # 9. Industry negative/process removals
    proc_neg = (df['sector']=='Industry') & (df['source_p']=='Process Negative Emissions')
    df.loc[proc_neg, ['sector','subsector','subsector_detail','emis_type']] = [
        'Carbon dioxide removal','Engineered','Mineral carbonation','Sequestration'
    ]
    forest_proc = (
        (df['sector']=='Industry') &
        (df['subsector']=='Forestry and logging') &
        (df['emis_type']=='Process')
    )
    df.loc[forest_proc, ['sector','subsector','subsector_detail','emis_type']] = [
        'Carbon dioxide removal','Land','Forestry and logging','Sequestration'
    ]

    # 10. Units
    df['unit'] = 'ktCO2e'

    # 11. Ensure no NaNs in grouping keys
    all_cols = common_cols + ['sector','subsector','subsector_detail','emis_type','unit']
    df[all_cols] = df[all_cols].fillna('-')

     # 12. Pivot & gap-fill
    emis_pivot = pd.pivot_table(
        df, values='val',
        index=common_cols + ['sector','subsector','subsector_detail','emis_type','unit'],
        columns='year',
        aggfunc='sum',
        fill_value=0
    )
    emis_summary = gap_fill(emis_pivot).reset_index()   
    return emis_summary

# TODO: 
# process_energy_efficiency_ind, # process_energy_efficiency_bld.

# ---------------------------------------------
# Main
# ---------------------------------------------
if __name__ == '__main__':
    # Process each module

    ### Energy Use Modules ###
    transport = process_energy_transport()
    print('Transport energy processed')
    commercial = process_energy_commercial()
    print('Commercial energy processed')
    residential = process_energy_residential()
    print('Residential energy processed')
    industry = process_energy_industry()
    print('Industry energy processed')  
    power = process_energy_power()
    print('Power energy processed')
    all_energy = pd.concat([transport, commercial, residential, industry, power], ignore_index=True)
    
    print('All energy use data processed')

    ### Emissions Module ###
    
    emissions = process_emis()
    print('Emissions data processed')

    ### Generation modules ###
    
    h2 = process_h2()
    elec_cap = process_elec_cap_gen()
    print('All generation data processed')

    ### Energy Efficiency ### <- likely defunct due to new structure of IND2
    #eneff_ind = process_energy_efficiency_ind()
    #eneff_bld = process_energy_efficiency_bld()
   

    # Combine and export
    output_dir = OUTPUT_PATH / f'{TIMESTAMP}'
    output_dir.mkdir(parents=True, exist_ok=True)

    all_energy.to_csv(output_dir / 'energy_all_sectors.csv', index=False)
    elec_cap.to_csv(output_dir / 'elec_gen.csv', index=False)
    h2.to_csv(output_dir / 'h2_gen.csv', index=False)
    emissions.to_csv(output_dir / 'emissions.csv', index=False)

    print('All sector energy data combined and exported')
    print('Processing complete')
