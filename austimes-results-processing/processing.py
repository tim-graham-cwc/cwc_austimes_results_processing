from datetime import datetime
import pytz
import pandas as pd
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


### Run options
STATES = "y" #To split results by state, set to "y", otherwise "n"
SECTORAL_PLANS = "n" #To include sectoral plans sector mapping, set to "y", otherwise "n"
WIDE_OR_LONG = "w" #For wide format set to "w", for long set to "l"


### Define data file names
INPUT_TRA_FILENAME = "CORE_Fin energy Transport.csv"
INPUT_COM_FILENAME = "CORE_Fin energy Commercial.csv"
INPUT_RES_FILENAME = "CORE_Fin energy Residential.csv"
INPUT_IND_FILENAME = "CORE_Fin energy Industry.csv"
INPUT_ELC_FILENAME = "Elec fuels.csv"
INPUT_EMIS_FILENAME = "CORE_emis_detail.csv"
INPUT_ELCG_FILENAME = "CORE_Elec capacity and generation.csv"
INPUT_EnEff_IND_FILENAME = "CORE-EnEff Industry.csv"
INPUT_EnEff_BLD_FILENAME = "CORE-EnEff Buildings.csv"
INPUT_H2GC_FILENAME = "CORE_H2 capacity and generation.csv"
INPUT_PATH = "inputs/"
OUTPUT_PATH = "outputs/"
MAPPING_PATH = "mapping/"



### Get current date and time in Melbourne for labelling csv outputs
now = datetime.now()
tz_Melbourne = pytz.timezone('Australia/Melbourne')
datetime_Melbourne = datetime.now(tz_Melbourne)
dt = datetime_Melbourne.strftime("%Y-%m-%d_%H-%M")

### Read files
##Mapping files [To Do - create combined mapping file]
sd_to_s = pd.read_csv(MAPPING_PATH + "subsector_detail_to_subsector_mapping_v2.csv")
sd_to_s_dict = sd_to_s.to_dict('list')
com_to_et = pd.read_csv(MAPPING_PATH + "commodity_to_emission_type_mapping.csv")
com_to_et_dict = com_to_et.to_dict('list')
eu_to_sd = pd.read_csv(MAPPING_PATH + "enduse_to_subsector_detail_mapping.csv")
eu_to_sd_dict = eu_to_sd.to_dict('list')
sp_to_s = pd.read_csv(MAPPING_PATH + "subsector_p_to_subsector_mapping.csv")
sp_to_s_dict = sp_to_s.to_dict('list')
t_to_s = pd.read_csv(MAPPING_PATH + "tech_to_technology_mapping.csv")
t_to_s_dict = t_to_s.to_dict('list')
tech_to_technology = pd.read_csv(MAPPING_PATH + "tech_to_technology_mapping.csv")
tech_to_technology_dict = tech_to_technology.to_dict('list')
pc_to_td =pd.read_csv(MAPPING_PATH + "process_code_to_tech_detail_mapping.csv")
pc_to_td_dict = pc_to_td.to_dict('list')



##Data files
energy_tra = pd.read_csv(INPUT_PATH + INPUT_TRA_FILENAME)
energy_res = pd.read_csv(INPUT_PATH + INPUT_RES_FILENAME)
energy_com = pd.read_csv(INPUT_PATH + INPUT_COM_FILENAME)
energy_ind = pd.read_csv(INPUT_PATH + INPUT_IND_FILENAME)
energy_elc = pd.read_csv(INPUT_PATH + INPUT_ELC_FILENAME)
elec_cap_gen = pd.read_csv(INPUT_PATH + INPUT_ELCG_FILENAME)
eneff_ind = pd.read_csv(INPUT_PATH + INPUT_EnEff_IND_FILENAME)
eneff_bld = pd.read_csv(INPUT_PATH + INPUT_EnEff_BLD_FILENAME)
H2_gen_cap = pd.read_csv(INPUT_PATH + INPUT_H2GC_FILENAME)

### Common functions [To Do - move into class in separate file]
## Function to perform gap filling by linear interpolation
def gap_fill_dataframe(dataframe):
  # Building a dictionary with years which have data
  data_years = dataframe.columns.values.tolist()
  #print(data_years)
  all_years = [x for x in range(data_years[0],data_years[-1]+1)]
  #print(all_years)
  interp_dict = {
    "year": [],
    "years_since":  [],
    "years_to": []
  }
  n = 0
  for year in all_years:
    interp_dict['year'].append(year)
    if year in data_years:
      n = 0
      interp_dict['years_since'].append(n)
    else:
      n += 1
      interp_dict['years_since'].append(n)
  m = 0
  for year in reversed(all_years):
    if year in data_years:
      m = 0
      interp_dict['years_to'].insert(0, m)
    else:
      m += 1
      interp_dict['years_to'].insert(0, m)
    # Interpolating data in emissions summary sheet
  for index, row in dataframe.iterrows():
    n = 0
    for y in interp_dict['year']:
      ys = interp_dict['years_since'][n]
      yt = interp_dict['years_to'][n]
      current_year = y
      next_year = y + yt
      previous_year = y - ys
      if ys != 0:
        step = (dataframe.at[index, next_year] - dataframe.at[index, previous_year]) / (ys + yt)
        dataframe.at[index, current_year] = dataframe.at[index, previous_year] + (step*ys)
        #dataframe.at[index, current_year] = interp_value
      else:
        pass
      n += 1
  dataframe = dataframe[all_years]
  return dataframe

## Function to apply sectoral plan sectors
def add_sectoral_plan_mapping(df):
  columns = df.columns.values.tolist()
  if 'subsector' in columns:
    for index, row in df.iterrows():

      if row['sector'] == "Power" or row['sector'] == "Hydrogen":
        df.at[index, 'sectoral_plan_sector'] = "Electricity and Energy"
      elif row['sector'] == "Residential buildings" or row['sector'] == "Commercial buildings":
        df.at[index, 'sectoral_plan_sector'] = "Built environment"
      elif row['sector'] == "Transport":
        if row['subsector'] == "Other transport":
          df.at[index, 'sectoral_plan_sector'] = "Electricity and Energy"
        else:
          df.at[index, 'sectoral_plan_sector'] = "Transport"
      elif row['sector'] == "Carbon dioxide removal":
        df.at[index, 'sectoral_plan_sector'] = "Carbon dioxide removal"
      elif row['sector'] == "Industry":
        if row['subsector'] == "Gas Mining" or row['subsector'] == "Mining":
          df.at[index, 'sectoral_plan_sector'] = "Resources"
        elif row['subsector'] == "Agriculture" or row['subsector'] == "Forestry and logging":
          df.at[index, 'sectoral_plan_sector'] = "Agriculture and Land"
        elif row['subsector_detail'] == "Construction services" or row['subsector_detail'] == "Construction" or row['subsector_detail'] == "Refridgeration and AirCon" or row['subsector_detail'] == "Water supply, sewerage and drainage services":
          df.at[index, 'sectoral_plan_sector'] = "Built environment"
        else:
          df.at[index, 'sectoral_plan_sector'] = "Industry and Waste"
      elif  row['sector'] == "Land use sequestration":
        df.at[index, 'sectoral_plan_sector'] = "Agriculture and Land"
      else:
        df.at[index, 'sectoral_plan_sector'] = "-"
  else:

    for index, row in df.iterrows():
      if row['sector'] == "Power":
        df.at[index, 'sectoral_plan_sector'] = "Electricity and Energy"
      else:
        df.at[index, 'sectoral_plan_sector'] = "-"

  col_vars = [x for x in columns if isinstance(x, str)]
  col_vals = [x for x in columns if isinstance(x, int)]
  columns_new = col_vars + ['sectoral_plan_sector'] + col_vals
  df = df[columns_new]

  return df

## Function to convert wide to long format
def wide_to_long(df):
  columns = df.columns.values.tolist()
  col_vars = [x for x in columns if isinstance(x, str)]
  col_vals = [x for x in columns if isinstance(x, int)]

  df = pd.melt(df, id_vars=col_vars, value_vars = col_vals)
  df = df.dropna(subset=['value'])
  return df



### Common columns for dataframes
common_cols = ['scenario']
if STATES == 'y':
    common_cols.append('state')



### Energy Processing - Transport
# Get name of input to label output
input_trans_filename = INPUT_TRA_FILENAME.split(".")[0]

## Mapping input csv to output sector categories
#Renaming columns sector_p to sector and enduse to subsector_detail and fuel to fuel_type
energy_tra = energy_tra.rename(columns={"sector_p": "sector", "fuel": "fuel_type"})

# Defining subsector detail based on enduse
for index, row in energy_tra.iterrows():
  i = eu_to_sd_dict['enduse'].index(energy_tra.at[index, "enduse"])
  o = eu_to_sd_dict['subsector_detail'][i]
  energy_tra.at[index, "subsector_detail"] = o

# Defining subsector based on subsector detail
energy_tra["subsector"] = ""
for index, row in energy_tra.iterrows():
  i = sd_to_s_dict['subsector_detail'].index(energy_tra.at[index, "subsector_detail"])
  o = sd_to_s_dict['subsector'][i]
  energy_tra.at[index, "subsector"] = o

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','fuel_type','unit','year']
energy_sum_trans = energy_tra.groupby(cols, sort=True)['val'].sum()
energy_sum_trans_df = energy_sum_trans.to_frame()
energy_summary_trans = pd.pivot_table(energy_sum_trans_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

## Gap fill year data using linear interpolation
energy_summary_trans = gap_fill_dataframe(energy_summary_trans)
energy_summary_trans = energy_summary_trans.reset_index()
print("Transport energy results processed")


### Energy Processing - Commercial
## Get name of input to label output
input_com_filename = INPUT_COM_FILENAME.split(".")[0]

## Mapping input csv to output sector categories
# Setting sector names
energy_com["sector"] = "Commercial buildings"

# Setting fuel_type
for index, row in energy_com.iterrows():
  if energy_com.at[index, "fuel_override"] == "-":
    energy_com.at[index, "fuel_type"] = energy_com.at[index, "fuel"]
  else:
    energy_com.at[index, "fuel_type"] = energy_com.at[index, "fuel_override"]

# Add start fuel and end fuel for fuel switching processing
energy_com["start_fuel"] = energy_com["fuel"]
energy_com["end_fuel"] = energy_com["fuel_override"]

# Mapping subsector
energy_com["subsector"] = ""
for index, row in energy_com.iterrows():
  i = sp_to_s_dict['subsector_p'].index(energy_com.at[index, "buildingtype"])
  o = sp_to_s_dict['subsector'][i]
  energy_com.at[index, "subsector"] = o

# Mapping subsector_detail
energy_com["subsector_detail"] = ""
for index, row in energy_com.iterrows():
  i = eu_to_sd_dict['enduse'].index(energy_com.at[index, "enduse"])
  o = eu_to_sd_dict['subsector_detail'][i]
  energy_com.at[index, "subsector_detail"] = o

# Set units
energy_com["unit"] = "PJ"

# Calculate energy demand optimised
## Sort
columns = energy_com.columns.values.tolist()
remove_cols = ['val', 'val~den', 'varbl']
sort_columns = [x for x in columns if x not in remove_cols]
energy_com = energy_com.sort_values(by=sort_columns)


## Calculate Eint
value = 0
for index, row in energy_com.iterrows():
  if energy_com.at[index, "varbl"] == "IESTCS_EnInt":
    value = energy_com.at[index, "val"]/energy_com.at[index, "val~den"]
  elif energy_com.at[index, "varbl"] == "IESTCS_Out":
    energy_com.at[index, "EInt"] = value
    pass

# Drop rows where data does not represent energy demand
energy_com = energy_com.drop(energy_com[energy_com.varbl == "IESTCS_EnInt"].index)

# Calculate energy demand
energy_com["energy_demand"] = energy_com["val"]*energy_com["EInt"]

# Create another dataframe for fuel switching processing
energy_com_fs = energy_com

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','fuel_type','unit','year']
energy_sum_com = energy_com.groupby(cols, sort=True)['energy_demand'].sum()
energy_sum_com_df = energy_sum_com.to_frame()
energy_summary_com = pd.pivot_table(energy_sum_com_df, values='energy_demand',index=cols[:-1], columns='year', aggfunc='sum')


## Gap fill year data using linear interpolation
energy_summary_com = gap_fill_dataframe(energy_summary_com)
energy_summary_com = energy_summary_com.reset_index()

## Creating fuel switching output
# Remove all rows that do not represent fuel switching
energy_com_fs = energy_com_fs.drop(energy_com_fs[energy_com_fs.start_fuel == energy_com_fs.end_fuel].index)
energy_com_fs = energy_com_fs.drop(energy_com_fs[energy_com_fs.end_fuel == "-"].index)
# Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','start_fuel','end_fuel','unit','year']
energy_com_fs = energy_com_fs.groupby(cols, sort=True)['energy_demand'].sum()
energy_com_fs_df = energy_com_fs.to_frame()
energy_summary_com_fs = pd.pivot_table(energy_com_fs_df, values='energy_demand',index=cols[:-1], columns='year', aggfunc='sum')
# Gap fill year data using linear interpolation
energy_summary_com_fs = gap_fill_dataframe(energy_summary_com_fs)
energy_summary_com_fs = energy_summary_com_fs.reset_index()
print("Commercial energy results processed")


### Energy Processing - Residential
## Get name of input to label ouput
input_res_filename = INPUT_RES_FILENAME.split(".")[0]

## Mapping input csv to output sector categories
# Setting sector names
energy_res["sector"] = "Residential buildings"

# Add start fuel and end fuel for fuel switching processing
energy_res["start_fuel"] = energy_res["fuel_switched"]
energy_res["end_fuel"] = energy_res["fuel"]

# Rename column fuel to fuel_type
energy_res = energy_res.rename(columns={"fuel": "fuel_type"})

## Residential
# Mapping subsector
energy_res["subsector"] = ""
for index, row in energy_res.iterrows():
  i = sp_to_s_dict['subsector_p'].index(energy_res.at[index, "subsector_p"])
  o = sp_to_s_dict['subsector'][i]
  energy_res.at[index, "subsector"] = o

# Mapping subsector_detail
energy_res["subsector_detail"] = ""
for index, row in energy_res.iterrows():
  i = eu_to_sd_dict['enduse'].index(energy_res.at[index, "enduse"])
  o = eu_to_sd_dict['subsector_detail'][i]
  energy_res.at[index, "subsector_detail"] = o

# Create another dataframe for fuel switching processing
energy_res_fs = energy_res

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','fuel_type','unit','year']
energy_sum_res = energy_res.groupby(cols, sort=True)['val'].sum()
energy_sum_res_df = energy_sum_res.to_frame()
energy_summary_res = pd.pivot_table(energy_sum_res_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

## Gap fill year data using linear interpolation
energy_summary_res = gap_fill_dataframe(energy_summary_res)
energy_summary_res = energy_summary_res.reset_index()

## Creating fuel switching output
# Remove all rows that do not represent fuel switching
energy_res_fs = energy_res_fs.drop(energy_res_fs[energy_res_fs.start_fuel == energy_res_fs.end_fuel].index)
energy_res_fs = energy_res_fs.drop(energy_res_fs[energy_res_fs.end_fuel == "-"].index)
energy_res_fs = energy_res_fs.drop(energy_res_fs[energy_res_fs.start_fuel == "-"].index)
# Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','start_fuel','end_fuel','unit','year']
energy_res_fs = energy_res_fs.groupby(cols, sort=True)['val'].sum()
energy_res_fs_df = energy_res_fs.to_frame()
energy_summary_res_fs = pd.pivot_table(energy_res_fs_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

# Gap fill year data using linear interpolation
energy_summary_res_fs = gap_fill_dataframe(energy_summary_res_fs)
energy_summary_res_fs = energy_summary_res_fs.reset_index()
print("Residential energy results processed")


### Energy Processing - Industry
## Get name of input to label output
input_ind_filename = INPUT_IND_FILENAME.split(".")[0]

## Mapping input csv to ouput sector categories
# Setting sector names
energy_ind["sector"] = "Industry"

# Setting subsector detail
energy_ind = energy_ind.rename(columns={'fuel': 'start_fuel', 'fuel_override': 'end_fuel', 'subsector_c': 'subsector_detail'})

# Setting subsector
for index, row in energy_ind.iterrows():
  i = sd_to_s_dict['subsector_detail'].index(energy_ind.at[index, "subsector_detail"])
  o = sd_to_s_dict['subsector'][i]
  energy_ind.at[index, "subsector"] = o

# Setting fuel_type for energy processing
for index, row in energy_ind.iterrows():
  if (energy_ind.at[index, "end_fuel"] == "-"):
    energy_ind.at[index, "fuel_type"] = energy_ind.at[index, "start_fuel"]
  else:
    energy_ind.at[index, "fuel_type"] = energy_ind.at[index, "end_fuel"]

# Set units
energy_ind["unit"] = "PJ"

# Calculate energy demand optimised
## Sort
columns = energy_ind.columns.values.tolist()
remove_cols = ['val', 'val~den', 'varbl']
sort_columns = [x for x in columns if x not in remove_cols]
energy_ind = energy_ind.sort_values(by=sort_columns)

## Calculate Eint
value = 0
for index, row in energy_ind.iterrows():
  if energy_ind.at[index, "varbl"] == "IESTCS_EnInt":
    value = energy_ind.at[index, "val"]/energy_ind.at[index, "val~den"]
  elif energy_ind.at[index, "varbl"] == "IESTCS_Out":
    energy_ind.at[index, "EInt"] = value
    pass

# Drop rows where data does not represent energy demand
energy_ind = energy_ind.drop(energy_ind[energy_ind.varbl == "IESTCS_EnInt"].index)

# Calculate energy demand
energy_ind["energy_demand"] = energy_ind["val"]*energy_ind["EInt"]

# Create another dataframe for fuel switching processing
energy_ind_fs = energy_ind

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail',
                      #'process', #
                      'fuel_type','unit','year']
energy_sum_ind = energy_ind.groupby(cols, sort=True)['energy_demand'].sum()
energy_sum_ind_df = energy_sum_ind.to_frame()
energy_summary_ind = pd.pivot_table(energy_sum_ind_df, values='energy_demand',index=cols[:-1], columns='year', aggfunc='sum')

## Gap fill year data using linear interpolation
energy_summary_ind = gap_fill_dataframe(energy_summary_ind)
energy_summary_ind = energy_summary_ind.reset_index()

## Creating fuel switching output
# Remove all rows that do not represent fuel switching
energy_ind_fs = energy_ind_fs.drop(energy_ind_fs[energy_ind_fs.start_fuel == energy_ind_fs.end_fuel].index)
energy_ind_fs = energy_ind_fs.drop(energy_ind_fs[energy_ind_fs.end_fuel == "-"].index)
# Sum over years
cols = common_cols + ['sector','subsector','subsector_detail',
                      #'process',
                      'start_fuel','end_fuel','unit','year']
energy_ind_fs = energy_ind_fs.groupby(cols, sort=True)['energy_demand'].sum()
energy_ind_fs_df = energy_ind_fs.to_frame()
energy_summary_ind_fs = pd.pivot_table(energy_ind_fs_df, values='energy_demand',index=cols[:-1], columns='year', aggfunc='sum')
# Gap fill year data using linear interpolation
energy_summary_ind_fs = gap_fill_dataframe(energy_summary_ind_fs)
energy_summary_ind_fs = energy_summary_ind_fs.reset_index()
print("Industry energy results processed")


### Energy processing - Electricity
## Get name of input to label ouput
input_elc_filename = INPUT_ELC_FILENAME.split(".")[0]

## Set sector names
energy_elc['sector'] = "Power"

## Drop rows that do not represent input fuels (i.e. renewables and storage)
energy_elc = energy_elc.drop(energy_elc[energy_elc.fuel == "Electricity"].index)
energy_elc = energy_elc.drop(energy_elc[energy_elc.fuel == "Renewable"].index)
energy_elc = energy_elc.drop(energy_elc[energy_elc.fuel == "Solar"].index)
energy_elc = energy_elc.drop(energy_elc[energy_elc.fuel == "Wind"].index)

## Set subsector names
for index, row in energy_elc.iterrows():
  i = t_to_s_dict['tech'].index(energy_elc.at[index, "tech"])
  o = t_to_s_dict['technology'][i]
  energy_elc.at[index, "subsector"] = o

## Rename fuel to fuel_type
energy_elc = energy_elc.rename(columns={"fuel": "fuel_type"})
energy_elc['subsector_detail']=""

## Calculate energy demand in PJ and rename units
energy_elc['energy_demand'] = energy_elc['val']*3.6
energy_elc['unit'] = 'PJ'

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','fuel_type','unit','year']
energy_sum_elc = energy_elc.groupby(cols, sort=True)['energy_demand'].sum()
energy_sum_elc_df = energy_sum_elc.to_frame()
energy_summary_elc = pd.pivot_table(energy_sum_elc_df, values='energy_demand',index=cols[:-1], columns='year', aggfunc='sum')

## Gap fill year data using linear interpolation
energy_summary_elc = gap_fill_dataframe(energy_summary_elc)
energy_summary_elc = energy_summary_elc.reset_index()
print("Electricity energy results processed")


### Emissions
## Get name of input to label ouput
input_filename = INPUT_EMIS_FILENAME.split(".")[0]

## Reading input files
# Read veda report file
core_emis_detail = pd.read_csv(INPUT_PATH + INPUT_EMIS_FILENAME)
core_emis_detail["sector"] = ""
core_emis_detail["subsector"] = ""
core_emis_detail["subsector_detail"] = ""
core_emis_detail["emis_type"] = ""

## Mapping input csv to ouput sector categories
#Setting sector values
for index, row in core_emis_detail.iterrows():
  if row['sector_p'] == "LU_CO2seq" or row['sector_p'] == "DAC":
    core_emis_detail.at[index, "sector"] = "Carbon dioxide removal"
  elif row['sector_p'] == "Residential":
    core_emis_detail.at[index, "sector"] = "Residential buildings"
  elif row['sector_p'] == "Commercial":
    core_emis_detail.at[index, "sector"] = "Commercial buildings"
  else:
    core_emis_detail.at[index, "sector"] = core_emis_detail.at[index, "sector_p"]

# Setting subsector detail values
for index, row in core_emis_detail.iterrows():
  if row['sector'] == "Industry":
    if row['subsector_p'] == "-" and row['subsector_c'] == "-":
      if row['commodity'] == "UC_Bld_LU_CO2seq-":
        core_emis_detail.at[index, "subsector_detail"] = "UC_Bld_LU_CO2seq-"
      else:
        core_emis_detail.at[index, "subsector_detail"] = "Unassigned energy emissions"
    elif row['subsector_p'] == "-" and row['subsector_c'] != "-":
      core_emis_detail.at[index, "subsector_detail"] = core_emis_detail.at[index, "subsector_c"]
    else:
      core_emis_detail.at[index, "subsector_detail"] = core_emis_detail.at[index, "subsector_p"]
  elif row['sector'] == "Transport" or row['sector'] == "Residential buildings" or row['sector'] == "Commercial buildings":
    i = eu_to_sd_dict['enduse'].index(core_emis_detail.at[index, "enduse"])
    o = eu_to_sd_dict['subsector_detail'][i]
    core_emis_detail.at[index, "subsector_detail"] = o
  elif row['sector'] == "Carbon dioxide removal":
    if row['sector_p'] == "LU_CO2seq":
      core_emis_detail.at[index, "subsector_detail"] = "Land use sequestration"
    elif row['sector_p'] == "DAC":
      core_emis_detail.at[index, "subsector_detail"] = "Direct air capture"
    else:
      core_emis_detail.at[index, "subsector_detail"] = "-"
  else:
    core_emis_detail.at[index, "subsector_detail"] = "-"

# Setting subsector values
for index, row in core_emis_detail.iterrows():
  if row['sector'] == "Residential buildings" or  row['sector'] == "Commercial buildings":
    i = sp_to_s_dict['subsector_p'].index(core_emis_detail.at[index, "subsector_p"])
    o = sp_to_s_dict['subsector'][i]
    core_emis_detail.at[index, "subsector"] = o
  elif row['sector'] == "Hydrogen":
    core_emis_detail.at[index, "subsector"] = core_emis_detail.at[index, "tech"]
  elif row['sector'] == "Carbon dioxide removal":
    if row['subsector_detail'] == "Land use sequestration":
      core_emis_detail.at[index, "subsector"] = "Land"
    elif row['subsector_detail'] == "Direct air capture":
      core_emis_detail.at[index, "subsector"] = "Engineered"
    else:
      core_emis_detail.at[index, "subsector"] = "-"
  elif row['sector'] == "Power":
    core_emis_detail.at[index, "subsector"] = core_emis_detail.at[index, "tech"]
  elif row['sector'] == "Transport" or row['sector'] == "Industry":
    i = sd_to_s_dict['subsector_detail'].index(core_emis_detail.at[index, "subsector_detail"])
    o = sd_to_s_dict['subsector'][i]
    core_emis_detail.at[index, "subsector"] = o
  elif row['sector'] == "Carbon dioxide removal" and row['sector_p'] == "DAC":
    core_emis_detail.at[index, "subsector"] = "Direct air capture"
  else:
    core_emis_detail.at[index, "subsector"] = "-"

# Setting emission types
for index, row in core_emis_detail.iterrows():
  if row['sector'] == "Industry":
    if row['varbl'] == "Emi_CO2":
      if row['commodity'] == "INDCO2N":
        core_emis_detail.at[index, "emis_type"] = "Energy"
      elif row['commodity'] == "INDCO2P":
        core_emis_detail.at[index, "emis_type"] = "Process"
      else:
        pass
    elif row['varbl'] == "Cap_CO2":
      core_emis_detail.at[index, "emis_type"] = "Capture"
    elif row['varbl'] == "Emi_IndCO2_energy":
      core_emis_detail.at[index, "emis_type"] = "Energy"
  else:
    i = com_to_et_dict['commodity'].index(core_emis_detail.at[index, "commodity"])
    o = com_to_et_dict['emis_type'][i]
    core_emis_detail.at[index, "emis_type"] = o

# Drop Unassigned industry energy emissions as these represent duplicate
core_emis_detail = core_emis_detail.drop(core_emis_detail[core_emis_detail.subsector_detail == "Unassigned energy emissions"].index)

##Industry reclassifications
# Reclassifying mineral carbonation and forestry and logging as carbon dioxide removal subsectors
for index, row in core_emis_detail.iterrows():
  if (row['sector'] == "Industry") and (row['source'] == "Process Negative Emissions"):
    core_emis_detail.at[index, 'sector'] = "Carbon dioxide removal"
    core_emis_detail.at[index, 'subsector'] = "Engineered"
    core_emis_detail.at[index, 'subsector_detail'] = "Mineral carbonation"
  elif (row['sector'] == "Industry") and (row['subsector'] == "Forestry and logging") and (row['emis_type'] == "Process"):
    core_emis_detail.at[index, 'sector'] = "Carbon dioxide removal"
    core_emis_detail.at[index, 'subsector'] = "Land"
    core_emis_detail.at[index, 'subsector_detail'] = "Forestry and logging"
    core_emis_detail.at[index, 'emis_type'] = 'Sequestration'

core_emis_detail['units'] = "ktCO2e"

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','emis_type','units','year']
e_sum = core_emis_detail.groupby(cols, sort=True)['val'].sum()
e_sum_df = e_sum.to_frame()
emis_summary = pd.pivot_table(e_sum_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

emis_summary = gap_fill_dataframe(emis_summary)
emis_summary = emis_summary.reset_index()
print("Emissions results processed")



### Electricity generation and capacity
# Get name of input to label ouput
input_filename = INPUT_ELCG_FILENAME.split(".")[0]

# Rename sector_p column to sector (all results in csv are for power)
elec_cap_gen = elec_cap_gen.rename(columns={"sector_p": "sector"})

# Mapping tech
for index, row in elec_cap_gen.iterrows():
  i = tech_to_technology_dict['tech'].index(elec_cap_gen.at[index, "tech"])
  o = tech_to_technology_dict['technology'][i]
  elec_cap_gen.at[index, "technology"] = o

# Mapping tech detail
for index, row in elec_cap_gen.iterrows():
  process = elec_cap_gen.at[index, "process"]
  process_code = ""
  if "_" in process:
    process = process.split("_")[1] # Remove prefix
  else:
    pass
  if "-" in process:
    process = process.split("-")[0] # Remove anything after dash
  else:
    pass
  for char in process: # Strip process of digits
    if (char.isdigit() == True):
      pass
    else:
      process_code += char

  if process_code in pc_to_td_dict['process_code']:
    i = pc_to_td_dict['process_code'].index(process_code)
    o = pc_to_td_dict['tech_detail'][i]
    elec_cap_gen.at[index, "technology_detail"] = o
  else:
    elec_cap_gen.at[index, "technology_detail"] = "-"
  elec_cap_gen.at[index, "process_code"] = process_code

## Sum over years
cols = common_cols + ['sector','technology','technology_detail','unit','year']
elec_sum_cap_gen = elec_cap_gen.groupby(cols, sort=True)['val'].sum()
elec_sum_cap_gen_df = elec_sum_cap_gen.to_frame()
elec_summary_cap_gen = pd.pivot_table(elec_sum_cap_gen_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

## Gap fill year data using linear interpolation
elec_summary_cap_gen = gap_fill_dataframe(elec_summary_cap_gen)
elec_summary_cap_gen = elec_summary_cap_gen.reset_index()
print("Electricity gen/cap results processed")


### Energy efficiency processing
## Get name of input to label ouput
input_ind_filename = INPUT_EnEff_IND_FILENAME.split(".")[0]
input_bld_filename = INPUT_EnEff_BLD_FILENAME.split(".")[0]

## Mapping input csv to ouput sector categories
# Mapping sectors
eneff_ind["sector"] = "Industry" #All entries in core eneff ind are industry (may want to split out ag as own sector in future)

for index, row in eneff_bld.iterrows():
  if eneff_bld.at[index, "sector_p"] == "Commercial":
    eneff_bld.at[index, "sector"] = "Commercial buildings"
  elif eneff_bld.at[index, "sector_p"] == "Residential":
    eneff_bld.at[index, "sector"] = "Residential buildings"
  else:
    eneff_bld.at[index, "sector"] = "-"


# Mapping subsector detail
eneff_ind = eneff_ind.rename(columns={"subsector_p": "subsector_detail"})

for index, row in eneff_bld.iterrows():
  #print(eneff_bld.at[index, "enduse_c"])
  i = eu_to_sd_dict['enduse'].index(eneff_bld.at[index, "enduse_c"])
  o = eu_to_sd_dict['subsector_detail'][i]
  eneff_bld.at[index, "subsector_detail"] = o

# Mapping subsector
for index, row in eneff_ind.iterrows():
  i = sd_to_s_dict['subsector_detail'].index(eneff_ind.at[index, "subsector_detail"])
  o = sd_to_s_dict['subsector'][i]
  eneff_ind.at[index, "subsector"] = o

for index, row in eneff_bld.iterrows():
  i = sp_to_s_dict['subsector_p'].index(eneff_bld.at[index, "buildingtype"])
  o = sp_to_s_dict['subsector'][i]
  eneff_bld.at[index, "subsector"] = o

# Mapping fuel
eneff_ind = eneff_ind.rename(columns={"fuel": "fuel_type"})
eneff_bld = eneff_bld.rename(columns={"fuel": "fuel_type"})

# Mapping efficiency type
eneff_ind = eneff_ind.rename(columns={"source": "efficiency_type"})
eneff_bld['efficiency_type'] = "-" # No efficiency type provided in Core EnEff buildings

# Mapping efficiency type
for index, row in eneff_bld.iterrows():
  if eneff_bld.at[index, "ee_category"] == "EE":
    eneff_bld.at[index, "efficiency_category"] = "Commercial general"
  elif eneff_bld.at[index, "ee_category"] == "EE new":
    eneff_bld.at[index, "efficiency_category"] = "Residential new"
  elif eneff_bld.at[index, "ee_category"] == "EE existing":
    eneff_bld.at[index, "efficiency_category"] = "Residential existing"
  else:
    eneff_bld.at[index, "efficiency_category"] = "-"

for index, row in eneff_ind.iterrows():
  if eneff_ind.at[index, "ee_category"] == "Frontier levers":
    eneff_ind.at[index, "efficiency_category"] = "Frontier levers"
  elif eneff_ind.at[index, "ee_category"] == "EE 1":
    eneff_ind.at[index, "efficiency_category"] = "Process improvements"
  elif eneff_ind.at[index, "ee_category"] == "EE 2":
    eneff_ind.at[index, "efficiency_category"] = "Small equipment upgrades"
  elif eneff_ind.at[index, "ee_category"] == "EE 3":
    eneff_ind.at[index, "efficiency_category"] = "Major equipment upgrades"
  elif eneff_ind.at[index, "ee_category"] == "ETI":
    eneff_ind.at[index, "efficiency_category"] = "ETI upgrade"
  else:
    eneff_ind.at[index, "efficiency_category"] = "-"

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','fuel_type','efficiency_category','efficiency_type','unit','year']
eneff_sum_ind = eneff_ind.groupby(cols, sort=True)['val'].sum()
energy_sum_ind_df = eneff_sum_ind.to_frame()
eneff_summary_ind = pd.pivot_table(energy_sum_ind_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

eneff_sum_bld = eneff_bld.groupby(cols, sort=True)['val'].sum()
energy_sum_bld_df = eneff_sum_bld.to_frame()
eneff_summary_bld = pd.pivot_table(energy_sum_bld_df, values='val',index=cols[:-1], columns='year', aggfunc='sum')

# Combine dataframes
eneff_summary = pd.concat([eneff_summary_ind, eneff_summary_bld], axis=0)


## Gap fill year data using linear interpolation
eneff_summary = gap_fill_dataframe(eneff_summary)
eneff_summary = eneff_summary.reset_index()
print("Energy efficiency results processed")



### H2 generation and capacity
## Get name of input to label output
input_filename = INPUT_H2GC_FILENAME.split(".")[0]

## Mapping input csv to output sector categories
# Mapping sectors
H2_gen_cap["sector"] = H2_gen_cap["sector_p"]

# Mapping subsector and subsector detail
H2_gen_cap['subsector'] = ""
H2_gen_cap['subsector_detail'] = ""
for index, row in H2_gen_cap.iterrows():
  p = row['process'].split("_")[1] + "_" + row['process'].split("_")[2]
  #print(p)
  if p == "SMR_ccs":
    H2_gen_cap.at[index, "subsector"] = "Steam methane reforming"
    H2_gen_cap.at[index, "subsector_detail"] = "Gas-SMR with CCS"
  elif p == "elec_AE":
    H2_gen_cap.at[index, "subsector"] = "Electrolysis"
    H2_gen_cap.at[index, "subsector_detail"] = "Alkaline water electrolysis"
  elif p == "elec_PEM":
    H2_gen_cap.at[index, "subsector"] = "Electrolysis"
    H2_gen_cap.at[index, "subsector_detail"] = "Proton exchange membrane electrolysis"
  else:
    H2_gen_cap.at[index, "subsector"] = "-"
    H2_gen_cap.at[index, "subsector_detail"] = "-"

## Rename value column
H2_gen_cap = H2_gen_cap.rename(columns={"GrandTotal": "val"})

## Sum over years
cols = common_cols + ['sector','subsector','subsector_detail','unit','year']
H2_gen_cap_sum = H2_gen_cap.groupby(by=cols, as_index=False, sort=True)['val'].sum()
H2_gen_cap_summary = pd.pivot_table(H2_gen_cap_sum, values='val',index=cols[:-1], columns='year', aggfunc='sum')
## Gap fill data
H2_gen_cap_summary = gap_fill_dataframe(H2_gen_cap_summary)
H2_gen_cap_summary = H2_gen_cap_summary.reset_index()
print("Hydrogen gen/cap results processed")


### Applying user defined options
if SECTORAL_PLANS == "y":
  energy_summary_trans = add_sectoral_plan_mapping(energy_summary_trans)
  energy_summary_com = add_sectoral_plan_mapping(energy_summary_com)
  energy_summary_res = add_sectoral_plan_mapping(energy_summary_res)
  energy_summary_ind = add_sectoral_plan_mapping(energy_summary_ind)
  energy_summary_elc = add_sectoral_plan_mapping(energy_summary_elc)
  energy_summary_com_fs = add_sectoral_plan_mapping(energy_summary_com_fs)
  energy_summary_res_fs = add_sectoral_plan_mapping(energy_summary_res_fs)
  energy_summary_ind_fs = add_sectoral_plan_mapping(energy_summary_ind_fs)
  emis_summary = add_sectoral_plan_mapping(emis_summary)
  elec_summary_cap_gen = add_sectoral_plan_mapping(elec_summary_cap_gen)
  eneff_summary = add_sectoral_plan_mapping(eneff_summary)
  H2_gen_cap_summary = add_sectoral_plan_mapping(H2_gen_cap_summary)
  print("Sectoral mapping applied")
else:
  pass

## Combine energy and fuel switching dataframes
combined_energy = pd.concat([energy_summary_trans, energy_summary_com, energy_summary_res, energy_summary_ind, energy_summary_elc], axis=0)
combined_fuelswitch = pd.concat([energy_summary_com_fs, energy_summary_res_fs, energy_summary_ind_fs])

## Format
if WIDE_OR_LONG == "l":
  combined_energy = wide_to_long(combined_energy)
  combined_fuelswitch = wide_to_long(combined_fuelswitch)
  emis_summary = wide_to_long(emis_summary)
  elec_summary_cap_gen = wide_to_long(elec_summary_cap_gen)
  eneff_summary = wide_to_long(eneff_summary)
  H2_gen_cap_summary = wide_to_long(H2_gen_cap_summary)
else:
  pass
print("Results processing complete")


### Exporting files
from pathlib import Path

# Specify the nested directory structure
nested_directory_path = Path("outputs/" + dt)

# Create nested directories
nested_directory_path.mkdir(parents=True, exist_ok=True)
print(f"Files will be exported to '{nested_directory_path}'.")

output_path = OUTPUT_PATH + nested_directory_path.name + "/"

## Export to combined Excel file
with pd.ExcelWriter(output_path +  "Combined_results" + ".xlsx") as writer:

    # use to_excel function and specify the sheet_name and index
    # to store the dataframe in specified sheet

    combined_energy.to_excel(writer, sheet_name="Energy", index=False)
    combined_fuelswitch.to_excel(writer, sheet_name="Fuel switching", index=False)
    emis_summary.to_excel(writer, sheet_name="Emissions", index=False)
    elec_summary_cap_gen.to_excel(writer, sheet_name="Elec gen cap", index=False)
    eneff_summary.to_excel(writer, sheet_name="Energy efficiency", index=False)
    H2_gen_cap_summary.to_excel(writer, sheet_name="H2 gen cap", index=False)

#Export csvs
combined_energy.to_csv(output_path +  "energy" + ".csv", index=False)
combined_fuelswitch.to_csv(output_path +  "fuel-switch" + ".csv", index=False)
emis_summary.to_csv(output_path +  "emissions" + ".csv", index=False)
elec_summary_cap_gen.to_csv(output_path +  "electricity-gen-cap" + ".csv", index=False)
eneff_summary.to_csv(output_path +  "energy-efficiency" + ".csv", index=False)
H2_gen_cap_summary.to_csv(output_path +  "hydrogen-generation-capacity" + ".csv", index=False)

print("All files exported")