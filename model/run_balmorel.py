from gamspy import Container
import os
import argparse
import multiprocessing as mp
from datetime import timedelta
import time 
from copy import copy
from create_samples import lhc_sampler
import pandas as pd

def get_arg():
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument('--nb_scen', type=int, help='Number of scenarios (integer)')
    parser.add_argument('--input_sample', type=str, help='Name of the input sampling csv file (str)')
    parser.add_argument('--nb_cores', type=int, help='Number of cores (integer)')
    args = parser.parse_args()
    return args.nb_scen, args.input_sample, args.nb_cores

def run_scenario(index, sample):
    print("Running scenario {}".format(index+1))
    base_data = Container(load_from="../scenario_data/input_data/input_data_baseline.gdx")
    scenario_data = copy(base_data)
    
    North_l = ["DK","NO","SE","FIN","EE","LV","LT"]
    North = '|'.join(North_l)
    South_l = ["IT","ES","PT","GR","SI","HR","AL","MT","CY","BA","ME","MK","RS"]
    South = '|'.join(South_l)
    East_l = ["PL","CZ","SK","RO","BG","HU",]
    East = '|'.join(East_l)
    West_l = ["FR","DE","NL","UK","BE","LU","AT","CH","IE" ]
    West = '|'.join(West_l)
    
    SUBTECHGROUPKPOT = scenario_data["SUBTECHGROUPKPOT"].records
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(North)), "value"]*= sample["PV_LIMIT_NORTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(South)), "value"]*= sample["PV_LIMIT_SOUTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(East)), "value"]*= sample["PV_LIMIT_EAST"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(West)), "value"]*= sample["PV_LIMIT_WEST"]

    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(East)), "value"]*= sample["ONS_LIMIT_EAST"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(West)), "value"]*= sample["ONS_LIMIT_WEST"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(North)), "value"]*= sample["ONS_LIMIT_NORTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(South)), "value"]*= sample["ONS_LIMIT_SOUTH"]
   
    HYDROGEN_DH2 = scenario_data["HYDROGEN_DH2"].records
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains(North), "value"] *= sample["DH2_DEMAND_NORTH"]
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains(South), "value"] *= sample["DH2_DEMAND_SOUTH"]
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains(East), "value"] *= sample["DH2_DEMAND_EAST"]
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains(West), "value"] *= sample["DH2_DEMAND_WEST"]

    EMI_POL = scenario_data["EMI_POL"].records
    EMI_POL.loc[(EMI_POL["CCCRRRAAA"]=="DENMARK") & (EMI_POL["GROUP"]=="ALL_SECTORS") & (EMI_POL["EMIPOLSET"]=="TAX_CO2"), "value"] *= sample["CO2_TAX"]
    
    CCS_CO2CAPTEFF_G = scenario_data["CCS_CO2CAPTEFF_G"].records
    CCS_CO2CAPTEFF_G.loc[:, "value"] *= sample["CO2_EFF"]
    
    GDATA = scenario_data["GDATA_numerical"].records
    GDATA_CAT = scenario_data["GDATA_categorical"].records
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_ELYS_ELEC_AEC")) & (GDATA["GDATASET"]=="GDFE"),"value"] *= sample["ELYS_ELEC_EFF"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_ELYS_ELEC_AEC")) & (GDATA["GDATASET"]=="GDOMFCOST0"),"value"] *= sample["H2_OandM"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_H2S_H2")) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["H2S_INVC"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_STEAM-REFORMING-CCS")) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["SMR_CCS_INVC"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_STEAM-REFORMING-CCS")) & (GDATA["GDATASET"]=="GDOMFCOST0"),"value"] *= sample["SMR_CCS_OandM"]
    GDATA.loc[(GDATA["GGG"].isin(GDATA_CAT.loc[(GDATA_CAT["TYPES"]=="SOLARPV"), "GGG"])) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["PV_INVC"]
    GDATA.loc[(GDATA["GGG"].isin(GDATA_CAT.loc[(GDATA_CAT["TYPES"]=="WINDTURBINE_ONSHORE"), "GGG"])) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["ONS_WT_INVC"]
    
    XH2INVCOST = scenario_data["XH2INVCOST"].records
    XH2INVCOST.loc[:,"value"] *= sample["H2_TRANS_INVC"]
    
    XINVCOST = scenario_data["XINVCOST"].records
    XINVCOST.loc[:,"value"] *= sample["ELEC_TRANS_INVC"]
    
    FUELPRICE = scenario_data["FUELPRICE"].records
    FUELPRICE.loc[FUELPRICE["FFF"]=="IMPORT_H2","value"] *= sample["IMPORT_H2_P"]
    
    DE = scenario_data["DE"].records
    DE.loc[DE["RRR"].str.contains(North), "value"] *= sample["DE_DEMAND_NORTH"]
    DE.loc[DE["RRR"].str.contains(South), "value"] *= sample["DE_DEMAND_SOUTH"]
    DE.loc[DE["RRR"].str.contains(East), "value"] *= sample["DE_DEMAND_EAST"]
    DE.loc[DE["RRR"].str.contains(West), "value"] *= sample["DE_DEMAND_WEST"]
    
    DE.loc[DE["RRR"].str.contains(North) & (DE["DEUSER"].str.contains("TRANS_TRAINS|TRANS_BUS")), "value"] *= sample["TRANS_DEMAND_NORTH"]
    DE.loc[DE["RRR"].str.contains(South) & (DE["DEUSER"].str.contains("TRANS_TRAINS|TRANS_BUS")), "value"] *= sample["TRANS_DEMAND_SOUTH"]
    DE.loc[DE["RRR"].str.contains(East) & (DE["DEUSER"].str.contains("TRANS_TRAINS|TRANS_BUS")), "value"] *= sample["TRANS_DEMAND_EAST"]
    DE.loc[DE["RRR"].str.contains(West) & (DE["DEUSER"].str.contains("TRANS_TRAINS|TRANS_BUS")), "value"] *= sample["TRANS_DEMAND_WEST"]


    scenario_data.write("../scenario_data/input_data/input_data_scenario_{}.gdx".format(index+1))
    os.system("gams ./Balmorel_finish.gms --id=scenario_{0} r=s1 > ../scenario_data/log_files/output_file_scenario_{0}.txt".format(index+1))

if __name__ == '__main__': 
    if not os.path.isdir("../scenario_data"):
        os.makedirs("../scenario_data")
        os.makedirs("../scenario_data/log_files")
        os.makedirs("../scenario_data/input_data")
        os.makedirs("../scenario_data/output_data")
        
    # Arguments
    num_scen, input_file, nb_cores = get_arg()
    
    # Sampling
    sampler = lhc_sampler(input="../parameters/" + input_file, N=num_scen, rng=42)
    sampler.sample()
    sampler.save_samples("../scenario_data/input_data/samples.txt")
    samples = pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    
    # Get the base data of the sets we are going to change and launch the baseline
    sets = "FUELPRICE, GDATA_numerical, GDATA_categorical, EMI_POL, CCS_CO2CAPTEFF_G, XINVCOST, XH2INVCOST, DE, SUBTECHGROUPKPOT, HYDROGEN_DH2"
    os.system('gams ./Balmorel_ReadData.gms --params="{}" s=s1 > ../scenario_data/log_files/output_file_baseline.txt'.format(sets))
    os.system('gams ./Balmorel_finish.gms --id=baseline r=s1 > ../scenario_data/log_files/output_file_baseline2.txt')
    
    # Loop for multi-core launch
    tic = time.time()
    pool = mp.Pool(processes=nb_cores-1)
    results = pool.starmap_async(run_scenario, [(index, sample) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    # Merge the results files
    merge_cmd = "gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ../scenario_data/output_data/Results_scenario_{}.gdx".format(id+1)
    merge_cmd += " output=../scenario_data/output_data/Results_merged.gdx"
    os.system(merge_cmd)
    
    # Merge the input data files
    merge_cmd = "gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ../scenario_data/input_data/input_data_scenario_{}.gdx".format(id+1)
    merge_cmd += " exclude=FUELPRICE output=../scenario_data/input_data/input_data_merged.gdx"
    os.system(merge_cmd)
    
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))