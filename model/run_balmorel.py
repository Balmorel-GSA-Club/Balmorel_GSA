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
    args = parser.parse_args()
    return args.nb_scen, args.input_sample

def run_scenario(index, sample):
    print("Running scenario {}".format(index+1))
    base_data = Container(load_from="../scenario_data/input_data/input_data_baseline.gdx")
    scenario_data = copy(base_data)

    EMI_POL = scenario_data["EMI_POL"].records
    EMI_POL.loc[(EMI_POL["CCCRRRAAA"]=="DENMARK") & (EMI_POL["GROUP"]=="ALL_SECTORS") & (EMI_POL["EMIPOLSET"]=="TAX_CO2"), "value"] *= sample["CO2_TAX"]
    
    CCS_CO2CAPTEFF_G = scenario_data["CCS_CO2CAPTEFF_G"].records
    CCS_CO2CAPTEFF_G.loc[:, "value"] *= sample["CO2_EFF"]
    
    GDATA = scenario_data["GDATA_numerical"].records
    GDATA_CAT = scenario_data["GDATA_categorical"].records
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_ELYS_ELEC_AEC")) & (GDATA["GDATASET"]=="GDFE"),"value"] *= sample["ELYS_ELEC_EFF"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_ELYS_ELEC_AEC")) & (GDATA["GDATASET"]=="GDOMFCOST0"),"value"] *= sample["H2_OandM"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_H2S_H2-TNKC")) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["H2S_INVC"]
    GDATA.loc[(GDATA["GGG"].str.contains("GNR_STEAM-REFORMING-CCS")) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["SMR_CCS_INVC"]
    GDATA.loc[(GDATA["GGG"].isin(GDATA_CAT.loc[(GDATA_CAT["TYPES"]=="SOLARPV"), "GGG"])) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["PV_INVC"]
    GDATA.loc[(GDATA["GGG"].isin(GDATA_CAT.loc[(GDATA_CAT["TYPES"]=="WINDTURBINE_ONSHORE"), "GGG"])) & (GDATA["GDATASET"]=="GDINVCOST0"),"value"] *= sample["ONS_WT_INVC"]
    
    XH2INVCOST = scenario_data["XH2INVCOST"].records
    XH2INVCOST.loc[:,"value"] *= sample["H2_TRANS_INVC"]
    
    XINVCOST = scenario_data["XINVCOST"].records
    XINVCOST.loc[:,"value"] *= sample["ELEC_TRANS_INVC"]
    
    FUELPRICE = scenario_data["FUELPRICE"].records
    FUELPRICE.loc[FUELPRICE["FFF"]=="IMPORT_H2","value"] *= sample["IMPORT_H2_P"]

    scenario_data.write("../scenario_data/input_data/input_data_scenario_{}.gdx".format(index+1))
    os.system("gams ./Balmorel_finish.gms --id=scenario_{0} r=s1 > ../scenario_data/log_files/output_file_scenario_{0}.txt".format(index+1))

if __name__ == '__main__': 
    if not os.path.isdir("../scenario_data"):
        os.makedirs("../scenario_data")
        os.makedirs("../scenario_data/log_files")
        os.makedirs("../scenario_data/input_data")
        os.makedirs("../scenario_data/output_data")
        
    # Arguments
    num_scen, input_file = get_arg()
    
    # Sampling
    sampler = lhc_sampler(input=input_file, N=num_scen, rng=42)
    sampler.sample()
    sampler.save_samples("samples.txt")
    samples = pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    
    # Get the base data of the sets we are going to change and launch the baseline
    sets = "FUELPRICE, GDATA_numerical, GDATA_categorical, EMI_POL, CCS_CO2CAPTEFF_G, XINVCOST, XH2INVCOST"
    os.system('gams ./Balmorel_ReadData.gms --params="{}" s=s1 > ../scenario_data/log_files/output_file_baseline.txt'.format(sets))
    os.system('gams ./Balmorel_finish.gms --id=baseline r=s1 > ../scenario_data/log_files/output_file_baseline2.txt')
    
    # Loop for multi-core launch
    tic = time.time()
    pool = mp.Pool(processes=mp.cpu_count()-1)
    results = pool.starmap_async(run_scenario, [(index, sample) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    # Merge the results files
    merge_cmd = "gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ../scenario_data/output_data/Results_scenario_{}.gdx".format(id+1)
    os.system(merge_cmd)
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))
