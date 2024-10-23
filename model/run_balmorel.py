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

    North_l_PV = ["DK","NO","SE","NL", "DE4-E","DE4-N","DE4-W","FIN", "UK","EE","LV","LT","PL","BE"]
    North_PV = '|'.join(North_l_PV)
    South_l_PV = ["DE4-S","FR","IT","CH","AT","CZ","ES","PT","SK","HU","SI","HR","RO","BG","GR","IE","LU","AL","ME","MK","BA","RS","TR","MT","CY"]
    South_PV = '|'.join(South_l_PV)
    SUBTECHGROUPKPOT = scenario_data["SUBTECHGROUPKPOT"].records
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(North_PV)), "value"]*= sample["PV_LIMIT_NORTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(South_PV)), "value"]*= sample["PV_LIMIT_SOUTH"]
    

    North_l = ["NO","SE","NL","FIN", "UK","EE","LV","LT","PL","BE"]
    North = '|'.join(North_l)
    South_l = ["FR","IT","CH","AT","CZ","ES","PT","SK","HU","SI","HR","RO","BG","GR","IE","LU","AL","ME","MK","BA","RS","TR","MT","CY"]
    South = '|'.join(South_l)
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DK")), "value"]*= sample["ONS_LIMIT_DK"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DE")), "value"]*= sample["ONS_LIMIT_DE"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(North)), "value"]*= sample["ONS_LIMIT_NORTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains(South)), "value"]*= sample["ONS_LIMIT_SOUTH"]
    
    HYDROGEN_DH2 = scenario_data["HYDROGEN_DH2"].records
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains("DK"), "value"] *= sample["DH2_DEMAND_DK"]
    North_and_South_l = ["NO","SE","NL","FIN", "UK","EE","LV","LT","PL","BE","FR","IT","CH","AT","CZ","ES","PT","SK","HU","SI","HR","RO","BG","GR","IE","LU","AL","ME","MK","BA","RS","TR","MT","CY"]
    North_and_South = '|'.join(North_and_South_l)
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains("DE"), "value"] *= sample["DH2_DEMAND_DE"]
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.startswith((North_and_South)), "value"] *= sample["DH2_DEMAND_Rest"]

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
    
    sampler = lhc_sampler(input=input_file, N=num_scen, rng=42)
    sampler.sample()
    sampler.save_samples("samples.txt")
    samples = pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    sets = "DE, SUBTECHGROUPKPOT, HYDROGEN_DH2"
    os.system('gams ./Balmorel_ReadData.gms  --params="{}" s=s1 > ../scenario_data/log_files/output_file_baseline.txt'.format(sets))
    os.system('gams ./Balmorel_finish.gms --id=baseline r=s1 > ../scenario_data/log_files/output_file_baseline2.txt')
    tic = time.time()
    pool = mp.Pool(processes=mp.cpu_count()-1)
    # pool = mp.Pool(processes=4)
    results = pool.starmap_async(run_scenario, [(index, sample) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    merge_cmd = "gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ../scenario_data/output_data/Results_scenario_{}.gdx".format(id+1)
    os.system(merge_cmd)
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))
