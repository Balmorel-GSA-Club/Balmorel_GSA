import os
import sys
import argparse
import multiprocessing as mp
from datetime import timedelta
import time 
from copy import copy
from create_samples import sampler
import pandas as pd

sys.path.append(os.path.abspath("../GSA_parameters/"))
from parameters import GSA_parameters

def get_arg():
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument('--nb_scen', default=1, type=int, help='Number of scenarios (integer)')
    parser.add_argument('--input_sample', default="input_params.csv", type=str, help='Name of the input sampling csv file (str)')
    parser.add_argument('--nb_cores', default=2, type=int, help='Number of cores (integer)')
    args = parser.parse_args()
    return args.nb_scen, args.input_sample, args.nb_cores

def run_scenario(index, sample, parameters):
    print("Running scenario {}".format(index+1))
    os.system("gams ./Balmorel_finish.gms --id=scenario_{0} r=s1 > ../scenario_data/log_files/output_file_scenario_{0}.txt".format(index+1))

if __name__ == '__main__': 
    if not os.path.isdir("../scenario_data"):
        os.makedirs("../scenario_data")
        os.makedirs("../scenario_data/log_files")
        os.makedirs("../scenario_data/input_data")
        os.makedirs("../scenario_data/output_data")
        
    # Arguments
    num_scen, input_file, nb_cores = get_arg()
    
    # Copy input csv file to scenario data input data folder (more easy for gams part)
    os.system("cp ../GSA_parameters/{} ../scenario_data/input_data/input.csv".format(input_file))
    
    # Sampling
    sampler = sampler("Sobol", input="../scenario_data/input_data/input.csv", N=num_scen, rng=42)
    sampler.sample()
    sampler.save_samples("../scenario_data/input_data/samples.txt")
    samples = pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    
    # Get the base data of the sets we are going to change and launch the baseline
    parameters = GSA_parameters(input_file = "../scenario_data/input_data/input.csv")
    sets = parameters.load_sets()
    os.system('gams ./Balmorel_ReadData.gms --params="{}" s=s1 > ../scenario_data/log_files/output_file_baseline.txt'.format(sets))
    os.system('gams ./Balmorel_finish.gms --id=baseline r=s1 > ../scenario_data/log_files/output_file_baseline2.txt')
    
    # Loop for multi-core launch
    tic = time.time()
    pool = mp.Pool(processes = nb_cores-2)
    results = pool.starmap_async(run_scenario, [(index, sample, parameters) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    # Merge the results files
    merge_cmd = "gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ../scenario_data/output_data/Results_scenario_{}.gdx".format(id+1)
    merge_cmd += " output=../scenario_data/output_data/Results_merged.gdx"
    os.system(merge_cmd)
    
    # # Merge the input data files -> Not working for now
    # merge_cmd = "gdxmerge"
    # for id in range(len(samples)):
    #     merge_cmd += " ../scenario_data/input_data/input_data_scenario_{}.gdx".format(id+1)
    # merge_cmd += " exclude=FUELPRICE output=../scenario_data/input_data/input_data_merged.gdx"
    # os.system(merge_cmd)
    
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))