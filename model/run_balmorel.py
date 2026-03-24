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
    parser.add_argument('--threads', '-t', default=1, type=int, help='Number of threads for each scenario (integer)')
    parser.add_argument('--path', default='scenario_data', type=str, help='Path to save scenario data')
    parser.add_argument('--sampler', default='LHC', type=str, help='Sampling strategy used for sampling (Morris, Sobol, LHC, FAST)')
    args = parser.parse_args()
    return args.nb_scen, args.input_sample, args.nb_cores, args.threads, args.path, args.sampler

def run_scenario(index, sample, parameters, rpath, threads):
    print("Running scenario {}".format(index+1))
    os.system("gams ./Balmorel_finish.gms --id=scenario_{0} --rpath={1} r=s1 threads={2} > ../{1}/log_files/output_file_scenario_{0}.txt".format(index+1, rpath, threads))

if __name__ == '__main__': 
    num_scen, input_file, nb_cores, threads, rpath, sampling_strategy = get_arg()
    if not os.path.isdir("../{}".format(rpath)):
        os.makedirs("../{}".format(rpath))
        os.makedirs("../{}/log_files".format(rpath))
        os.makedirs("../{}/input_data".format(rpath))
        os.makedirs("../{}/output_data".format(rpath))
        os.makedirs("../{}/simex".format(rpath))
    
    # Copy input csv file to scenario data input data folder (more easy for gams part)
    os.system("cp ../GSA_parameters/{} ../{}/input_data/input.csv".format(input_file, rpath))
    
    # Sampling
    sampler = sampler(sampling_strategy, input="../{}/input_data/input.csv".format(rpath), N=num_scen, rng=42)
    sampler.sample()
    sampler.save_samples("../{}/input_data/samples.txt".format(rpath))
    samples = pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    
    # Get the base data of the sets we are going to change and launch the baseline
    parameters = GSA_parameters(input_file = "../{}/input_data/input.csv".format(rpath))
    sets = parameters.load_sets()
    os.system('gams ./Balmorel_ReadData.gms --params="{}" s=s1 > ../{}/log_files/output_file_baseline.txt'.format(sets, rpath))
    os.system('gams ./Balmorel_finish.gms --id=baseline --rpath={1} r=s1 threads={0}> ../{1}/log_files/output_file_baseline2.txt'.format(nb_cores-1, rpath))
    
    # Loop for multi-core launch
    tic = time.time()
    # Calculate number of scenarios to run in parallel based on the number of cores and threads
    num_processes = nb_cores // threads
    pool = mp.Pool(processes =num_processes)
    results = pool.starmap(run_scenario, [(index, sample, parameters, rpath, threads) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    # # Merge the results files
    # merge_cmd = "gdxmerge"
    # for id in range(len(samples)):
    #     merge_cmd += " ../{}/output_data/Results_scenario_{}.gdx".format(rpath, id+1)
    # merge_cmd += " output=../{}/output_data/Results_merged.gdx".format(rpath)
    # os.system(merge_cmd)
    
    # # Merge the input data files -> Not working for now
    # merge_cmd = "gdxmerge"
    # for id in range(len(samples)):
    #     merge_cmd += " ../scenario_data/input_data/input_data_scenario_{}.gdx".format(id+1)
    # merge_cmd += " exclude=FUELPRICE output=../scenario_data/input_data/input_data_merged.gdx"
    # os.system(merge_cmd)
    
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))
