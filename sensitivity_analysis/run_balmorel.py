from gamspy import Container
import os
import multiprocessing as mp
from datetime import timedelta
import time 
from copy import copy
from create_samples import morris_sampler
import pandas as pd

def run_scenario(index, sample):
    print("Running scenario {}".format(index+1))
    base_data = Container(load_from="C:/Users/taigr/Desktop/comp/Balmorel/base/model/base_input_data.gdx")
    scenario_data = copy(base_data)
    GDATA = scenario_data["GDATA_numerical"].records
    GDATA.loc[(GDATA["GGG"].str.contains("ELYS|STEAM")) & (GDATA["GDATASET"].str.contains("GDINVCOST0")),"value"]*=sample["ELEC_INVC"]
    FUELPRICE = scenario_data["FUELPRICE"].records
    FUELPRICE.loc[FUELPRICE["FFF"]=="NATGAS","value"]*=sample["NATGAS_P"]
    SUBTECHGROUPKPOT = scenario_data["SUBTECHGROUPKPOT"].records
    SUBTECHGROUPKPOT.loc[SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV", "value"]*=sample["PV_NORTH"]
    #SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE" & SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DK")), "value"]*= sample["ON_SHORE_DK"]
    scenario_data.write("C:/Users/taigr/Desktop/comp/Balmorel/base/model/base_input_data_scenario_{}.gdx".format(index+1))
    os.system("gams ./Balmorel_finish.gms --id={0} r=s1 > output_file_scenario_{0}.txt".format(index+1))

if __name__ == '__main__': 
    sampler=morris_sampler(input="input_morris.csv", N=1)
    sampler.sample()
    samples=pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    sets = "DE, FUELPRICE, GDATA_numerical, GDATA_categorical, SUBTECHGROUPKPOT"
    #os.system('gams ./Balmorel_ReadData.gms --params="{}" s=s1 > output_file_start.txt'.format(sets))
    base_data = Container(load_from="C:/Users/taigr/Desktop/comp/Balmorel/base/model/base_input_data.gdx")

    tic = time.time()
    #pool = mp.Pool(processes=mp.cpu_count()-1)
    pool = mp.Pool(processes=2)
    results = pool.starmap(run_scenario, [(index, sample) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    merge_cmd="gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ./ScenarioResults_{}.gdx".format(id+1)
    os.system(merge_cmd)
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))