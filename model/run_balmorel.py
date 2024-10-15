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
    base_data = Container(load_from="./base_input_data.gdx")
    scenario_data = copy(base_data)

    EMI_POL = scenario_data["EMI_POL"].records
    EMI_POL.loc[(EMI_POL["CCCRRRAAA"]=="DENMARK") & (EMI_POL["GROUP"]=="ALL_SECTORS") & (EMI_POL["EMIPOLSET"]=="TAX_CO2"), "value"]=sample["CO2_TAX"]
    
    XINVCOST = scenario_data["XINVCOST"].records
    XINVCOST.loc[:,"value"] *= sample["E_T_INVC"]

    GDATA = scenario_data["GDATA_numerical"].records
    GDATA.loc[(GDATA["GGG"].str.contains("ELYS")) & (GDATA["GDATASET"].str.contains("GDINVCOST0")),"value"]*=sample["ELEC_INVC"]
    GDATA.loc[(GDATA["GGG"].str.contains("STEAM")) & (GDATA["GDATASET"].str.contains("GDINVCOST0")),"value"]*=sample["ELEC_STEAM_INVC"]

    FUELPRICE = scenario_data["FUELPRICE"].records
    FUELPRICE.loc[FUELPRICE["FFF"]=="NATGAS","value"]*=sample["NATGAS_P"]
    
    SUBTECHGROUPKPOT = scenario_data["SUBTECHGROUPKPOT"].records
    SUBTECHGROUPKPOT.loc[SUBTECHGROUPKPOT["TECH_GROUP"]=="SOLARPV", "value"]*=sample["PV_NORTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DK")), "value"]*= sample["ON_SHORE_DK"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DE")), "value"]*= sample["ON_SHORE_DE"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_ONSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.startswith(("NO","SE","NL"))), "value"]*= sample["ON_SHORE_NORTH"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_OFFSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DK")), "value"]*= sample["OFF_SHORE_DK"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_OFFSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.contains("DE")), "value"]*= sample["OFF_SHORE_DE"]
    SUBTECHGROUPKPOT.loc[(SUBTECHGROUPKPOT["TECH_GROUP"]=="WINDTURBINE_OFFSHORE") & (SUBTECHGROUPKPOT["CCCRRRAAA"].str.startswith(("NO","SE","NL"))), "value"]*= sample["OFF_SHORE_NORTH"]
    
    XH2INVCOST = scenario_data["XH2INVCOST"].records
    XH2INVCOST.loc[:,"value"] *= sample["H2_T_INVC"]

    HYDROGEN_DH2 = scenario_data["HYDROGEN_DH2"].records
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains("DK"), "value"] *= sample["H2_Demand_DK"]
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.contains("DE"), "value"] *= sample["H2_Demand_DE"]
    HYDROGEN_DH2.loc[HYDROGEN_DH2["CCCRRRAAA"].str.startswith(("NO", "SE", "NL")), "value"] *= sample["H2_Demand_Rest"]
    
    DE = scenario_data["DE"].records
    DE.loc[DE["RRR"].str.contains("DK"), "value"] *= sample["DE_Demand_DK"]
    DE.loc[DE["RRR"].str.contains("DE"), "value"] *= sample["DE_Demand_DE"]
    DE.loc[DE["RRR"].str.startswith(("NO", "SE", "NL")), "value"] *= sample["DE_Demand_Rest"]

    XKRATE = scenario_data["XKRATE"].records
    XKRATE.loc[:,"value"] = sample["E_T_AVAIL"]

    scenario_data.write("../scenario_data/input_data/input_data_scenario_{}.gdx".format(index+1))
    os.system("gams ./Balmorel_finish.gms --id={0} r=s1 > output_file_scenario_{0}.txt".format(index+1))

if __name__ == '__main__': 
    sampler=morris_sampler(input="input_params.csv", N=1)
    sampler.sample()
    samples=pd.DataFrame(sampler.samples, columns = sampler.problem["names"])
    sets = "DE, FUELPRICE, GDATA_numerical, GDATA_categorical, SUBTECHGROUPKPOT, EMI_POL, XINVCOST, HYDROGEN_DH2, XH2INVCOST, XKRATE"
    os.system('gams ./Balmorel_ReadData.gms --params="{}" s=s1 > output_file_baseline.txt'.format(sets))

    tic = time.time()
    #pool = mp.Pool(processes=mp.cpu_count()-1)
    pool = mp.Pool(processes=4)
    results = pool.starmap_async(run_scenario, [(index, sample) for index, sample in samples.iterrows()])
    pool.close()
    pool.join()

    merge_cmd="gdxmerge"
    for id in range(len(samples)):
        merge_cmd += " ../scenario_data/output_data/ScenarioResults_{}.gdx".format(id+1)
    os.system(merge_cmd)
    tac = time.time()
    time_trajectory = tac-tic
    print("Time to create scenarios:", timedelta(seconds=time_trajectory))