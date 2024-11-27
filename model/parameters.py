import pandas as pd
import numpy as np
from gamspy import SpecialValues

# This class is used to deal with paramneters in the model
class GSA_parameters :
    def __init__(self, input_file):
        self.input = pd.read_csv(input_file)
        self.parameters = self.input["Parameter"]

    def load_sets(self):
        sets = "FUELPRICE, GDATA_numerical, GDATA_categorical, EMI_POL, CCS_CO2CAPTEFF_G, XINVCOST, XH2INVCOST, DE, SUBTECHGROUPKPOT, HYDROGEN_DH2"
        return sets
    
    def update_input(self, scenario_data, sample):
        North_l = ["DK","NO","SE","FIN","EE","LV","LT"]
        North = '|'.join(North_l)
        South_l = ["IT","ES","PT","GR","SI","HR","AL","MT","CY","BA","ME","MK","RS"]
        South = '|'.join(South_l)
        East_l = ["PL","CZ","SK","RO","BG","HU",]
        East = '|'.join(East_l)
        West_l = ["FR","DE","NL","UK","BE","LU","AT","CH","IE" ]
        West = '|'.join(West_l)

        FUELPRICE = scenario_data["FUELPRICE"].records
        FUELPRICE.loc[FUELPRICE["FFF"]=="NATGAS","value"]*=sample["NATGAS_P"]
        
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
        EMI_POL.loc[(EMI_POL["GROUP"]=="ALL_SECTORS") & (EMI_POL["EMIPOLSET"]=="TAX_CO2"), "value"] *= sample["CO2_TAX"]
        
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
        
        return scenario_data