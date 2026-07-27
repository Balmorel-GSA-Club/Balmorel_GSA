* File Balmorel.gms
$TITLE Balmorel version 4.03 (March 2022; latest 20220318)
$ifi not exist "../scenario_data/simex/simex_%id%"            execute 'mkdir -p "../scenario_data/simex/simex_%id%"';
$setglobal simex_path "../scenario_data/simex/simex_%id%"

SCALAR IBALVERSN 'This version of Balmorel' /503.20220318/;
* Efforts have been made to make a good model.
* However, most probably the model is incomplete and subject to errors.
* It is distributed with the idea that it will be usefull anyway,
* and with the purpose of getting the essential feedback,
* which in turn will permit the development of improved versions
* to the benefit of other user.
* Hopefully it will be applied in that spirit.

* All GAMS code of the Balmorel model is distributed under ICS license,
* see the license file in the base/model folder.


*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------


* This is a preliminary version of Balmorel 3.03.
* It is intended for review and commenting.
* See a short list of changes since previous version 3.02 in base/documentation/Balmorel303.txt.
* It is scheduled that a final version 3.03 will be available early 2017.

*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------


* GAMS options are $included from file balgams.opt.
* In order to make them apply globally, the option $ONGLOBAL will first be set here:
$ONGLOBAL

$ifi %system.filesys%==UNIX
execute 'chmod  -R ug+rw "../.."';

* The balgams file holds control settings for GAMS.
* Use local file if it exists, otherwise use the one in folder  ../../base/model/.
$ifi     exist 'balgams.opt'  $include  'balgams.opt';
$ifi not exist 'balgams.opt'  $include  '../../base/model/balgams.opt';

* The balopt file holds option (control) settings for the Balmorel model.
* Use local file if it exists, otherwise use the one in folder  ../../base/model/.


$ifi     exist 'balopt.opt'  $include                  'balopt.opt';
$ifi not exist 'balopt.opt'  $include '../../base/model/balopt.opt';

$ifi %system.filesys%==UNIX
execute 'chmod  -R ug+rw "../.."';

* If merging of savepoint files is to be performed,
* make sure that there are no gdx files initially in applied folders:
$ifi %system.filesys%==UNIX
$ifi %MERGESAVEPOINTRESULTS%==yes  execute "rm *.gdx";
$ifi %system.filesys%==MSNT
$ifi %MERGESAVEPOINTRESULTS%==yes  execute "del *.gdx";
$ifi %system.filesys%==UNIX
$ifi %MERGESAVEPOINTRESULTS%==yes  execute "rm ../output/temp/*.gdx";
$ifi %system.filesys%==MSNT
$ifi %MERGESAVEPOINTRESULTS%==yes  execute "del ..\output\temp\*.gdx";

*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------
*---- Some generally applicable stuff: -----------------------------------------
*-------------------------------------------------------------------------------


* The following may be used in display, put or execute_unload statements (the text, not the value, holds relevant information)
SCALAR SystemDateTime "%system.date%,  %system.time%" /NA/;

* Displaying only a text string will not be accessible through the 'table of content' on the .lst file, therefore this
SCALAR INFODISPLAY  "See information next:" /NA/;
DISPLAY INFODISPLAY, "Balmorel run stared at", SystemDateTime;

* Convenient Factors, typically relating Output and Input:
SCALAR IOF100     'Multiplier 100'        /100/;
SCALAR IOF1000    'Multiplier 1000'       /1000/;
SCALAR IOF1000000 'Multiplier 1000000'    /1000000/;
SCALAR IOF0001    'Multiplier 0.001'      /0.001/;
SCALAR IOF0000001 'Multiplier 0.000001'   /0.000001/;
SCALAR IOF3P6     'Multiplier 3.6'        /3.6/;
SCALAR IOF24      'Multiplier 24'         /24/;
SCALAR IOF8760    'Multiplier 8760'       /8760/;
SCALAR IOF8784    'Multiplier 8784'       /8784/;
SCALAR IOF365     'Multiplier 365'        /365/;
SCALAR IOF05      'Multiplier 0.5'        /0.5/;
SCALAR IOF1_      'Multiplier 1 (used with QOBJ and derivation of marginal values)'   /1/;!! special, possibly to disappear in future versions
* Scalars for occational use, their meaning will be context dependent:
SCALAR ISCALAR1   '(Context dependent)';
SCALAR ISCALAR2   '(Context dependent)';
SCALAR ISCALAR3   '(Context dependent)';
SCALAR ISCALAR4   '(Context dependent)';
SCALAR ISCALAR5   '(Context dependent)';

* Initialisations of printing of log and error files and messages:
$INCLUDE '../../base/logerror/logerinc/error1.inc';

* Ensuring existence of needed output folders:
$ifi not dexist "../../simex"            execute 'mkdir -p "../../simex"';
$ifi not dexist "../logerror/logerinc"   execute 'mkdir -p "../logerror/logerinc"';
$ifi not dexist "../output/economic"     execute 'mkdir -p "../output/economic"';
$ifi not dexist "../output/inputout"     execute 'mkdir -p "../output/inputout"';
$ifi not dexist "../output/printout"     execute 'mkdir -p "../output/printout"';
$ifi not dexist "../output/temp"         execute 'mkdir -p "../output/temp"';


*-------------------------------------------------------------------------------
*---- End: Some generally applicable stuff -------------------------------------
*-------------------------------------------------------------------------------

*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------
* In case of Model Balbase4 the following included file substitutes the remaining part of the present file Balmorel.gms:
* Use local file if it exists, otherwise use the one in folder  ../../base/model/.
$ifi %BB4%==yes $ifi     exist 'Balmorelbb4_ReadData.inc'  $include  'Balmorelbb4_ReadData.inc';
$ifi %BB4%==yes $ifi not exist 'Balmorelbb4_ReadData.inc'  $include  '../../base/model/Balmorelbb4_ReadData.inc';



$ifi not exist "../scenario_data/simex/simex_%id%"            execute 'mkdir -p "../scenario_data/simex/simex_%id%"';
$setglobal simex_path "../scenario_data/simex/simex_%id%"
*-----Condition on the id of the run different of baseline ---------------------
execute_unload '../scenario_data/input_data/input_data_%id%.gdx', FUELPRICE, GDATA_numerical, GDATA_categorical, EMI_POL, CCS_CO2CAPTEFF_G, XINVCOST, XH2INVCOST, DE, SUBTECHGROUPKPOT, HYDROGEN_DH2, EV_BEV_available, BEV_TECH_DATA, DR_ADOPTIONRATE, DR_DATA;

$ifi %id%==baseline $goto nobaseline
execute_load '../scenario_data/input_data/input_data_%id%.gdx', FUELPRICE, GDATA_numerical, GDATA_categorical, EMI_POL, CCS_CO2CAPTEFF_G, XINVCOST, XH2INVCOST, DE, SUBTECHGROUPKPOT, HYDROGEN_DH2, EV_BEV_available, BEV_TECH_DATA, DR_ADOPTIONRATE, DR_DATA;     
$onMultiR
$onembeddedCode Python:
# Id of the scenario
id_value = int('%id%'.replace("scenario_", ""))

# Import necessary libraries
import gdxpds 
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath("../GSA_parameters/"))
from parameters import GSA_parameters

# Loading the input samples
parameters = GSA_parameters(input_file = "../scenario_data/input_data/input.csv")
samples = pd.read_csv("../scenario_data/input_data/samples.txt", header=None)
samples.columns = parameters.parameters
sample = samples.loc[id_value-1]

gdx_path = f"../scenario_data/input_data/input_data_%id%.gdx"
gdx_data = gdxpds.to_dataframes(gdx_path)
set_list = parameters.load_sets().split(", ")
scenario_data = {s: gdx_data[s] for s in set_list}
#set_list = parameters.load_sets().split(", ")
#scenario_data = {}
#gams.epsAsZero = False

#for set in set_list :
#    set_data = list(gams.get(set, keyFormat=KeyFormat.FLAT))
    # Transform the data into a dataframe
#    set_data = pd.DataFrame(set_data)
#    print(set_data.shape)
#    scenario_data[set] = set_data
for set in set_list:
    print(scenario_data[set])

parameters.update_input(scenario_data, sample)

#with pd.ExcelWriter(f"../scenario_data/input_data/input_data_scenario_{id_value}.xlsx") as writer:
#    for key, df in scenario_data.items():
#        df.to_excel(writer, sheet_name=key, index=False)

for set in set_list :
    # Transform the data of the dataframe into a list of tuples
    set_data = list(scenario_data[set].itertuples(index=False, name=None))
    # Put them back into the system
    gams.set(set, set_data)

$offEmbeddedCode 
$offMulti

$label nobaseline

GDATA(GGG,GDATASET_numerical) = GDATA_numerical(GGG, GDATASET_numerical);
FDATA(FFF,FDATASET)$ (not sameAs(FDATASET,'FDACRONYM'))=FDATA_numerical(FFF,FDATASET);

*-------------------------------------------------------------------------------

$ifi %BB4%==yes $ifi     exist 'Balmorelbb4_finish.inc'  $include  'Balmorelbb4_finish.inc';
$ifi %BB4%==yes $ifi not exist 'Balmorelbb4_finish.inc'  $include  '../../base/model/Balmorelbb4_finish.inc';

*--- Main results calculation -----------------------------------------------
$ifi %OUTPUT_SUMMARY%==yes $ifi     exist 'OUTPUT_SUMMARY.inc' $include         'OUTPUT_SUMMARY.inc';
$ifi %OUTPUT_SUMMARY%==yes $ifi not exist 'OUTPUT_SUMMARY.inc' $include         '../../base/output/OUTPUT_SUMMARY.inc';
*--- End of Main results calculation ---------------------------------------


$ontext
$ifi %BB4%==yes $goto ENDOFMODEL
*-------------------------------------------------------------------------------
*-------------------------------------------------------------------------------

*----- End of model:------------------------------------------------------------
$label ENDOFMODEL
$ifi %all_endofmodelgdx%==yes execute_unload "all_endofmodel.gdx";
*----- End of model ------------------------------------------------------------


*--- Results collection for this case ------------------------------------------

$ifi not %system.filesys%==MSNT $goto endofMSNToutput
*The following section until $label endofMSNToutput is related to Windows output only
*Please use only backslash \ instead of forward slash / in this section until the label

$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxmerge "%relpathoutput%temp\*.gdx"';
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'move merged.gdx "%relpathoutput%%CASEID%.gdx"';

$ifi %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxmerge "..\output\%CASEID%.gdx"';
$ifi %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'move merged.gdx "%relpathoutput%%CASEID%-results.gdx"'

$ifi %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2MDB%==yes execute '=gdx2access "%relpathoutput%%CASEID%-results.gdx"';
$ifi %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2SQLITE%==yes execute '=gdx2sqlite -i "%relpathoutput%%CASEID%-results.gdx" -o "%relpathoutput%%CASEID%-results.db"';

*--- Results collection and comparison for differents cases --------------------

$ifi not %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxmerge "%relpathoutput%%CASEID%.gdx" "%relpathModel%..\..\%MERGEWITH%/output\%MERGEWITH%.gdx"';
$ifi not %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'move merged.gdx "%relpathoutput%%CASEID%-resmerged.gdx"';

$ifi not %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2MDB%==yes execute '=gdx2access "%relpathoutput%%CASEID%-resmerged.gdx"';
$ifi not %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2SQLITE%==yes execute '=gdx2sqlite -i "%relpathoutput%%CASEID%-resmerged.gdx" -o "%relpathoutput%%CASEID%-resmerged.db"';

$ifi not %DIFFCASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxdiff "%relpathoutput%%CASEID%-results.gdx" "%relpathModel%..\..\%DIFFWITH%/output/%DIFFWITH%-results.gdx"';
$ifi not %DIFFCASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'move diffile.gdx "%relpathoutput%%CASEID%-diff.gdx"';

$label endofMSNToutput

$ifi not %system.filesys%==UNIX $goto endofUNIXoutput
*The following section until $label endofUNIXoutput is related to UNIX output only
*Please use only forward slash / instead of backslash \ in this section until the label

$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxmerge "../output/temp/*.gdx"';
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'mv ./merged.gdx ./"%relpathoutput%%CASEID%.gdx"';

$ifi %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxmerge "../output/%CASEID%.gdx"';
$ifi %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'mv ./merged.gdx ./"%relpathoutput%%CASEID%-results.gdx"'

$ifi %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2MDB%==yes execute '=gdx2access ./"%relpathoutput%%CASEID%-results.gdx"';
$ifi %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2SQLITE%==yes execute '=gdx2sqlite -i ./"%relpathoutput%%CASEID%-results.gdx" -o ./"%relpathoutput%%CASEID%-results.db"';

*--- Results collection and comparison for differents cases --------------------

$ifi not %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxmerge ./"%relpathoutput%%CASEID%.gdx" ./"%relpathModel%../../%MERGEWITH%/output/%MERGEWITH%.gdx"';
$ifi not %MERGECASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'mv ./merged.gdx ./"%relpathoutput%%CASEID%-resmerged.gdx"';

$ifi not %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2MDB%==yes execute '=gdx2access ./"%relpathoutput%%CASEID%-resmerged.gdx"';
$ifi not %MERGECASE%==NONE
$ifi %MERGEDSAVEPOINTRESULTS2SQLITE%==yes execute '=gdx2sqlite -i ./"%relpathoutput%%CASEID%-resmerged.gdx" -o ./"%relpathoutput%%CASEID%-resmerged.db"';

$ifi not %DIFFCASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'gdxdiff ./"%relpathoutput%%CASEID%-results.gdx" ./"%relpathModel%../../%DIFFWITH%/output/%DIFFWITH%-results.gdx"';
$ifi not %DIFFCASE%==NONE
$ifi %MERGESAVEPOINTRESULTS%==yes  execute 'mv ./diffile.gdx ./"%relpathoutput%%CASEID%-diff.gdx"';


$label endofUNIXoutput

*----- End of file:------------------------------------------------------------
$label endoffile
$offtext
