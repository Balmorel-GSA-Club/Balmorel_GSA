GDATA(GGG,GDATASET_numerical) = GDATA_numerical(GGG, GDATASET_numerical);
FDATA(FFF,FDATASET)$ (not sameAs(FDATASET,'FDACRONYM'))=FDATA_numerical(FFF,FDATASET);

*-----Condition on the id of the run different of baseline ---------------------
$ifi %id%==baseline $goto nobaseline

$onMultiR
$onembeddedCode Python:
# Id of the scenario
id_value = int('%id%'.replace("scenario_", ""))

# Import necessary libraries
import gams.transfer as gt
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

set_list = parameters.load_sets().split(", ")
scenario_data = {}
gams.epsAszero=False

for set in set_list :
    set_data = gams.get(set, keyFormat=KeyFormat.FLAT)
    if set == "SUBTECHGROUPKPOT" :
        print(4.94066E-324)
        print(set)
        print(set_data)
    # Transform the data into a dataframe
    set_data = pd.DataFrame(set_data)
    scenario_data[set] = set_data
    if set == "SUBTECHGROUPKPOT" :
        print(set)
        print(set_data)


parameters.update_input(scenario_data, sample)

for set in set_list :
    # Transform the data of the dataframe into a list of tuples
    set_data = list(scenario_data[set].itertuples(index=False, name=None))
    # Put them back into the system
    gams.set(set, set_data)
    print(set, " successfully updated")

$offEmbeddedCode 
$offMulti

$label nobaseline

* Test
execute_unload '../scenario_data/output_data/ScenarioResults_%id%.gdx' EMI_POL, SUBTECHGROUPKPOT ;

*-------------------------------------------------------------------------------

* $ifi %BB4%==yes $ifi     exist 'Balmorelbb4_finish.inc'  $include  'Balmorelbb4_finish.inc';
* $ifi %BB4%==yes $ifi not exist 'Balmorelbb4_finish.inc'  $include  '../../base/model/Balmorelbb4_finish.inc';

* *--- Main results calculation -----------------------------------------------
* $ifi %OUTPUT_SUMMARY%==yes $ifi     exist 'OUTPUT_SUMMARY.inc' $include         'OUTPUT_SUMMARY.inc';
* $ifi %OUTPUT_SUMMARY%==yes $ifi not exist 'OUTPUT_SUMMARY.inc' $include         '../../base/output/OUTPUT_SUMMARY.inc';
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
