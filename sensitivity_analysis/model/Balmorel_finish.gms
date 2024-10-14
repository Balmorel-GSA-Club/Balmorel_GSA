$onMultiR
$gdxLoadAll 'base_input_data_scenario_%id%.gdx';
$offMulti

GDATA(GGG,GDATASET_numerical) = GDATA_numerical(GGG, GDATASET_numerical);
FDATA(FFF,FDATASET)$ (not sameAs(FDATASET,'FDACRONYM'))=FDATA_numerical(FFF,FDATASET);

$ifi %BB4%==yes $ifi     exist 'Balmorelbb4.inc'  $include  'Balmorelbb4_finish.inc';
$ifi %BB4%==yes $ifi not exist 'Balmorelbb4.inc'  $include  '../../base/model/Balmorelbb4_finish.inc';

execute_unload "ScenarioResults_%id%.gdx"
PRO_YCRAGF
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
