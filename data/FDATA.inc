* File FDATA.inc                                                                                                                                             
                                                                                                                                                             
* This file is part of the Balmorel model, version 2.11 Alpha (April 2004).                                                                                  
* For CO2 see http://acm.eionet.europa.eu/docs/ETCACC_TechnPaper_2003_10_CO2_EF_fuels.pdf                                                                    
*------------------------------------------------------------------------------                                                                              
                                                                                                                                                             
* PARAMETER FDATA contains information about emission characteristics of fuels.                                                                              
                                                                                                                                                             
* In addition it contains a name (FDACRONYM) for the fuel, note that this must be as an ACRONYM previously.                                                  
* Presnetly it also contains a numerical code identication of the individual fuels,                                                                          
* however, this may become invalid, so you shold use only the FDACRONYM.                                                                                     
* added biomass fuel emission factors using the 'Emission Factors for Greenhouse Gas Inventories' from epa.gov                                                           
                                                                                                                                                                         
TABLE FDATA(FFF,FDATASET)   'Fuel specific values'                                                                                                                       
* Merge biofuel-related fuels?                                                                                                                                           
                                                                                                               FDACRONYM          FDNB FDCO2     FDSO2 FDN2O FDRE        
*                                                                                                              Units                   kg/GJ     kg/GJ kg/GJ [1;0]       
NUCLEAR                                                                                                        NUCLEAR            1    0         0     0     0           
NATGAS                                                                                                         NATGAS             2    56.1      0     0.001 0           
COAL                                                                                                           COAL               3    94.6      0.714 0.003 0           
LIGNITE                                                                                                        LIGNITE            4    111.1     1.428 0.003 0           
FUELOIL                                                                                                        FUELOIL            5    74        0.446 0.002 0           
LIGHTOIL                                                                                                       LIGHTOIL           6    74        0.023 0.002 0           
HEAVYFUELOIL                                                                                                   HEAVYFUELOIL       7    77.4                  0           
ORIMULSION                                                                                                     ORIMULSION         8    80        1.337 0.002 0           
SHALE                                                                                                          SHALE              9    102       1.428 0.003 0           
PEAT                                                                                                           PEAT               10   107       0.24  0.003 0           
MUNIWASTE                                                                                                      MUNIWASTE          11   95.6885   0.156 0.004 1           
STRAW                                                                                                          STRAW              12   124.66935 0.025 0.004 1           
WOOD                                                                                                           WOOD               13   98.959    0.025 0.004 1           
WOODCHIPS                                                                                                      WOODCHIPS          14   98.959    0.025 0.004 1           
RECYCLEDWOOD                                                                                                   RECYCLEDWOOD       16   98.959    0.025 0.004 1           
WOODPELLETS                                                                                                    WOODPELLETS        17   98.959    0.025 0.004 1           
WOODWASTE                                                                                                      WOODWASTE          18   98.959    0.025 0.004 1           
BIOOIL                                                                                                         BIOOIL             19   72.2042   0.025 0.004 1           
LNG                                                                                                            LNG                20   56.1      0     0.001 0           
LPG                                                                                                            LPG                21   63.1                  0           
BIOGAS                                                                                                         BIOGAS             22   54.93385  0.025 0.004 1           
OTHERGAS                                                                                                       OTHERGAS           23   66.7                  0           
WASTEHEAT                                                                                                      WASTEHEAT          24   0         0     0     1           
WIND                                                                                                           WIND               25   0         0     0     1           
WATER                                                                                                          WATER              26   0         0     0     1           
SUN                                                                                                            SUN                27   0         0     0     1           
ELECTRIC                                                                                                       ELECTRIC           28   0         0     0     0           
WATERRES                                                                                                       WATERRES           29   0         0     0     1           
HEAT                                                                                                           HEAT                                          0           
THERMGAS                                                                                                       THERMGAS                0                     0           
DUMMY                                                                                                          DUMMY              30   0         0     0     0           
COAL_EXETS                                                                                                     COAL_EXETS         31             0.714 0.003 0           
NATGAS_EXETS                                                                                                   NATGAS_EXETS       32             0     0.001 0           
FUELOIL_EXETS                                                                                                  FUELOIL_EXETS      33             0.446 0.002 0           
LPG_EXETS                                                                                                      LPG_EXETS          34                         0           
HEAVYFUELOIL_EXETS                                                                                             HEAVYFUELOIL_EXETS 35                         0           
COKE                                                                                                           COKE               36   106       1.428 0.003 0           
RETORTGAS                                                                                                      RETORTGAS          37   56.8      0     0.001 0           
;                                                                                                                                                            


FDATA('NUCLEAR','FDRE')            = 0 ;
FDATA('NATGAS','FDRE')             = 0 ;
FDATA('COAL','FDRE')               = 0 ;
FDATA('LIGNITE','FDRE')            = 0 ;
FDATA('FUELOIL','FDRE')            = 0 ;
FDATA('LIGHTOIL','FDRE')           = 0 ;
FDATA('HEAVYFUELOIL','FDRE')       = 0 ;
FDATA('ORIMULSION','FDRE')         = 0 ;
FDATA('SHALE','FDRE')              = 0 ;
FDATA('PEAT','FDRE')               = 0 ;
FDATA('MUNIWASTE','FDRE')          = 1 ;
FDATA('STRAW','FDRE')              = 1 ;
FDATA('WOOD','FDRE')               = 1 ;
FDATA('WOODCHIPS','FDRE')          = 1 ;
FDATA('RECYCLEDWOOD','FDRE')       = 1 ;
FDATA('WOODPELLETS','FDRE')        = 1 ;
FDATA('WOODWASTE','FDRE')          = 1 ;
FDATA('BIOOIL','FDRE')             = 1 ;
FDATA('LNG','FDRE')                = 0 ;
FDATA('LPG','FDRE')                = 0 ;
FDATA('BIOGAS','FDRE')             = 1 ;
FDATA('OTHERGAS','FDRE')           = 0 ;
FDATA('WASTEHEAT','FDRE')          = 1 ;
FDATA('WIND','FDRE')               = 1 ;
FDATA('WATER','FDRE')              = 1 ;
FDATA('SUN','FDRE')                = 1 ;
FDATA('ELECTRIC','FDRE')           = 0 ;
FDATA('WATERRES','FDRE')           = 1 ;
FDATA('HEAT','FDRE')               = 0 ;
FDATA('THERMGAS','FDRE')           = 0 ;
FDATA('DUMMY','FDRE')              = 0 ;
FDATA('COAL_EXETS','FDRE')         = 0 ;
FDATA('NATGAS_EXETS','FDRE')       = 0 ;
FDATA('FUELOIL_EXETS','FDRE')      = 0 ;
FDATA('LPG_EXETS','FDRE')          = 0 ;
FDATA('HEAVYFUELOIL_EXETS','FDRE') = 0 ;
FDATA('COKE','FDRE')               = 0 ;
FDATA('RETORTGAS','FDRE')          = 0 ;