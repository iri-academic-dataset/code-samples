/*==+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|
        10        20        30        40        50        60        70        80
====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|
This code aggregates the IRI store data from UPC-store-week to brand-market-year. 
More generally, it can be used as a template to aggregate to alternative
permutations of three keys (1) PRODUCTS, (2) CROSS-SECTIONAL UNITS, AND (3) 
TIME, e.g. UPC-market-year, brand-store-week, etc. 

The first part of the code defines the aggregation levels. The second code 
reads the data, performs aggregation, and defines several variables. 


NOTES: 

(1 For reading the data in one data statement I squeezed the space 
characters out of some directory names that come standard with the IRI data set. 
Specifically, on the directory structure I changed \Academic Dataset External\ 
to \AcademicDatasetExternal\


DATE: 
27 August 2008

Bug reports to:
Bart Bronnenberg
bart.bronnenberg@uvt.nl
====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+===*/

options THREADS CPUCOUNT=2 ;


/*==+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|
        10        20        30        40        50        60        70        80
====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|

						I. AGGREGATION LEVELS PER DATA KEY

A. PRODUCTS: From Sku to Brand
B. CROSS-SECTIONAL UNITS: From Store to Market 
C. TEMPORAL UNITS: From Week to Month, Quarter, or Year

Aggregation script uses the beer category as an example. 
My root directory for the IRI data is "V:\AcademicDataBaseIRIFiveYears" . The 
directories and file locations on the academic data set are relative to that 
directory. 

/*I.A. From Sku to Brand */
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;

PROC IMPORT OUT= WORK.Attributes 
            DATAFILE= "V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\parsed stub files\prod_beer.xls" 
            DBMS=EXCEL REPLACE;
     SHEET="Sheet1$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data Sku2BrandTemp ; * reads merge fields SY...ITEM and transforms to numeric ;
	set Attributes ; 
	if L2 EQ "" then delete ;
	BRAND = L5 ; 
	SYt = input(SY,2.0) ; GEt = input(GE,4.0) ; VENDt = input(VEND,6.0) ; 
    ITEMt = input(ITEM,6.0) ; 
	drop SY GE VEND ITEM ; 
data Sku2Brand ; 
	set Sku2BrandTemp ;
	SY = SYt ;	GE = GEt ; VEND =VENDt ; ITEM = ITEMt ;
    keep BRAND SY GE VEND ITEM VOL_EQ ; 
run ; /*NOTE - there are several informative fields about SKU attributes  
               which are ignored here. Refer to the data release notes. */

/*I.B. From Store to Market */
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;
data Store2Market  ;
 	infile datalines ; 
	length file2read $200 ;
	input file2read $ ; 
	infile dummy filevar = file2read end = done ;
    do while(not done) ;
       input IRI_KEY OU $9-10 MARKET_NAME $21-45 OPEN ;
	   output ; 
	end ;
    datalines ; 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year1\External\beer\Delivery_Stores 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year2\External\beer\Delivery_Stores 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year3\External\beer\Delivery_Stores 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year4\External\beer\Delivery_Stores 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year5\External\beer\Delivery_Stores 
  	;
	* PLACEHOLDER FOR CHAIN DEFINITION ON CROSS-SECTION KEYS ;
proc sort data = Store2Market nodup ; * 5 YEAR JOINT SET OF UNIQUE STORES
    by IRI_KEY OPEN ;
run ;

/*I.C. From Week to Year */
* contains unique week, month, and year as aggregation fields
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;

PROC IMPORT OUT= WORK.time 
            DATAFILE= "W:\DataIRI\time attributes\IRIweeks.xls" 
            DBMS=EXCEL REPLACE;
     SHEET="Sheet1$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=YES;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

data Week2Year ; 
	set time ;
	xldate = (WEEK-400)*7+31900 ; *FOR USE WITH WINDOWS EXCEL ;
	sasdate = (WEEK-400)*7+9984 ;
	dd = day(sasdate) ;
	mo = month(sasdate) ;
	yr = year(sasdate) ;
data Week2Year ;
    set Week2Year ;
    by yr mo;
    retain month ;
    if first.mo then month+1 ;
run ; 


/*==+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|
        10        20        30        40        50        60        70        80
====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|

							II. MOVEMENT DATA
A. 	Data statements
B.	Aggregate to brand
C. 	Aggregate to market
D. 	Aggregate to years
E.  Computation of measures
F.  Market Coordinates
====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+===*/

*II.A. 	Data statements ;
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;
data StoreDataRaw  ;
 	infile datalines ; 
	length file2read $200 ;
	input file2read $ ; 
	infile dummy filevar = file2read end = done ;
    do while(not done) ;
       input IRI_KEY  WEEK SY GE VEND ITEM UNITS DOLLARS F $47-51 D PR ;
	   output ; 
	end ;
    datalines ; 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year1\External\beer\beer_groc_1114_1165 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year2\External\beer\beer_groc_1166_1217  
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year3\External\beer\beer_groc_1218_1269 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year4\External\beer\beer_groc_1270_1321 
    V:\AcademicDataBaseIRIFiveYears\AcademicDatasetExternal\Year5\External\beer\beer_groc_1322_1373 
    ;
proc sort data = StoreDataRaw ; 
	by SY GE VEND ITEM ;
proc sort data = Sku2Brand ;
	by SY GE VEND ITEM ;
run ; 


/* II.B.	Aggregate to brand */
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;
data StoreDataRawTemp ;
 	merge StoreDataRaw Sku2Brand ; 
	by SY GE VEND ITEM ; 
	VOL = UNITS*VOL_EQ ; 
	if F  NE "NONE" then FEAT = 1 ; else FEAT = 0 ; * feature dummy ;
	if D NE 0 then DISP = 1 ; else DISP = 0 ; * display dummy ; 
	if PR NE 0 then PRED = 1 ; else PRED = 0 ; * discount dummy ;
	FVOL = FEAT*VOL ; *volume on any feature ;
	DVOL = DISP*VOL ; *volume on any display ;
	PVOL = PRED*VOL ; *volume on any price discount ;
	if UNITS = . then delete ; 
	if BRAND = '' then delete ;
	drop SY GE VEND ITEM VOL_EQ F D PR FEAT DISP PRED; 
run ;

proc sort data = StoreDataRawTemp ; 
	by IRI_KEY WEEK BRAND ;
run ;

proc summary data = StoreDataRawTemp; 
var UNITS DOLLARS VOL FVOL DVOL PVOL;
by IRI_KEY WEEK BRAND ;
output out = Move_Br_St_Wk sum = U D V F I R; *Units-Dollars-Volume-Feature-dIsplay-Reduction ;
run ;

proc datasets ; 
	delete sku2brandtemp attributes storedataraw storedatarawtemp ;
run; 

* II.C. Aggregate to market 
NOTE -- he store panel is not static. Therefore part of the volumetric
        movement across time will be from attrition and recruitment in 
        the store panel. 
NOTE -- the market totals are sums of sales across stores that are member of 
        the store panel. The market totals are not estimates of total market 
        volume. 
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;

proc sort data = Move_Br_St_Wk ; 
	by IRI_KEY ;
proc sort data = Store2Market nodup; 
	by IRI_KEY ; 
run ; 

data MoveTemp ; 
	merge Move_Br_St_Wk Store2Market ; 
	by IRI_key ; 
	if U = . then delete ;
	if MARKET_NAME = "." then delete ;
	* for beer category ;
	if MARKET_NAME = "PHILADELPHIA" then delete ; 
	if MARKET_NAME = "PROVIDENCE,RI" then delete ; 
	if MARKET_NAME = "HARRISBURG/SCRANT" then delete ; 
run ; 
* NOTE -- there may be more records for the same IRI_KEY in 
          the Store2Market set. This happens when the store reopens 
          with say a different square footage. In 2001, 7/2051 occurrences.
          The Merge uses the last record, which is the more recent one ;
* NOTE -- in the beer category there are little or no movement data for 
          the grocery channel in Philadelphia, Providence, and Harrisburg/Scrant. ;  

proc sort data = MoveTemp ;
	by MARKET_NAME BRAND WEEK ;
run ;

proc summary data = MoveTemp ; 
    var U D V F I R; *Units-Dollars-Volume-Feature-dIsplay-Reduction ;
    by MARKET_NAME BRAND WEEK ;
    output out = Move_Br_Mkt_Wk sum = U D V F I R; 
run ;

proc datasets ; 
	delete movetemp movement1 Move_br_st_wk;
run; 


/* II.D. Aggregate to totals over time */
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;

proc sort data = Move_Br_Mkt_Wk ; 
by MARKET_NAME BRAND ;
run ;

proc summary data = Move_Br_Mkt_Wk ; 
var U D V F I R; *Units-Dollars-Volume-Feature-dIsplay-Reduction ;
by MARKET_NAME BRAND ;
output out = Move_Br_Mkt_Yr sum = U D V F I R; 
run ;


/* E.	Compute meaasures */
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;

proc summary data = Move_Br_Mkt_Yr ;
var V ;
by MARKET_NAME   ;
output out = mkttot sum = CATVOL ; 
run ;

data Move_Br_Mkt_Yr ; 
merge Move_Br_Mkt_Yr mkttot ; 
by MARKET_NAME ; 
MarketShare = V/CATVOL ;
Price = D/(V+1E-12) ;
Feature = F/(V+1E-12) ; 
Display = I/(V+1E-12) ; 
PriceRed = R/(V+1E-12) ; 
drop _type_ _freq_ ; 
run ;

/*F.	Merge in Coordinates */
* The coordinates are computed as means of latitude and longitude of stores
within a given IRI market taken from the TD linx data base;  
*====+====|====+====|====+====|====+====|====+====|====+====|====+====|====+====|;

data Coord ; 
input MARKET_NAME $1-25 lat lon ;
cards ;
ATLANTA                           33.72 -84.26
BIRMINGHAM/MONTG.                 33.10 -86.67
BOSTON                            42.25 -71.17
BUFFALO/ROCHESTER                 42.88 -78.18
CHARLOTTE                         35.49 -81.03
CHICAGO                           41.84 -87.88
CLEVELAND                         41.42 -81.64
DALLAS, TX                        32.76 -96.93
DES MOINES                        41.67 -93.41
DETROIT                           42.54 -83.32
EAU CLAIRE                        44.82 -91.49
GRAND RAPIDS                      42.57 -85.73
GREEN BAY                         44.37 -88.31
HARRISBURG/SCRANT                 40.73 -76.49
HARTFORD                          41.87 -72.63
HOUSTON                           29.76 -95.40
INDIANAPOLIS                      39.87 -86.09
KANSAS CITY                       39.04 -94.50
KNOXVILLE                         36.15 -83.43
LOS ANGELES                       34.00 -117.49
MILWAUKEE                         42.95 -88.41
MINNEAPOLIS/ST. PAUL              45.08 -93.24
MISSISSIPPI                       32.85 -89.66
NEW ENGLAND                       44.14 -71.12
NEW ORLEANS, LA                   30.31 -89.91
NEW YORK                          40.76 -74.01
OKLAHOMA CITY                     35.42 -97.44
OMAHA                             41.16 -96.20
PEORIA/SPRINGFLD.                 40.39 -89.22
PHILADELPHIA                      39.88 -75.25
PHOENIX, AZ                       32.85 -111.59
PITTSFIELD                        42.45 -73.26
PORTLAND,OR                       45.00 -123.02
PROVIDENCE,RI                     41.64 -71.44
RALEIGH/DURHAM                    35.98 -79.44
RICHMOND/NORFOLK                  37.18 -76.96
ROANOKE                           37.48 -80.63
SACRAMENTO                        38.60 -121.17
SALT LAKE CITY                    40.79 -111.90
SAN DIEGO                         32.85 -117.07
SAN FRANCISCO                     37.72 -122.26
SEATTLE/TACOMA                    47.57 -122.25
SOUTH CAROLINA                    33.80 -81.06
SPOKANE                           47.69 -117.04
ST. LOUIS                         38.62 -90.37
SYRACUSE                          43.24 -75.85
TOLEDO                            40.96 -83.27
TULSA,OK                          36.03 -95.86
WASHINGTON, DC                    38.91 -77.02
WEST TEX/NEW MEX                  33.65 -104.50
;
run ;

proc sort data = Move_Br_Mkt_Yr ;
   by MARKET_NAME ;
proc sort data = Coord ;
   by MARKET_NAME ;
data Move_Br_Mkt_Yr2 ; 
   merge Move_Br_Mkt_Yr Coord ; 
   by MARKET_NAME ;
   if U=. then delete ;
   if MARKET_NAME = "" then delete ;
   keep MARKET_NAME BRAND MARKETSHARE U D V Price Feature Display PriceRed LAT LON ; 
run ;



proc sort ; 
by BRAND MARKET_NAME ;
run ;

proc datasets ; 
	delete coord mkttot move_br_mkt_wk  ;
run ; 

PROC EXPORT DATA= WORK.move_br_mkt_yr
            OUTFILE= "YourFileName.xls" 
            DBMS=EXCEL5 REPLACE;
RUN;


