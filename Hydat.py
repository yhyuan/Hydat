# Required packages: arcpy from ArcGIS 10.1 and pyodbc from https://code.google.com/p/pyodbc/. 
# visit https://code.google.com/p/pyodbc/downloads/list and download pyodbc-3.0.7.win32-py2.7.exe
# Input data: 
#        1) An Access database named Hydat.mdb, which is unziped from ftp://arccf10.tor.ec.gc.ca/wsc/software/HYDAT/Hydat_20150717.zip. 
#        1) An Access database named Hydat.mdb, which is unziped from ftp://arccf10.tor.ec.gc.ca/wsc/software/HYDAT/Hydat_20170118.zip. 
# Steps: 1) Use C:\Windows\SysWOW64\odbcad32.exe (The default one in control panel might not work since it is 64 bit) to create a ODBC User DSN using Hydat as the name for Hydat.mdb since the office is 32 bit.

# Output data: 
#		A file geodatabases named Hydat.gdb

import sys, string, os, time, zipfile
import arcpy
reload(sys)
sys.setdefaultencoding("latin-1")
import pyodbc

start_time = time.time()

PATH = "."
OUTPUT_PATH = "output"
INPUT_PATH = "input"
if arcpy.Exists(OUTPUT_PATH + "\\Hydat.gdb"):
	os.system("rmdir " + OUTPUT_PATH + "\\Hydat.gdb /s /q")

arcpy.CreateFileGDB_management(OUTPUT_PATH, "Hydat", "9.3")
arcpy.env.workspace = OUTPUT_PATH + "\\Hydat.gdb"

def createFeatureClass(featureName, featureData, featureFieldList, featureInsertCursorFields):
	print "Create " + featureName + " feature class"
	featureNameNAD83 = featureName + "_NAD83"
	featureNameNAD83Path = arcpy.env.workspace + "\\"  + featureNameNAD83
	arcpy.CreateFeatureclass_management(arcpy.env.workspace, featureNameNAD83, "POINT", "", "DISABLED", "DISABLED", "", "", "0", "0", "0")
	# Process: Define Projection
	arcpy.DefineProjection_management(featureNameNAD83Path, "GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]")
	# Process: Add Fields	
	for featrueField in featureFieldList:
		arcpy.AddField_management(featureNameNAD83Path, featrueField[0], featrueField[1], featrueField[2], featrueField[3], featrueField[4], featrueField[5], featrueField[6], featrueField[7], featrueField[8])
	# Process: Append the records
	cntr = 1
	try:
		with arcpy.da.InsertCursor(featureNameNAD83, featureInsertCursorFields) as cur:
			for rowValue in featureData:
				cur.insertRow(rowValue)
				cntr = cntr + 1
	except Exception as e:
		print "\tError: " + featureName + ": " + e.message
		print e
	# Change the projection to web mercator
	#arcpy.Project_management(featureNameNAD83Path, arcpy.env.workspace + "\\" + featureName, "PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]", "NAD_1983_To_WGS_1984_5", "GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]")
	#arcpy.FeatureClassToShapefile_conversion([featureNameNAD83Path], OUTPUT_PATH + "\\Shapefile")
	#arcpy.Delete_management(featureNameNAD83Path, "FeatureClass")
	print "Finish " + featureName + " feature class."


cnxn = pyodbc.connect('DSN=Hydat')
availableDict = {}
dataTypes = ["DLY_FLOWS", "DLY_LEVELS", "SED_DLY_LOADS", "SED_DLY_SUSCON"]
for dataType in dataTypes:
	cursor = cnxn.cursor()
	cursor.execute("select DISTINCT STATION_NUMBER from " + dataType)
	rows = cursor.fetchall()
	availableDict[dataType] = set(map(lambda row: row[0], rows))

featureName = "EC_Hydrometric_Stations"
featureFieldList = map(lambda fieldname: [fieldname, "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["STATION_NUMBER", "STATION_NAME", "PROV_TERR_STATE_LOC", "REGIONAL_OFFICE_ID", "HYD_STATUS", "SED_STATUS"])
featureFieldList = featureFieldList + map(lambda fieldname: [fieldname, "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["LATITUDE", "LONGITUDE", "DRAINAGE_AREA_GROSS", "DRAINAGE_AREA_EFFECT"])
featureFieldList = featureFieldList + map(lambda fieldname: [fieldname, "LONG", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["RHBN", "REAL_TIME", "CONTRIBUTOR_ID", "OPERATOR_ID", "DATUM_ID"])
featureFieldList = featureFieldList + map(lambda fieldname: [fieldname, "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["HYD_STATUS_EN", "HYD_STATUS_FR", "SED_STATUS_EN", "SED_STATUS_FR", "DLY_FLOWS_URL", "DLY_LEVELS_URL", "SED_DLY_LOADS_URL", "SED_DLY_SUSCON_URL"])
# featureFieldList = [["STATION_NUMBER", "TEXT", "", "", "", "", "NON_NULLABLE", "REQUIRED", ""], ["STATION_NAME", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["PROV_TERR_STATE_LOC", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["REGIONAL_OFFICE_ID", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["HYD_STATUS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["SED_STATUS", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["LATITUDE", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["LONGITUDE", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["DRAINAGE_AREA_GROSS", "DOUBLE", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["DRAINAGE_AREA_EFFECT", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["RHBN", "LONG", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["REAL_TIME", "LONG", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["CONTRIBUTOR_ID", "LONG", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["OPERATOR_ID", "LONG", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["DATUM_ID", "LONG", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["HYD_STATUS_EN", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["HYD_STATUS_FR", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""], ["SED_STATUS_EN", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["SED_STATUS_FR", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["DLY_FLOWS_URL", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["DLY_LEVELS_URL", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["SED_DLY_LOADS_URL", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""],  ["SED_DLY_SUSCON_URL", "TEXT", "", "", "", "", "NULLABLE", "NON_REQUIRED", ""]]
featureInsertCursorFields = tuple(["SHAPE@XY"] + map(lambda list: list[0],featureFieldList))

cnxn = pyodbc.connect('DSN=Hydat')
cursor = cnxn.cursor()
cursor.execute("select * from STATIONS WHERE PROV_TERR_STATE_LOC = 'ON'")
#cursor.execute("select * from STATIONS")
rows = cursor.fetchall()
#print len(rows)

def queryData(dataType, StationNumber):
	cursor = cnxn.cursor()
	#print "select * from " + dataType +  " WHERE STATION_NUMBER='" + StationNumber + "'"
	cursor.execute("select * from " + dataType +  " WHERE STATION_NUMBER='" + StationNumber + "'")
	head = ""
	if (dataType == "DLY_FLOWS"):
		head = "STATION_NUMBER,YEAR,MONTH,FULL_MONTH,NO_DAYS,MONTHLY_MEAN,MONTHLY_TOTAL,FIRST_DAY_MIN,MIN,FIRST_DAY_MAX,MAX,FLOW1,FLOW_SYMBOL1,FLOW2,FLOW_SYMBOL2,FLOW3,FLOW_SYMBOL3,FLOW4,FLOW_SYMBOL4,FLOW5,FLOW_SYMBOL5,FLOW6,FLOW_SYMBOL6,FLOW7,FLOW_SYMBOL7,FLOW8,FLOW_SYMBOL8,FLOW9,FLOW_SYMBOL9,FLOW10,FLOW_SYMBOL10,FLOW11,FLOW_SYMBOL11,FLOW12,FLOW_SYMBOL12,FLOW13,FLOW_SYMBOL13,FLOW14,FLOW_SYMBOL14,FLOW15,FLOW_SYMBOL15,FLOW16,FLOW_SYMBOL16,FLOW17,FLOW_SYMBOL17,FLOW18,FLOW_SYMBOL18,FLOW19,FLOW_SYMBOL19,FLOW20,FLOW_SYMBOL20,FLOW21,FLOW_SYMBOL21,FLOW22,FLOW_SYMBOL22,FLOW23,FLOW_SYMBOL23,FLOW24,FLOW_SYMBOL24,FLOW25,FLOW_SYMBOL25,FLOW26,FLOW_SYMBOL26,FLOW27,FLOW_SYMBOL27,FLOW28,FLOW_SYMBOL28,FLOW29,FLOW_SYMBOL29,FLOW30,FLOW_SYMBOL30,FLOW31,FLOW_SYMBOL31\n"
	if (dataType == "DLY_LEVELS"):		
		head = "STATION_NUMBER,YEAR,MONTH,PRECISION_CODE,FULL_MONTH,NO_DAYS,MONTHLY_MEAN,MONTHLY_TOTAL,FIRST_DAY_MIN,MIN,FIRST_DAY_MAX,MAX,LEVEL1,LEVEL_SYMBOL1,LEVEL2,LEVEL_SYMBOL2,LEVEL3,LEVEL_SYMBOL3,LEVEL4,LEVEL_SYMBOL4,LEVEL5,LEVEL_SYMBOL5,LEVEL6,LEVEL_SYMBOL6,LEVEL7,LEVEL_SYMBOL7,LEVEL8,LEVEL_SYMBOL8,LEVEL9,LEVEL_SYMBOL9,LEVEL10,LEVEL_SYMBOL10,LEVEL11,LEVEL_SYMBOL11,LEVEL12,LEVEL_SYMBOL12,LEVEL13,LEVEL_SYMBOL13,LEVEL14,LEVEL_SYMBOL14,LEVEL15,LEVEL_SYMBOL15,LEVEL16,LEVEL_SYMBOL16,LEVEL17,LEVEL_SYMBOL17,LEVEL18,LEVEL_SYMBOL18,LEVEL19,LEVEL_SYMBOL19,LEVEL20,LEVEL_SYMBOL20,LEVEL21,LEVEL_SYMBOL21,LEVEL22,LEVEL_SYMBOL22,LEVEL23,LEVEL_SYMBOL23,LEVEL24,LEVEL_SYMBOL24,LEVEL25,LEVEL_SYMBOL25,LEVEL26,LEVEL_SYMBOL26,LEVEL27,LEVEL_SYMBOL27,LEVEL28,LEVEL_SYMBOL28,LEVEL29,LEVEL_SYMBOL29,LEVEL30,LEVEL_SYMBOL30,LEVEL31,LEVEL_SYMBOL31\n"
	if (dataType == "SED_DLY_LOADS"):
		head = "STATION_NUMBER,YEAR,MONTH,FULL_MONTH,NO_DAYS,MONTHLY_MEAN,MONTHLY_TOTAL,FIRST_DAY_MIN,MIN,FIRST_DAY_MAX,MAX,LOAD1,LOAD2,LOAD3,LOAD4,LOAD5,LOAD6,LOAD7,LOAD8,LOAD9,LOAD10,LOAD11,LOAD12,LOAD13,LOAD14,LOAD15,LOAD16,LOAD17,LOAD18,LOAD19,LOAD20,LOAD21,LOAD22,LOAD23,LOAD24,LOAD25,LOAD26,LOAD27,LOAD28,LOAD29,LOAD30,LOAD31\n"
	if (dataType == "SED_DLY_SUSCON"):
		head = "STATION_NUMBER,YEAR,MONTH,FULL_MONTH,NO_DAYS,MONTHLY_TOTAL,FIRST_DAY_MIN,MIN,FIRST_DAY_MAX,MAX,SUSCON1,SUSCON_SYMBOL1,SUSCON2,SUSCON_SYMBOL2,SUSCON3,SUSCON_SYMBOL3,SUSCON4,SUSCON_SYMBOL4,SUSCON5,SUSCON_SYMBOL5,SUSCON6,SUSCON_SYMBOL6,SUSCON7,SUSCON_SYMBOL7,SUSCON8,SUSCON_SYMBOL8,SUSCON9,SUSCON_SYMBOL9,SUSCON10,SUSCON_SYMBOL10,SUSCON11,SUSCON_SYMBOL11,SUSCON12,SUSCON_SYMBOL12,SUSCON13,SUSCON_SYMBOL13,SUSCON14,SUSCON_SYMBOL14,SUSCON15,SUSCON_SYMBOL15,SUSCON16,SUSCON_SYMBOL16,SUSCON17,SUSCON_SYMBOL17,SUSCON18,SUSCON_SYMBOL18,SUSCON19,SUSCON_SYMBOL19,SUSCON20,SUSCON_SYMBOL20,SUSCON21,SUSCON_SYMBOL21,SUSCON22,SUSCON_SYMBOL22,SUSCON23,SUSCON_SYMBOL23,SUSCON24,SUSCON_SYMBOL24,SUSCON25,SUSCON_SYMBOL25,SUSCON26,SUSCON_SYMBOL26,SUSCON27,SUSCON_SYMBOL27,SUSCON28,SUSCON_SYMBOL28,SUSCON29,SUSCON_SYMBOL29,SUSCON30,SUSCON_SYMBOL30,SUSCON31,SUSCON_SYMBOL31\n"
	f = open (OUTPUT_PATH + '\\' + dataType + '_ON_' + StationNumber + '.csv',"w")
	f.write(head + "\n".join(map(lambda item: ",".join(map(lambda x: " " if (x is None) else str(x), list(item))), cursor.fetchall())))
	f.close()
	return 1

def convertRow(row):
	hyd_status = [None, None]
	if (row[4] == 'A'):
		hyd_status = ['Active', 'En service']
	if (row[4] == 'D'):
		hyd_status = ['Discontinued', 'Ferm\xe9e']
	sed_status = [None, None]
	if (row[5] == 'A'):
		sed_status = ['Active', 'En service']
	if (row[5] == 'D'):
		sed_status = ['Discontinued', 'Ferm\xe9e']
	
	temp = map(lambda dataType: queryData(dataType, row[0]) if row[0] in availableDict[dataType] else None, dataTypes)
	return [(row[7], row[6])] + list(row) + hyd_status + sed_status + map(lambda dataType: 'http://lrcbikdcapmdw07/media/HYDAT_DATA/' + dataType + '_ON_' + row[0] + '.csv' if row[0] in availableDict[dataType] else None, dataTypes)
	#return [(row[7], row[6])] + list(row)
rows = map(convertRow, rows)
#print rows[0]
createFeatureClass(featureName, rows, featureFieldList, featureInsertCursorFields)

elapsed_time = time.time() - start_time
print elapsed_time
