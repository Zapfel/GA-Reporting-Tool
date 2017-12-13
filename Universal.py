import numpy as np
import pandas as pd
import openpyxl
from dplython import (DplyFrame, X, diamonds, select, sift, sample_n,
    sample_frac, head, arrange, mutate, group_by, summarize, DelayFunction)
from google2pandas import *
import datetime
from dateutil import relativedelta


Brands = pd.read_csv("US_Merck_Index_2017.csv")
conn = GoogleAnalyticsQuery(secrets='./ga-creds/client_secrets.json', token_file_name='./ga-creds/analytics.dat')

Indicators = pd.read_csv("CustomBrandColumnModifier.csv")
Indicators = Indicators.loc[:,['Campaign Name','Indication']].rename(columns={'Campaign Name': 'Campaign'})
#Changing Column name so Indicators can be binded to Brands
Indicators.loc[:,'Campaign'] = Indicators.loc[:,'Campaign'].str.lower()




#Pulls monthly website data
def Website(Brands,start_date,end_date,conn):
	Reports = ['Overview','Medium','Pages']

	for ReportType in Reports:
		print('Starting ' + ReportType)
		data = Data_Pull(Brands,start_date,end_date,ReportType,conn)
		FileName = ReportType + '-' + datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d') + '-' + datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d') + '.csv'
		print('Writing ' + ReportType + ' to file ' + FileName) 
		data.to_csv(FileName,index = False)




#Major function of the Universal Reporting Tool,
#Binds all other function definitions into 
#All functions below Data_Pull are ordered in list they are called from Data_Pull
	
def Data_Pull(Brands,start_date,end_date,ReportType,conn):
	i = 0
	for ID in Brands.iloc[:,1]: 
		print(Brands.iloc[i][0])
		List = Query_Mod(Brands.iloc[i,:].tolist(),ReportType)
		df = Data_Loop(List,ID,ReportType,start_date,end_date,conn)
		if Brands.iloc[i,1] == 119847739:
			df = df.rename(columns={'goal4Completions': 'KPI'})
			df = df >> mutate(Brand = Brands.iloc[i,0])
		else:
			df = df.rename(columns={'goal16Completions': 'KPI'})
			df = df >> mutate(Brand = Brands.iloc[i,0])

		#Adding Indication column for swoop and yieldbot reports
		if(ReportType in ('Swoop','Yieldbot','Medium')):
			if(ID == 120033970):
				df = pd.merge(df,Indicators, on = 'Campaign', how = 'left')
			else:
				df = df >> mutate(Indication = 'N/A')
		
	#Merging data frames in the loop
		if i == 0:
			BE = df
		else:
			frames = [BE,df]
			BE = pd.concat(frames)

		i = i + 1

	BE['Start Date'] = start_date
	BE['End Date'] = end_date

	df = Column_Adjust(BE,ReportType)
	'''
	if(ReportType == 'Swoop'):
		ReportType2 = 'Swoop_P23'
		List = Query_Mod(Brands.iloc[26,:].tolist(),ReportType2)
		P23 = Data_Loop(List,Brands.iloc[26,1],ReportType2,start_date,end_date,conn)
		P23 = P23 >> mutate(Keyword = np.nan) >> mutate(Indication = 'N/A') >> mutate(NewUsers = np.nan) >> mutate(Pageviews = np.nan) >> mutate(Bounces = np.nan) >> mutate(pageviewsPerSession = np.nan)
		P23 = P23 >>  mutate(avgTimeOnPage = np.nan) >> mutate(KPI = np.nan) >> mutate(StartDate = start_date) >> mutate(EndDate = end_date) >> mutate(Brand = Brands.iloc[26,0])
		P23 = Column_Adjust(P23,ReportType2)
		frames2 = [BE,P23]
		BE = pd.concat(frames2)
	'''
	return(df)


#Dim_Metric_List() function, Called from Data_Pull() function
#List of Dimensions, and Metrics generation function for each type of report.
#PotentialElementsList is a row from the data frame Brands, which is pulled from the US_Merck_Index_2017.csv File.
#ReportType is a variable that determines what type of report is created.
#Since each report has a unique list of elements for dimensions and metrics this is how we can continually update this file
def Query_Mod(PotentialElementsList,ReportType):

	#Initializing lists for Metrics, and Dimensions
	Metrics_List = Super_Dict('Metrics',ReportType)

	Dimensions_List = Super_Dict('Dimensions',ReportType)

	#KPI, 2+PV, 3+PV are different for each Brand, we need to adjust them accordingly
	#Appending Metrics list to have each brands proper KPI
	if(PotentialElementsList[1] == 119847739):
		Metrics_List.append('ga:goal4Completions')

	elif(ReportType not in ('ReportType_SpecificBrand','Pages')):
		Metrics_List.append('ga:goal16Completions')

	List = [Metrics_List,Dimensions_List]
	if(ReportType not in ('Overview','Medium','Pages','Ad-Hoc')): #Adjust accordingly to new report additions, similiar line in Data_Gather() function
		Filter = Super_Dict('Filter',ReportType)
		List.append(Filter)
	return(List)




# Updating Query_Mod() and Column_Adjust() For future reports.
# To add a new report to the above, define a name for your report. for this example lets call it 'A'
# find a common list of Metrics, and Dimensions that 'A' uses, and add them to their respective dictionaries
# If any unique elements exist two things can be done. 
# 1 adjust the code above with an if statement that conditionally adds the element we want based on somehting
# Modify the US_Merck_Index_2017.csv file with an additional column reflecting the data we want added.


#Wrapper for Data_Gather, loops data if its greater than 10,000 rows to get all data
def Data_Loop(List,ID,ReportType,start_date,end_date,conn):
	i = 1
	index = 1
	DataCheck = True
	while DataCheck == True:
		df = Data_Gather(List,ID,index,ReportType,start_date,end_date,conn)
		if i == 1: 
			df1 = df
		else:
			data = [df1,df]
			df1 = pd.concat(data)
			

		if df.shape[0] != 10000:
			DataCheck = False
		else:
			index = index + 10000
	
		i = i + 1
	return(df1)





#Data_Gather() Function
#Data_Gather queries GA 3 times for each ID
#The first Query pulls all the main elements of the data 
#Second Query pulls the 2+PV data
#Final QUery pulls the 3+PV data
#It then merges these 3 data frames into one and returns it to the Data_Pull() function
def Data_Gather(List,ID,index,ReportType,start_date,end_date,conn):
	data = []
	PV2n3 = 'ga:sessions' #Metric modifier. Used to get sessions count.
	for i in [1,2,3]:
		query = {\
    	   		 'ids' : ID,
    	         'metrics' : List[0],
    	  	     'dimensions' : List[1],
    	  		 'start_date' : start_date,
    	         'end_date' : end_date,
    	         'start_index' : index,
    	   	     'max_results' : 10000
    	   	     }
    		     
		if(ReportType not in ('Overview','Medium','Pages','Ad-Hoc')): #Be sure to adjust the no filter list in Query_Mod() as well
   			query['filters'] = List[2]
    	#Conditional Modifiers for the query
		if(i == 2): # Adds segment for 2+PV
			query['segment'] = 'sessions::condition::ga:pageDepth>=2'
			query['metrics'] = PV2n3  #All sessions with segment pagedepth>=2 and pagedepth>=3 are 2+PV and 3+PV respectively. 

		elif(i == 3): # Adds segment for 3+PV
			query['segment'] = 'sessions::condition::ga:pageDepth>=3'
			query['metrics'] = PV2n3 

		#If statements below  add to statements above, only used for keytruda due to its additional segment.
		#Logic if i = 2|3 & id = keytruda it will keep above segment info, and attach the if statement below
		#If id != keytruda, then the below if statements will not run, and if id === keytruda then it will only add the BRAND segment info

		if(((ID == 'ID INTEGER') and (i == 2)) or ((ID == 'ID INTEGER') and (i == 3))): #Adds BRAND segment to the 2+PV & 3+PV segment
			query['segment'] = query['segment'] + ';sessions::condition::!ga:pagePath=@/hcp/'
			query['metrics'] = PV2n3 

		elif(ID == 120033970): #Adds segment for keytruda
			query['segment'] = 'sessions::condition::!ga:pagePath=@/hcp/'

		#Querying GA step
		df, metadata = conn.execute_query(**query)

		#Changing column names. Later in the function this will allow us to merge all 3 data frames into one with all the proper data we are looking for.
		if(i == 2):
			df = df.rename(columns={'sessions' : '2+PV'})
		elif(i == 3):
			df = df.rename(columns={'sessions' : '3+PV'})

		#Appending the dataframe to our data list (list of dataframes). With this we can merge all 3
		data.append(df)

	#Here we want to merge the data frames over all the dimensions.
	#To get a list of dimensions take the column names of data[1] or data[2] since the only metric they have is 2+pv or 3+pv
	#Drop the last element and then merge over that list
	cols = data[1].columns.tolist() #Gives list of all column name

	del cols[-1] #Deletes last element of the list of column names (2+PV in our case)
	
	Data = pd.merge(pd.merge(data[0],data[1],how = 'left',on=cols),data[2],how = 'left',on=cols) #Merges all the DFs

	Data = Data.fillna(0) #From the left joins nan's have appeared. This converts them to 0's

	#Adding Start Date and End date 
	return(Data)


#Column_Adjust() function. Takes in a pandas dataframe df which has been modified to ave all necessary columns of a report type, and the desired report type 
#to know which columns are the proper ones. Returns the data in the format in an ordered manner, with column names changed to reflect their status in SQL 
def Column_Adjust(df,ReportType):
	#List of column names that require changing
	col_names = {'adContent' : 'Ad Content','Users' : 'UVs', 'NewUsers' : 'New Users','pageviewsPerSession' : 'Page/Sessions', 'avgTimeOnPage' : 'Avg Session Durations',
				 'StartDate' : 'Start Date','EndDate' : 'End Date', 'yearMonth' : 'Month of Year','sessionDuration' : 'Session Duration', 'deviceCategory' : 'Device',
				 'pagePath' : 'Page', 'pageTitle' : 'Page Title'}
	#Reordering the columns to output correctly
	df = df[Super_Dict('cols',ReportType)]
	names = df.columns.tolist()

	#Renaming columns for proper output and sql saving
	for i in names:  
		if i in col_names:
			names[names.index(i)] = col_names[i]
	df.columns = names

	return(df)


#Super dict is a function containing a nested dictionary.
#It holds most of the hard coded data required for the script,
#and exists so all of it is easily accessible together
#Returns a request of a desired report we want.
#Requests are metrics,dimensions,filters, and columns
#referenced in Query_mod, and column adjust functions
def Super_Dict(RequestType,ReportType):

	SuperDict = {

	'Metrics' : {'FE_BE' : ["ga:Sessions","ga:Users","ga:Bounces","ga:Pageviews"],

			     'Display' : ["ga:Sessions","ga:Users","ga:Bounces","ga:Pageviews"],

			     'Swoop' : ["ga:Sessions",'ga:NewUsers',"ga:Users","ga:Pageviews","ga:Bounces",'ga:pageviewsPerSession','ga:avgTimeOnPage'],

			     'Swoop_P23' :  ['ga:Sessions','ga:Users'],

			     'Yieldbot' : ["ga:Sessions",'ga:NewUsers',"ga:Users","ga:Pageviews","ga:Bounces",'ga:pageviewsPerSession','ga:avgTimeOnPage'],

			     'Overview' : ['ga:Sessions','ga:Users','ga:Bounces','ga:NewUsers','ga:Pageviews','ga:sessionDuration'],

			     'Medium' : ['ga:Sessions','ga:Users'],

			     'Pages' : ['ga:Pageviews'],

			     'Ad-Hoc' : ['ga:dcmClicks','ga:dcmImpressions']}, #End Metrics

	'Dimensions' : {'FE_BE' : ['ga:yearMonth','ga:Campaign','ga:Source','ga:Keyword','ga:adContent'],

				    'Display' : ['ga:yearMonth','ga:Source','ga:Campaign','ga:Keyword','ga:adContent'],

				    'Swoop' : ['ga:Date','ga:Campaign','ga:adContent','ga:deviceCategory','ga:Keyword'],

				    'Swoop_P23' : ['ga:source','ga:landingPagePath','ga:Medium','ga:Date','ga:Campaign','ga:adContent','ga:deviceCategory'],

				    'Yieldbot' : ['ga:Date','ga:Campaign','ga:adContent','ga:deviceCategory','ga:Keyword'],

				    'Overview' : ['ga:yearMonth'],

				    'Medium' : ['ga:yearMonth','ga:deviceCategory','ga:Medium','ga:Source','ga:Campaign','ga:adContent'],

				    'Pages' : ['ga:yearMonth','ga:pagePath','ga:pageTitle'],

				    'Ad-Hoc' : ['ga:dcmlasteventsiteplacement']}, #end Dimensions

	'Filter' : {'FE_BE' : ["medium=~display",'or',"medium=~cpm","or","medium=~video"],

			    'Swoop' : 'ga:source=@SEARCH_PARTNER1',

			    'Swoop_P23' : ['medium=~referral','and','landingPagePath=~eng=3'],

			    'Yieldbot' : 'ga:source=@SEARCH_PARTNER2',

			    'Display' : ["medium=~display",'or',"medium=~cpm","or","medium=~video"],

			    'Ad-Hoc' : ['medium=~social']},	#end 'Filter', note: some report types may not have filters, this is ok.

	#this is for the column_adjust function. All names here should be entered similiar to how they are in the ga query. ***CASE SENSITIVE***
	'cols' : {'FE_BE' : ['Brand','yearMonth','Start Date','End Date','Campaign','Source','Keyword','adContent',"Sessions","Users","Bounces","Pageviews",'KPI','2+PV','3+PV'],

			  'Display' : ['Brand','yearMonth','Source','Campaign','Keyword','adContent',"Sessions","Users","Bounces","Pageviews",'KPI','2+PV','3+PV'],

			  'Swoop' : ['Brand','Date','Start Date','End Date','Campaign','adContent','deviceCategory','Keyword','Indication',"Sessions",
				  			 		   'NewUsers',"Users","Pageviews","Bounces",'pageviewsPerSession','avgTimeOnPage','KPI','2+PV'],

			  'Swoop_P23' : ['Brand','Date','StartDate','EndDate','Campaign','adContent','deviceCategory','Keyword','Indication',"Sessions",
				  			               'NewUsers',"Users","Pageviews","Bounces",'pageviewsPerSession','avgTimeOnPage','KPI','2+PV'],

			  'Yieldbot' : ['Brand','Date','Start Date','End Date','Campaign','adContent','deviceCategory','Keyword','Indication',"Sessions",
				  						  'NewUsers',"Users","Pageviews","Bounces",'pageviewsPerSession','avgTimeOnPage','KPI','2+PV'],

			  'Overview' : ['Brand','yearMonth','Sessions','Users','Bounces','NewUsers','Pageviews','sessionDuration','KPI','2+PV','3+PV'],

			  'Medium' : ['yearMonth','Brand','deviceCategory','Medium','Source','Campaign','adContent','Indication','Sessions','Users','KPI'],

			  'Pages' : ['Brand','yearMonth','pagePath','pageTitle','Pageviews'],

			  'Ad-Hoc' : ['Brand','dcmlasteventsiteplacement','dcmClicks','dcmImpressions']}	#End 'cols'

			  }#End superdict

	data = SuperDict[RequestType][ReportType]
	return(data)
'''
IMPORTANT NOTE: When running ReportType = 'Ad-Hoc' you can adjust Brands to fit what you need. Access the .csv file merck_index_2017 and use Brands.iloc[[1,2,3,...],:] to get exact rows you want in report.


import Website as W
BrandList = W.DataStorage('BrandList')
BrandIds = W.DataStorage('BrandIds')
start_date = '2017-10-01'
end_date = '2017-10-30'

test2 = [test,BrandIds[test],BrandList[test]]
Data = pd.DataFrame(np.array(test2).reshape(-1,len(test2)))
print(Data_Pull(Data,start_date,end_date,'',conn))



writer = pd.ExcelWriter(FileName, engine='openpyxl')
Data.to_excel(writer, sheet_name=ReportType,index = False)
writer.save()
writer.close()
print('data count below')
print(Data.shape[0])
'''
