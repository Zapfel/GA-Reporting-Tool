import numpy as np
import pandas as pd
import openpyxl
import Universal as urt
import top_pages as tp
from google2pandas import *
import warnings
warnings.filterwarnings("ignore", category=ResourceWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

Brands = pd.read_csv("CSV_WITH_FORMAT_BRAND-BRAND_ID-BRAND_NAME.csv")
conn = GoogleAnalyticsQuery(secrets='./ga-creds/client_secrets.json', token_file_name='./ga-creds/analytics.dat')
dcm = pd.read_csv('./DCM/DCM-10-17.csv',skiprows = 9) #Potentially required DCM file
start_date = '2017-11-01'
end_date = '2017-11-30'

xlsx = pd.ExcelFile('Page Path.xlsx') #EXCEL FILE CONTAINING PAGES

#Function to produce the top pages section of the website data pull

#FORMAT
#MediaDate (Start_date), Brand(used in xlsx.parse), Category1(overview,mediumtype,Top Pages) Category2(devicetpye, page path) Category3(medium, pagename), Visits, Bounces, Visit Duration, UniqueVisitors Custom
#NewVisits, ReturningVists, Pageviews, PageViewsCustom2P, PageViewsCustom3P, DataSourceID


def Website(dcm,start_date,end_date,conn):

	BrandList = DataStorage('BrandList')
	BrandIds = DataStorage('BrandIds')
	DataDict = {}

	for Brand in BrandList:
		BrandData = [Brand,BrandIds[Brand],BrandList[Brand]]
		BrandData = pd.DataFrame(np.array(BrandData).reshape(-1,len(BrandData)),columns = ['Brand','ID','Name'])

		Ov = Overview(BrandData).reset_index(drop=True)
		Med = Medium(BrandData,dcm).reset_index(drop=True)
		TopPages = Top_Pages(BrandData).reset_index(drop=True)
		
		DataList = [Ov,Med,TopPages]
		df = pd.concat(DataList)

		DataDict[BrandList[Brand]] = df
	return(DataDict)


#Individually scrubs each overview column. Designed for future changes when necessary
def Overview(BrandData):
	df = urt.Data_Pull(BrandData,start_date,end_date,'Overview',conn)
	df.drop('Brand', axis = 1)
	df['Brand'] = BrandData.iloc[0,2]
	df = Column_Scrub(df,'Overview',BrandData['Name'])
	return(df)



def Medium(BrandData,dcm):
	df = urt.Data_Pull(BrandData,start_date,end_date,'Medium',conn)
	df = df.rename(columns={'Ad Content' : 'Placement ID'})
	dcm = dcm[['Placement ID','Placement']]
	df = pd.merge(df,dcm, how = 'left',on = 'Placement ID')
	df = Medium_Clean(df,BrandData)
	#df = df[['Brand','Medium','Device','Sessions','UVs']]
	df = Column_Scrub(df,'Medium',BrandData['Name'])
	return(df)

#Need a function to handle exceptions in Medium data, and converting (none) to direct
#Takes medium data frame and applies transformations so it will output correctly, used at start of medium function
def Medium_Clean(df,BrandData):

	df['Medium'] = df['Medium'].str.lower()
	df['Source'] = df['Source'].str.lower()


	for i in DataStorage('Medium_Clean','MediumConversion'):
		df.loc[df['Medium'] == i,'Medium'] = DataStorage('Medium_Clean','MediumConversion',i)

	for i in DataStorage('Medium_Clean','SourceMediumConversion'):
		df.loc[df['Source'] == i,'Medium'] = DataStorage('Medium_Clean','SourceMediumConversion',i)
		if i == 'facebook':
			df.loc[df['Source'].str.contains(i),'Medium'] = 'social'

	#Loops to determine if a brand has video or banner information that needs to be checked.
	AttributedData = MC_Reattribution(df)
	df = MC_Video_Banner(df,BrandData)
	df = MC_Group(df,AttributedData,BrandData)
	#for i in MediumList

	return(df)

#Replaced mistyped column names and attribute their data to other columns based on weighted averages
def MC_Reattribution(df):
	MediumCheck = df.Medium.unique().tolist()
	Reattribution = pd.DataFrame(columns = ['Medium','Source','Device','Sessions','UVs'])
	AttributedData = pd.DataFrame(columns = ['Medium','Source','Device','Sessions','UVs'])
	AttributionCheck = df.groupby(['Medium','Source','Device'],as_index=False)['Sessions','UVs'].sum()
	#Creating a dataframe of data to be reattributed based on device and source
	for i in MediumCheck:
		if i not in DataStorage('Medium_Clean','MediumList'):
			data = df.loc[df['Medium'] == i,['Medium','Source','Device','Sessions','UVs']].reset_index(drop=True)
			Reattribution = Reattribution.append(data)

	if Reattribution.empty is not True:
		for i in range(0,Reattribution.shape[0]):
			data = Reattribution.iloc[i,:]
			AC_data = AttributionCheck[(AttributionCheck['Source'] == data['Source']) & (AttributionCheck['Device'] == data['Device']) & (AttributionCheck['Medium'] != data['Medium'])]

			AC_data['Session Share'] = round((AC_data.Sessions / AC_data['Sessions'].sum()) * data['Sessions'])
			AC_data['UVs Share'] = round((AC_data.iloc[:,4] / AC_data.iloc[:,4].sum()) * data['UVs'])
			AttributedData = AttributedData.append(AC_data)
		print(AttributedData)
		AttributedData = AttributedData.iloc[:,[0,1,2,4,6]]
		AttributedData = AttributedData.rename(columns={'Session Share': 'Sessions','UVs Share':'UVs'})
	return(AttributedData)

	#The sole purpose of this function is to classify data into medium column as a 'Video or 'Banner', 
	#if the data is not in the given dictionarys VideoList or BannerList, this function will make no changes
def MC_Video_Banner(df,BrandData):

	if BrandData.iloc[:,2].str in DataStorage('Medium_Clean','VideoList').keys():
		for i in DataStorage('Medium_Clean','VideoList',BrandData['Name']):
			df.loc[((df['Placement'].str.contains(i)) & (df['Placement'] != np.nan)),'Medium'] = 'video'

	if BrandData.iloc[:,2].str in DataStorage('Medium_Clean','BannerList'):
		for i in DataStorage('Medium_Clean','BannerList',BrandData['Name']):
			df.loc[((df['Placement'].str.contains(i)) & (df['Placement'] != np.nan)),'Medium'] = 'banner'

	return(df)

	#This function groups the medium into categories based on Medium, and Device. It further
	#updates the data frame to be 'Complete' i.e. ensuring that each medium, has one of each device category,
	#even if the category sessions and uv's are 0. any further manipulations to the grouped dataframe can be performed here
def MC_Group(df,AttributedData,BrandData):

	grouped_df = df.groupby(['Medium','Device'],as_index=False)['Sessions','UVs'].sum()
	RowIter = len(grouped_df.index) - 1
	AttributeListM = AttributedData.Medium.unique().tolist()
	AttributeListD = AttributedData.Device.unique().tolist()

	for i in grouped_df.Medium.unique().tolist():
		if i not in DataStorage('Medium_Clean','MediumList'):
			grouped_df = grouped_df.loc[grouped_df['Medium'] != i]

	for medium in grouped_df.Medium.unique().tolist():
		for device in grouped_df.Device.unique().tolist():
			if device not in (grouped_df.loc[grouped_df['Medium'] == medium,'Device'].tolist()):
				grouped_df = grouped_df.append({'Medium':medium,'Device':device,'Sessions':0,'UVs':0}, ignore_index = True)

	for medium in AttributeListM:
		for device in AttributeListD:
			grouped_df.loc[(grouped_df['Medium'] == medium) & (grouped_df['Device'] == device), 'Sessions'] = grouped_df.loc[(grouped_df['Medium'] == medium) & (grouped_df['Device'] == device), 'Sessions'] + \
																										AttributedData.loc[(AttributedData['Medium'] == medium) & (AttributedData['Device'] == device),'Sessions']
			
			grouped_df.loc[(grouped_df['Medium'] == medium) & (grouped_df['Device'] == device), 'UVs'] = grouped_df.loc[(grouped_df['Medium'] == medium) & (grouped_df['Device'] == device), 'UVs'] + \
																										AttributedData.loc[(AttributedData['Medium'] == medium) & (AttributedData['Device'] == device),'UVs']

	grouped_df['Brand'] = BrandData.iloc[0,2]

	#print(grouped_df)
	return(grouped_df)

	#Runs top_pages.py script to pull pageview data and attribute to proper naming 
	#data = tp.top_pages(Brands,start_date,end_date,conn)
def Top_Pages(BrandData):
	df = tp.top_pages(BrandData,start_date,end_date,conn)
	TopPages = DataStorage('Init','xlsx').parse(BrandData.iloc[0,2])
	df = df.drop(['Clean Page Title'], 1)
	df = pd.merge(TopPages,df, how = 'left',on = 'Clean URLs')
	df = df[pd.notnull(df['Pageviews'])]
	df = Column_Scrub(df,'Top_Pages',BrandData.iloc[0,2])
	return(df)


#Takes a data frame, and the name of the report: TopPages, Medium, or Overview and returns a df in website data format
def Column_Scrub(df,ReportType,Brand):
	cols = df.columns.tolist() #List of columns in our current Data Frame
	ColumnConversions = DataStorage(DictName = 'ColumnConversions')
	ColumnNamesWebsite = DataStorage(DictName = 'ColumnNamesWebsite')
	ConversionData = DataStorage('ConversionData', ReportType)

	#loop to update dataframe columns to proper naming conventions
	for i in cols:
		if i in ColumnConversions:
			cols[cols.index(i)] = ColumnConversions[i]

	df.columns = cols #Renames the columns in the dataframe to their changed counterparts
	#loop to add additional columns not part of the data frame and append appropriate data 
	#for i in ColumnNamesWebsite

	#Adding new website data columns:
	#We need a list of columns that aren't in the default 
	#Conversion data contains a dictionary with no matching columns to our specified df. these elements are static every run.
	for i in ConversionData:
		df[i] = ConversionData[i] #Sets every column in the DF to be equal to the value of the conversion data
	df.reset_index()
	#any additional elements to be added can go here. For now its just returning visitors, under the overview reports.
	df['MediaDate'] = DataStorage('Init','start_date') #Adding media date
	if ReportType == 'Overview':
		df['ReturningVisits'] = df['UniqueVisitorsCustom'] - df['NewVisits']

	#Reordering columns to order website data requires them.
	df = df[ColumnNamesWebsite]

	return(df)


def DataStorage(DictName = None,KeyName = None,SubKeyName = None):
	
	SuperDict_Website = {
	#Initialization data. Referenced once, easiest to have in here.
	'Init' : {
	'start_date' : '2017-10-1',
	'end_date' : '2017-10-31',
	'xlsx' : pd.ExcelFile('Page Path.xlsx'),
	'Brands' : pd.read_csv("US_Merck_Index_2017.csv"),
	'conn' : GoogleAnalyticsQuery(secrets='./ga-creds/client_secrets.json', token_file_name='./ga-creds/analytics.dat'),
	'dcm' : pd.read_csv('./DCM/DCM-10-17.csv',skiprows = 9)
	},#Basic Varaible Initialization for ease of access,

	#Referenced in Column_Scrub function, list of all column names in the final website data file, all these names must be contained in the DF referencing this list
	'ColumnNamesWebsite' : ['MediaDate','Brand','Data Type','Category1','Category2','Category3','Visits','Bounces','VisitDuration','UniqueVisitorsCustom',
				   'NewVisits','ReturningVisits','Pageviews','PageViewsCustom2P','PageViewsCustom3P','DataSourceID','SocialValues','Category4','PagePathVisits'], #End ColumnNamesWebsite

	#Referenced in Column_Scrub function
	'ColumnConversions' : {
	#Pageviews
	'Clean URLs' : 'Category3','Title' : 'Category2',
	#Medium
	'Medium' : 'Category1', 'Device' : 'Category2',
	#Overview
	'Sessions' : 'Visits', 'Session Duration' : 'VisitDuration', 'UVs': 'UniqueVisitorsCustom', 'New Users' : 'NewVisits', '2+PV' : 'PageViewsCustom2P','3+PV' : 'PageViewsCustom3P'
	},#End ColumnConversions

	#Data for columns that aren't filled in, pre-defined. No functions here outside of MediaDate
	'ConversionData' : {
	#All columns Overview data pull doesnt contain, that are required in website data pull. Same concept for Medium, and Top_Pages
	'Overview' : {'Data Type' : 'Website','Category1': 'Overview', 'Category2' : np.nan, 'Category3' : np.nan, 'DataSourceID' : 'CWTest',
				  'SocialValues' : np.nan, 'Category4' : np.nan, 'PagePathVisits' : np.nan},#End 'Overview' Dict

	'Medium' : {'Data Type' : 'Website', 'Category3' : 'Medium', 'Bounces' : np.nan, 'VisitDuration' : np.nan, 'UniqueVistorsCustom' : np.nan,
			  'NewVisits' : np.nan, 'ReturningVisits' : np.nan, 'Pageviews' : np.nan, 'PageViewsCustom2P' : np.nan, 'PageViewsCustom3P' : np.nan, 'DataSourceID' : 'CWTest',
			  'SocialValues' : np.nan, 'Category4' : np.nan, 'PagePathVisits' : np.nan}, #End 'Medium' Dict

	'Top_Pages' : {'Data Type' : 'Website', 'Category1' : 'Top Pages','Visits' : np.nan, 'Bounces' : np.nan,  
			  'VisitDuration' : np.nan, 'UniqueVisitorsCustom' : np.nan, 'NewVisits' : np.nan, 'ReturningVisits' : np.nan,'PageViewsCustom2P' : np.nan,  
			  'PageViewsCustom3P' : np.nan, 'DataSourceID' : 'CWTest','SocialValues' : np.nan, 'Category4' : np.nan, 'PagePathVisits' : np.nan} #End 'TopPages' Dict

	},#End ConversionData
	#Dictionary linking each brand together. for merging into a single website excel sheet
	'BrandLinks' : {'Brand' : ['Brand Name 1','Brand Name 2']},#End Brandlinks

	#dictionary containing GA brand names and referrential brand names for the SQL server
	'BrandList' : {'Brand' : 'Brand Name'}, #End Brandlist

	#Dictionary containing GA brand names and ID's for pulling data 
	'BrandIds' : {'Brand' : 'Brand View ID'},#End BrandIds

	'Medium_Clean' : {
	#This list is run before others, allowing it to re-attribute mistyped data 
	'MediumList' : ['direct','natural search','paid search','inoffice','email','referring sites','social'],
	'VideoList' : {'HPV' : ['Skippable','NonSkip','Video','Hulu']}, #End VideoList
	'BannerList' : {'HPV' : ['Spotify']}, #End BannerList
	'MediumConversion' : {'(none)' : 'direct','organic' : 'natural search', 'cpm' : 'paid search','cpc' : 'paid search','phreesia' : 'inoffice','referral' : 'referring sites'}, #End Medium Conversion
	'SourceMediumConversion' : {'t.co' : 'social', 'facebook' : 'social','shinglesinfo.com' : 'referred from shinglesinfo.com','hpv.com' : 'referred from hpv.com'} #End SourceMediumConversion
	}#End Medium_Clean

	}#End SuperDict_Website

	if KeyName is None:
		data = SuperDict_Website[DictName]
	elif SubKeyName is None:
		data = SuperDict_Website[DictName][KeyName]
	else:
		data = SuperDict_Website[DictName][KeyName][SubKeyName]

	return(data)

#Medium(Brands,dcm,start_date,end_date,conn)
#print(Top_Pages(Brands,xlsx,start_date,end_date,conn))
#print(DataStorage('ColumnNamesWebsite'))
#Overview(,start_date,end_date,conn)

#BrandList = DataStorage('BrandList')
#BrandIds = DataStorage('BrandIds')
#Brand = 'US - Pneumoccocal vaccine - Pneumo 23 - HCC - pneumovax23.com'

#BrandData = [Brand,BrandIds[Brand],BrandList[Brand]]
#BrandData = pd.DataFrame(np.array(BrandData).reshape(-1,len(BrandData)),columns = ['Brand','ID','Name'])
#print(BrandData['Brand'].str)

#for Brand in BrandList:
#	Data = [Brand,BrandIds[Brand],BrandList[Brand]]
#	Data = pd.DataFrame(np.array(Data).reshape(-1,len(Data)),columns = ['Brand','ID','Name'])
#	Data1 = Overview(Data)
#	print(Data1)
	#print(data)
#if BrandData['Name'] not in list(DataStorage('Medium_Clean','VideoList').keys()):
	#print('hi')
#data['Gardasil'].to_csv('Test7.csv')


data = Website(dcm,start_date,end_date,conn)
data['Brand Name'].to_csv('test.csv')
