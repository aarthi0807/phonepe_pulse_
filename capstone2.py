import json
import os
import pandas as pd
import mysql.connector
from mysql.connector.errors import Error

mydb = mysql.connector.connect(
  host="localhost",
  user="remoteuser",
  password="******",
  database='phonepe_pulse'
)

cursor = mydb.cursor()

#function to get aggregated data from json file and keep it in dataframe
def get_aggregated_data(filepath,quarter,states2):            
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read())
        #normalize the json file and store in dataframe
        df = pd.json_normalize(data['data']['transactionData'], meta = ['name'], record_path = ['paymentInstruments'])
        df = df.iloc[:,1:]
        df = df.rename(columns = {'count' : 'transaction_count','name' : 'transaction_name'})
        #appending year, quarter and state columns to the original dataframe
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df = pd.concat([df,df2], axis=1)
    else:
        print("path doesn't exist")
    return(df)


#function to get district transaction data from json file and keep it in dataframe
def get_district_trans(filepath,quarter,states2):            
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read())
        #normalize the json file and store in dataframe
        df = pd.json_normalize(data['data']['hoverDataList'], meta = ['name'], record_path = ['metric'])
        df = df.iloc[:,1:]
        df = df.rename(columns = {'name' : 'district'})
        #appending year, quarter and state columns to the original dataframe
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df = pd.concat([df,df2], axis=1)
    return(df)

#function to get district user data from json file and keep it in dataframe
def get_district_user(filepath,quarter,states2):
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read())
        df = data['data']['hoverData']
        df1 = pd.DataFrame(df).transpose().reset_index()
        df1 = df1.rename(columns = {'index' : 'district'})
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df1 = pd.concat([df1,df2], axis=1)
    return(df1)

def get_top_trans(filepath,quarter,states2):
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read())       
        #normalize the json file and store in dataframe
        df = pd.json_normalize(data['data']['districts']) 
        df = df.drop('metric.type', axis=1)
        df = df.rename(columns = {'entityName' : 'district', 'metric.count' : 'transaction_count', 'metric.amount' : 'amount'})
        #appending year, quarter and state columns to the original dataframe
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df = pd.concat([df,df2], axis=1)
        return(df)

def get_top_pincode_trans(filepath,quarter,states2):
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read()) 
        #normalize the json file and store in dataframe
        df = pd.json_normalize(data['data']['pincodes'])
        df = df.drop('metric.type', axis=1)
        df = df.rename(columns = {'entityName' : 'pincode', 'metric.count' : 'transaction_count', 'metric.amount' : 'amount'})
        #appending year, quarter and state columns to the original dataframe
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df = pd.concat([df,df2], axis=1)
        return(df)

def get_top_user(filepath,quarter,states2):
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read())       
        #normalize the json file and store in dataframe
        df = pd.json_normalize(data['data']['districts']) 
        df = df.rename(columns = {'name' : 'district'})
        #appending year, quarter and state columns to the original dataframe
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df = pd.concat([df,df2], axis=1)
        return(df)

def get_top_pincode_user(filepath,quarter,states2):
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            data = json.loads(file.read()) 
        #normalize the json file and store in dataframe
        df = pd.json_normalize(data['data']['pincodes'])
        df = df.rename(columns = {'name' : 'pincode'})
        #appending year, quarter and state columns to the original dataframe
        df2 = pd.DataFrame([{'year' : yr, 'quarter' : quarter, 'state' : states2} for i in range(len(df))])
        df = pd.concat([df,df2], axis=1)
        return(df)

def df_to_mysql(df,table_name):
    #creating column list for insertion
    cols = ",".join([str(i) for i in df.columns.tolist()])

    # Insert DataFrame recrds one by one.
    for i,row in df.iterrows():
        try: 
            sql = "INSERT INTO "+table_name+" (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row))

            # the connection is not autocommitted by default, so we must commit to save our changes
            mydb.commit()
        except mysql.connector.Error:
            print("Error")
    print("Done")

states = ["andaman-&-nicobar-islands","andhra-pradesh","arunachal-pradesh","assam","bihar","chandigarh","chhattisgarh","dadra-&-nagar-haveli-&-daman-&-diu","delhi","goa","gujarat","haryana",	"himachal-pradesh","jammu-&-kashmir","jharkhand","karnataka","kerala","ladakh","lakshadweep","madhya-pradesh","maharashtra","manipur","meghalaya","mizoram","nagaland","odisha","puducherry","punjab","rajasthan","sikkim","tamil-nadu","telangana","tripura","uttar-pradesh","uttarakhand","west-bengal"]
states2 = ["Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam","Bihar","Chandigarh","Chhattisgarh","Dadra and Nagar Haveli and Daman and Diu","Delhi","Goa","Gujarat","Haryana","Himachal Pradesh","Jammu & Kashmir","Jharkhand","Karnataka","Kerala","Ladakh","Lakshadweep","Madhya Pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Puducherry","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura","Uttar Pradesh","Uttarakhand","West Bengal"]
year = [2018,2019,2020,2021,2022]
json_files = ['1.json','2.json','3.json','4.json']
agg_df = pd.DataFrame()
dis_trans_df = pd.DataFrame()
dis_user_df = pd.DataFrame()
top_trans_df = pd.DataFrame()
top_pincode_trans_df =pd.DataFrame()
top_user_df = pd.DataFrame()
top_pincode_user_df = pd.DataFrame()

k = 0
for state in states:
    for yr in year:
        n = 1
        for file_name in json_files:
            quarter = 'Q'+str(n)
            n += 1
            # #file path for aggregated data-india level
            # filepath1 = f"Desktop/pulse/data/aggregated/transaction/country/india/state/{state}/{yr}/{file_name}"
            # agg_df = pd.concat([agg_df,get_aggregated_data(filepath1,quarter,states2[k])], ignore_index = True)

            # #file path for transaction data-state level
            # filepath2 = f"Desktop/pulse/data/map/transaction/hover/country/india/state/{state}/{yr}/{file_name}"
            # dis_trans_df = pd.concat([dis_trans_df,get_district_trans(filepath2,quarter,states2[k])],ignore_index = True)

            # #file path for user data-state level
            # filepath3 = f"Desktop/pulse/data/map/user/hover/country/india/state/{state}/{yr}/{file_name}"
            # dis_user_df = pd.concat([dis_user_df,get_district_user(filepath3,quarter,states2[k])], ignore_index=True)

            # #file path for top transaction data-state level
            # filepath4 = f"Desktop/pulse/data/top/transaction/country/india/state/{state}/{yr}/{file_name}"
            # top_trans_df = pd.concat([top_trans_df,get_top_trans(filepath4,quarter,states2[k])], ignore_index=True)
            # top_pincode_trans_df = pd.concat([top_pincode_trans_df,get_top_pincode_trans(filepath4,quarter,states2[k])], ignore_index=True)

            #file path for top user data-state level
            filepath5 = f"Desktop/pulse/data/top/user/country/india/state/{state}/{yr}/{file_name}"
            top_user_df = pd.concat([top_user_df,get_top_user(filepath5,quarter,states2[k])], ignore_index=True)
            top_pincode_user_df = pd.concat([top_pincode_user_df,get_top_pincode_user(filepath5,quarter,states2[k])], ignore_index=True)
    k += 1


# df_to_mysql(agg_df,'aggregated_data')

# df_to_mysql(dis_trans_df,'district_trans')

# df_to_mysql(dis_user_df,'district_user')

# df_to_mysql(top_trans_df,'top_trans')

# df_to_mysql(top_pincode_trans_df,'top_pincode_trans')

df_to_mysql(top_user_df,'top_user')

df_to_mysql(top_pincode_user_df,'top_pincode_user')