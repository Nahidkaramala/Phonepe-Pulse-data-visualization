import pandas as pd
import json
import os
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt
from PIL import Image

connection_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Nahid123',
}
connection = pymysql.connect(**connection_params)
cursor = connection.cursor()
database_name = 'Phonepe_data_visualization'
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
connection = pymysql.connect(host= 'localhost',user = 'root',password = 'Nahid123',database = 'Phonepe_data_visualization')
cursor = connection.cursor()

engine = create_engine('mysql+pymysql://root:Nahid123@localhost/Phonepe_data_visualization', echo=False)

def data_collection():
    try:
        git.Repo.clone_from("https://github.com/PhonePe/pulse.git", 'phonepe_pulse_git')
    except:
        pass

import requests
def geo_state_list():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data = json.loads(response.content)
    geo_state = [i['properties']['ST_NM'] for i in data['features']]
    geo_state.sort(reverse=False)
    return geo_state
custom_state_list=geo_state_list()

def Aggregate_Transaction():
    
    path="pulse/data/aggregated/transaction/country/india/state/"
    Agg_state_list=os.listdir(path)
    
    Agg_trans_data={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}

    for i in Agg_state_list:
        p_i=path+i+"/"
        Agg_yr=os.listdir(p_i)
        for j in Agg_yr:
            p_j=p_i+j+"/"
            Agg_yr_list=os.listdir(p_j)
            for k in Agg_yr_list:
                p_k=p_j+k
                Data=open(p_k,'r')
                D=json.load(Data)
                
                try:
                    for z in D['data']['transactionData']:
                      Name=z['name']
                      count=z['paymentInstruments'][0]['count']
                      amount=z['paymentInstruments'][0]['amount']
                      Agg_trans_data['Transacion_type'].append(Name)
                      Agg_trans_data['Transacion_count'].append(count)
                      Agg_trans_data['Transacion_amount'].append(amount)
                      Agg_trans_data['State'].append(i)
                      Agg_trans_data['Year'].append(j)
                      Agg_trans_data['Quater'].append(int(k.strip('.json')))
                except:
                    pass

    
    
    Agg_Trans=pd.DataFrame(Agg_trans_data)
    Agg_Trans['State'] = Agg_Trans['State'].replace(dict(zip(Agg_state_list, custom_state_list)))
    
    df=Agg_Trans.to_sql('agg_transdata', engine, if_exists='replace', index=False,
                            dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                   'Year': sqlalchemy.types.Integer, 
                                   'Quater': sqlalchemy.types.Integer, 
                                   'Transacion_type': sqlalchemy.types.VARCHAR(length=50), 
                                   'Transacion_count': sqlalchemy.types.Integer,
                                   'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
    

    return Agg_Trans


def Aggregated_User():
    path_2 = "pulse/data/aggregated/user/country/india/state/"
    Agg_user_state_list = os.listdir(path_2)

    Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

    for i in Agg_user_state_list:
        p_i = path_2 + i + "/"
        Agg_yr = os.listdir(p_i)

        for j in Agg_yr:
            p_j = p_i + j + "/"
            Agg_yr_list = os.listdir(p_j)

            for k in Agg_yr_list:
                p_k = p_j + k
                Data = open(p_k, 'r')
                B = json.load(Data)
                try:

                    for l in B["data"]["usersByDevice"]:
                        brand_name = l["brand"]
                        count_ = l["count"]
                        ALL_percentage = l["percentage"]
                        Agg_user["State"].append(i)
                        Agg_user["Year"].append(j)
                        Agg_user["Quarter"].append(int(k.strip('.json')))
                        Agg_user["Brands"].append(brand_name)
                        Agg_user["User_Count"].append(count_)
                        Agg_user["User_Percentage"].append(ALL_percentage*100)
                except:
                    pass

    Agg_user_d=pd.DataFrame(Agg_user)
    
    Agg_user_d['State'] = Agg_user_d['State'].replace(dict(zip(Agg_user_state_list, custom_state_list)))

    
    df1 = Agg_user_d.to_sql('agg_userdata', engine, if_exists='replace', index=False,
                  dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                         'Year': sqlalchemy.types.Integer, 
                         'Quater': sqlalchemy.types.Integer,
                         'Brands': sqlalchemy.types.VARCHAR(length=50), 
                         'User_Count': sqlalchemy.types.Integer, 
                         'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
    
    return Agg_user_d

def Map_Transaction():    

    path3 = "pulse/data/map/transaction/hover/country/india/state/"

    agg_state_list = os.listdir(path3)

    Map_trans = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
            'Transaction_count': [], 'Transaction_amount': []}

    for i in agg_state_list:
        path_i = path3 + i + '/'                 
        
       
        if os.path.exists(path_i):
            agg_year_list = os.listdir(path_i)      
            
            for j in agg_year_list:
                path_j = path_i + j + '/'          
                agg_year_json = os.listdir(path_j)  
                
                for k in agg_year_json:
                    path_k = path_j + k             
                    with open(path_k, 'r') as f:
                        d = json.load(f)
                    
                    try:
                        for z in d['data']['hoverDataList']:
                            district = z['name'].split(' district')[0]
                            count = z['metric'][0]['count']
                            amount = z['metric'][0]['amount']

                            Map_trans['State'].append(i)
                            Map_trans['Year'].append(j)
                            Map_trans['Quarter'].append('Q' + str(k[0]))
                            Map_trans['District'].append(district)
                            Map_trans['Transaction_count'].append(count)
                            Map_trans['Transaction_amount'].append(amount)
                    except:
                        pass
                
    Agg_map_Tran = pd.DataFrame(Map_trans)
    Agg_map_Tran['State'] = Agg_map_Tran['State'].replace(dict(zip(agg_state_list, custom_state_list)))
    
    df2 = Agg_map_Tran.to_sql('agg_map_transaction', engine, if_exists = 'replace', index=False,
                      dtype={'State':sqlalchemy.types.VARCHAR(100),
                            'Year':sqlalchemy.types.Integer,
                            'Quarter': sqlalchemy.types.VARCHAR(10),
                            'District': sqlalchemy.types.VARCHAR(100),
                            'Transaction_count': sqlalchemy.types.Integer,
                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})

    return Agg_map_Tran


def map_user():
       
    path = "pulse/data/map/user/hover/country/india/state/"
    agg_state_list = os.listdir(path)

    Map_user = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Registered_user': [], 'App_opens': []}

    for i in agg_state_list:
        path_i = path + i + '/'
        agg_year_list = os.listdir(path_i)

    for j in agg_year_list:
        path_j = path_i + j + '/'
        agg_year_json = os.listdir(path_j)

        for k in agg_year_json:
            path_k = path_j + k
            f = open(path_k, 'r')
            d = json.load(f)

            try:
                for z_key, z_value in d['data']['hoverData'].items():
                    district = z_key.split(' district')[0]
                    reg_user = z_value['registeredUsers']
                    app_opens = z_value['appOpens']

                    Map_user['State'].append(i)
                    Map_user['Year'].append(j)
                    Map_user['Quater'].append('Q'+str(k[0]))
                    Map_user['District'].append(district)
                    Map_user['Registered_user'].append(reg_user)
                    Map_user['App_opens'].append(app_opens)
            except:
                pass
    Agg_map_user=pd.DataFrame(Map_user)
    Agg_map_user['State'] = Agg_map_user['State'].replace(dict(zip(agg_state_list, custom_state_list)))
    
    df3 = Agg_map_user.to_sql('agg_map_user', engine, if_exists = 'replace', index=False,
               dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                      'Year': sqlalchemy.types.Integer, 
                      'Quater': sqlalchemy.types.VARCHAR(10),
                      'District': sqlalchemy.types.VARCHAR(length=50), 
                      'Registered_User': sqlalchemy.types.Integer, 
                      'App_opens':sqlalchemy.types.Integer})
    
    return Agg_map_user

def top_transaction_district():
       
    path = "pulse/data/top/transaction/country/india/state/"
    agg_state_list = os.listdir(path)

    Top_trans_dist = {'State': [], 'Year': [], 'Quarter': [], 'District': [],
            'Transaction_count': [], 'Transaction_amount': []}

    for i in agg_state_list:
        path_i = path + i + '/'                 
        agg_year_list = os.listdir(path_i)      

        for j in agg_year_list:
            path_j = path_i + j + '/'           
            agg_year_json = os.listdir(path_j)  

            for k in agg_year_json:
                path_k = path_j + k             
                f = open(path_k, 'r')
                d = json.load(f)
                try:
                    for z in d['data']['districts']:
                        district = z['entityName']
                        count = z['metric']['count']
                        amount = z['metric']['amount']


                        Top_trans_dist['State'].append(i)
                        Top_trans_dist['Year'].append(j)
                        Top_trans_dist['Quarter'].append('Q'+str(k[0]))
                        Top_trans_dist['District'].append(district)
                        Top_trans_dist['Transaction_count'].append(count)
                        Top_trans_dist['Transaction_amount'].append(amount)  
                except:
                    pass

    df_top_trans=pd.DataFrame(Top_trans_dist)
    df_top_trans['State'] = df_top_trans['State'].replace(dict(zip(agg_state_list, custom_state_list)))
    
    
    df4 = df_top_trans.to_sql('top_transaction_district', engine, if_exists = 'replace', index=False,
                     dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                            'Year': sqlalchemy.types.Integer, 
                            'Quarter': sqlalchemy.types.VARCHAR(10),   
                            'District': sqlalchemy.types.VARCHAR(100),
                            'Transaction_count': sqlalchemy.types.Integer, 
                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
    
    return df_top_trans

def top_transaction_pincode():
    path = "pulse/data/top/transaction/country/india/state/"
    agg_state_list = os.listdir(path)

    Top_trans_PC = {'State': [], 'Year': [], 'Quater': [], 'Pincode': [],
            'Transaction_count': [], 'Transaction_amount': []}

    for i in agg_state_list:
        path_i = path + i + '/'                 
        agg_year_list = os.listdir(path_i)      

        for j in agg_year_list:
            path_j = path_i + j + '/'           
            agg_year_json = os.listdir(path_j)  

            for k in agg_year_json:
                path_k = path_j + k             
                f = open(path_k, 'r')
                d = json.load(f)
                try:
                    for z in d['data']["pincodes"]:
                        pincode = z['entityName']
                        count = z['metric']['count']
                        amount = z['metric']['amount']


                        Top_trans_PC['State'].append(i)
                        Top_trans_PC['Year'].append(j)
                        Top_trans_PC['Quater'].append('Q'+str(k[0]))
                        Top_trans_PC['Pincode'].append(pincode)
                        Top_trans_PC['Transaction_count'].append(count)
                        Top_trans_PC['Transaction_amount'].append(amount)  
                except:
                    pass

    df_top_trans_pin=pd.DataFrame(Top_trans_PC)
    df_top_trans_pin['State'] = df_top_trans_pin['State'].replace(dict(zip(agg_state_list, custom_state_list)))
    
    df5 = df_top_trans_pin.to_sql('top_transaction_pincode', engine, if_exists = 'replace', index=False,
                     dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                            'Year': sqlalchemy.types.Integer, 
                            'Quarter': sqlalchemy.types.Integer,   
                            'Pincode': sqlalchemy.types.Integer,
                            'Transaction_count': sqlalchemy.types.Integer, 
                            'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})
    return df_top_trans_pin

def top_user_districts():
    
    path = "pulse/data/top/user/country/india/state/"
    agg_state_list = os.listdir(path)

    Top_user_dist = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Registered_user': []}

    for i in agg_state_list:
        path_i = path + i + '/'                 
        agg_year_list = os.listdir(path_i)      

        for j in agg_year_list:
            path_j = path_i + j + '/'           
            agg_year_json = os.listdir(path_j)  

            for k in agg_year_json:
                path_k = path_j + k             
                f = open(path_k, 'r')
                d = json.load(f)
                
                try:
                    for z in d['data']["districts"]:
                        district=z["name"]
                        reg_user = z['registeredUsers']
                        Top_user_dist['State'].append(i)
                        Top_user_dist['Year'].append(j)
                        Top_user_dist['Quater'].append('Q'+str(k[0]))                    
                        Top_user_dist['District'].append(district)
                        Top_user_dist['Registered_user'].append(reg_user)
                except:
                    pass
                    
    df_top_user_district=pd.DataFrame(Top_user_dist)
    df_top_user_district['State'] = df_top_user_district['State'].replace(dict(zip(agg_state_list, custom_state_list)))
    
    df6 = df_top_user_district.to_sql('top_user_district', engine, if_exists = 'replace', index=False,
                     dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                            'Year': sqlalchemy.types.Integer, 
                            'Quarter': sqlalchemy.types.VARCHAR(10),   
                            'District': sqlalchemy.types.VARCHAR(100),
                            'Registered_user': sqlalchemy.types.Integer, 
                            })
    return df_top_user_district


def top_user_pincode():
    path = "pulse/data/top/user/country/india/state/"
    agg_state_list = os.listdir(path)

    Top_user_PC = {'State': [], 'Year': [], 'Quater': [], 'Pincode': [], 'Registered_user': []}

    for i in agg_state_list:
        path_i = path + i + '/'                 
        agg_year_list = os.listdir(path_i)      

        for j in agg_year_list:
            path_j = path_i + j + '/'           
            agg_year_json = os.listdir(path_j)  

            for k in agg_year_json:
                path_k = path_j + k             
                f = open(path_k, 'r')
                d = json.load(f)
                
                try:
                    for z in d['data']['pincodes']:
                        pincode = z['name']
                        reg_user = z['registeredUsers']
                        Top_user_PC['State'].append(i)
                        Top_user_PC['Year'].append(j)
                        Top_user_PC['Quater'].append('Q'+str(k[0]))                    
                        Top_user_PC['Pincode'].append(pincode)
                        Top_user_PC['Registered_user'].append(reg_user)
                except:
                    pass
                
    df_top_user_pincode=pd.DataFrame(Top_user_PC)
    df_top_user_pincode['State'] = df_top_user_pincode['State'].replace(dict(zip(agg_state_list, custom_state_list)))
    
    df7 = df_top_user_pincode.to_sql('top_user_pincode', engine, if_exists = 'replace', index=False,
                     dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                            'Year': sqlalchemy.types.Integer, 
                            'Quarter': sqlalchemy.types.Integer,   
                            'District': sqlalchemy.types.VARCHAR(100),
                            'Registered_user': sqlalchemy.types.Integer})
    return df_top_user_pincode

        
# Agg_Trans.to_csv('agg_trans.csv',index=False)
# Agg_user.to_csv('agg_user.csv',index=False)
# Agg_map_Tran.to_csv('map_trans.csv',index=False)
# Agg_map_user.to_csv('map_user.csv',index=False)
# df_top_trans.to_csv('top_trans_dist.csv',index=False)
# df_top_trans_pin.to_csv('top_trans_pincode.csv',index=False)
# df_top_user_district.to_csv('top_user_dist.csv',index=False)
# df_top_user_pincode.to_csv('top_user_pincode.csv',index=False)


def agg_trans_avg(Agg_Trans):
    Agg_Trans = Agg_Trans[Agg_Trans["Transacion_count"] != 0]

    avg = Agg_Trans["Transacion_amount"] / Agg_Trans["Transacion_count"]

    return avg.tolist()


# file_name = 'output_data2018.csv'
file_path = r'C:\Users\HP\Desktop\virenv\output_data2018.csv'

df=pd.read_csv(file_path)
fig = px.choropleth(df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Transactions',
    color_continuous_scale='greens'
)
fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(title_text='Transaction Counts by State in 2018')

# fig.show()


st.set_page_config(
    page_title="phonepe data visualization",
    page_icon="👋",
)
st.title(":violet[phonepe data visualization]")

image_path = "C:/Users/HP/Desktop/icn.png"        
original_image = Image.open(image_path)
new_size = (int(original_image.width * 0.1), int(original_image.height * 0.1))
resized_image = original_image.resize(new_size)
st.image(resized_image, use_column_width=True)

st.info(
                """
                #### 
                PhonePe is a digital payments platform in India, offering a wide range of financial services, including
                money transfers, bill payments, and online purchases. Launched in 2015, it operates through a mobile app 
                and provides a secure and convenient way for users to manage their finances. PhonePe supports Unified 
                Payments Interface (UPI), allowing seamless transactions between bank accounts. The platform has gained
                popularity for its user-friendly interface and extensive network of merchants, making it one of the 
                leading digital payment solutions in the country. As of my last knowledge update in January 2022, 
                PhonePe is known for its innovative features and continuous expansion into various financial services.
                """,icon="🔍"
                )

if st.button("show 2018 Aggregate Trans data"):
    col1,clo2=st.columns(2)
    with col1:
        st.write(fig.show())

with st.sidebar:
    st.info("""For hassle-free easy payments, download the app now""")
    if st.button("Download the app"):
        applink = ['Mac iOS', 'Android']
        selected = st.selectbox("Select the device", applink)
        if selected == 'Mac iOS':
            st.markdown("[Download PhonePe for Mac iOS](https://ppe.onelink.me/rPjp/2kk1w03o)")
        elif selected == 'Android':
            st.markdown("[Download PhonePe for Android](https://ppe.onelink.me/rPjp/2kk1w03o)")

# with st.sidebar:
#     st.info(""" For hassle free easy payments download the app now""")
#     if st.button("Download the app"):
#         applink=['mac ios','Andriod']
#         selected=st.selectbox("Select the device", applink)
#         if selected=='mac ios':
#             st.write("https://ppe.onelink.me/rPjp/2kk1w03o")
#         elif selected=='Andriod':
#             st.write(" https://ppe.onelink.me/rPjp/2kk1w03o")
    
