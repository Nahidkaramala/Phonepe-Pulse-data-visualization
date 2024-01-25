import pandas as pd
import json
import os
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import seaborn as sns
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

    Map_user = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Registered_user': [], 'App_opens': []}

    for i in agg_state_list:
                path_i = path + i + "/"
                agg_year_list = os.listdir(path_i)

                for j in agg_year_list:
                    path_j = path_i + j + "/"
                    agg_year_json = os.listdir(path_j)
                    
                    for k in agg_year_json:
                        path_k = path_j + k
                        Data=open(path_k,'r')
                        d=json.load(Data)
                    

                    try:
                        for z_key, z_value in d['data']['hoverData'].items():
                            district = z_key.split(' district')[0]
                            reg_user = z_value['registeredUsers']
                            app_opens = z_value['appOpens']

                            Map_user['State'].append(i)
                            Map_user['Year'].append(j)
                            Map_user['Quarter'].append('Q'+str(k[0]))
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
                      'Quarter': sqlalchemy.types.VARCHAR(10),
                      'District': sqlalchemy.types.VARCHAR(length=50), 
                      'Registered_User': sqlalchemy.types.Integer, 
                      'App_opens':sqlalchemy.types.Integer})
    
    # return df3
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
    # return df7

        
        
    

        
# Agg_Trans.to_csv('agg_trans.csv',index=False)
# Agg_user.to_csv('agg_user.csv',index=False)
# Agg_map_Tran.to_csv('map_trans.csv',index=False)
# Agg_map_user.to_csv('map_user.csv',index=False)
# df_top_trans.to_csv('top_trans_dist.csv',index=False)
# df_top_trans_pin.to_csv('top_trans_pincode.csv',index=False)
# df_top_user_district.to_csv('top_user_dist.csv',index=False)
# df_top_user_pincode.to_csv('top_user_pincode.csv',index=False)



####------------Streamlit Part----------####



def state_list():
    cursor.execute("""SELECT  distinct State FROM phonepe_data_visualization.agg_transdata;""")
    s = cursor.fetchall()
    state = [i[0] for i in s]
    return state

def state_dict():
    original = state_list()
    geo = geo_state_list()
    data = {}
    for i in range(0, len(original)):
        data[original[i]] = geo[i]
    return data

def state_list_val(data):
    dat = []
    for key, val in data.items():
        dat.append(val)
    return dat

def year_list():
    cursor.execute(
        """SELECT distinct Year FROM phonepe_data_visualization.agg_userdata;""")
    data = cursor.fetchall()
    data = [i[0] for i in data]
    return data

def Brand_names():
    cursor.execute("""SELECT distinct Brands FROM phonepe_data_visualization.agg_userdata;""")
    data=cursor.fetchall()
    data=[i[0] for i in data]
    return data

def quarter_list():
    cursor.execute(
        """SELECT distinct Quarter FROM phonepe_data_visualization.agg_userdata;""")
    data = cursor.fetchall()
    data = [i[0] for i in data]
    return data

def get_trans_data():
                cursor.execute(
                    '''SELECT * FROM phonepe_data_visualization.agg_transdata;''')
                column_names = [column[0] for column in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=column_names)
                return df
        
def agg_trans_avg(agg_trans):
    data = []
    for i in range(0, len(agg_trans)):
        avg = agg_trans.iloc[i]["Transaction_amount"] / agg_trans.iloc[i]["Transaction_count"]
        data.append(avg)
    return data

def transaction_type():
    cursor.execute("""SELECT distinct Transaction_type FROM phonepe_data_visualization.agg_transdata;""")
    data = cursor.fetchall()
    data = [i[0] for i in data]
    return data

def get_agg_users():
    cursor.execute("""SELECT * FROM phonepe_data_visualization.agg_map_user;""")
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    d= pd.DataFrame(data, columns=columns)
    return d

def get_map_users():
    cursor.execute("""SELECT * FROM phonepe_data_visualization.agg_map_user;""")
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    d = pd.DataFrame(data, columns=columns)
    return d


def get_map_transaction():
    cursor.execute("""SELECT * FROM phonepe_data_visualization.agg_map_transaction;""")
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    d= pd.DataFrame(data, columns=columns)
    return d

def agg_user_brands():
    cursor.execute("""SELECT * FROM phonepe_data_visualization.agg_userdata;""")
    data = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    return df


def new_frame(v):
    i = [i for i in range(1, len(v)+1)]
    data = pd.DataFrame(v.values, columns=v.columns, index=i)
    return data

###################################################################################
st.title=":violet[Phone pe pulse data visualization]"
st.set_page_config(layout="wide")
selected = option_menu(None,
    options=["About", "Transactions-Data",
            "Users-Data"],
    icons=["house", "cash-coin", "bi-people"],
    default_index=0,
    orientation="horizontal",
    styles={"container": {"width": "100%"},
            "options": {"margin": "10px"},
            "icon": {"color": "black", "font-size": "24px"},
            "nav-link": {"font-size": "24px", "text-align": "center", "margin": "15px", "--hover-color": "#6F36AD"},
            "nav-link-selected": {"background-color": "#6F36AD"}})


##################################################

if selected == "About":

    im1 = Image.open("ICN.png")
    st.image(im1, width=500)

    col1, col2 = st.columns(2)

    with col1:
        im = Image.open("info.png.jpg")
        st.markdown("")
        st.image(im)

    with col2:
        st.markdown("#### India with 89.5 million digital transactions in the year 2022 has topped the list of five countries in digital payments, according to data from MyGovIndia")
        st.markdown(
            "#### India is number one in digital payments. India is one of the countries where mobile data is the cheapest")
        st.markdown("#### The Reserve Bank of India’s ‘Payments Vision 2025’ document observes that ‘payment systems foster economic development and financial stability’ while supporting financial inclusion")
        st.markdown("#### The adoption of digital payment methods, while accelerated by the COVID-19 pandemic, has also been enabled by the widening number of banks which have backed the UPI system")

        st.markdown("#### to make United Payments Interface (UPI) more user-friendly,RBI has proposed incorporating Artificial Intelligence-powered conversational features on UPI, to enable digital payments through voice commands")

   


    with st.sidebar:
        message = "<p style='color: violet; font-size: 35px;'>For hassle-free easy payments, download the app now</p>"
        st.markdown(message, unsafe_allow_html=True)
       
        if st.button("Download the app"):
            applink = ['Mac iOS', 'Android']
            selected = st.selectbox("Select the device", applink)
            if selected == 'Mac iOS':
                st.markdown("[Download PhonePe for Mac iOS](https://ppe.onelink.me/rPjp/2kk1w03o)")
            elif selected == 'Android':
                st.markdown("[Download PhonePe for Android](https://ppe.onelink.me/rPjp/2kk1w03o)")

        message = "<p style='color: blue; font-size: 45px;'>Fintech</p>"
        st.markdown(message, unsafe_allow_html=True)


################################################################################

if selected == "Transactions-Data":
    with st.container():
        st.markdown("#### :green[TRANSACTIONS INSIGHTS]")
        col1, col2, col3 = st.columns(3)
       
        with col1:
            state = st.selectbox(label="Select the state",
                                options=state_list(), index=0)
        with col2:
            year = st.selectbox(label="Select the year",
                                options=year_list(), index=0)
        with col3:
            quarter = st.selectbox(
                label="Select the Quarter", options=quarter_list(), index=0)
           


    col1,col2,col3= st.columns(3)
    with col1:
        connection_params = {
                'host': 'localhost',
                'user': 'root',
                'password': 'Nahid123',
            }
        connection = pymysql.connect(**connection_params)
        cursor = connection.cursor()

        

        df_agg_tran = get_trans_data()
        avg_value = agg_trans_avg(df_agg_tran)
        avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
        df_av = pd.concat([df_agg_tran, avg_value], axis=1)
        v = df_av[(df_av["Year"] == year) & (df_av["Quarter"] == quarter)
                    & (df_av["State"] == state)]
        plt.figure(figsize=(8, 4))
        fig = px.bar(v, x="Transaction_type", y="Transaction_count",
                color="Transaction_type", log_y=True,
                labels={"Transaction_count": "Count", "Transaction_type": "Transaction Type"},
                title="Bar Plot of Transaction Count by Type")
        fig.update_xaxes(title_text="Transaction Type", tickangle=90)

        st.write(fig)
    
    with col2:
        st.empty()

    with col3:
            st.markdown("")
            new_v = new_frame(v)
            st.table(new_v)



    col1, col2= st.columns(2)
    with col1:
        year_df = st.selectbox(label="Select year",
                            options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)

    with col2:
        transaction_type = st.selectbox(label="Select the transaction type",
                                        options=transaction_type(), index=0)
    


    col1, col2= st.columns(2)
    with col1:
        df_agg_total = get_trans_data()
        df_agg_total = df_agg_total.groupby(["State", "Year", "Transaction_type"])[
            ["Transaction_count", "Transaction_amount"]].sum().reset_index()
        df_agg_avg = agg_trans_avg(df_agg_total)
        df_agg_avg = pd.DataFrame(df_agg_avg, columns=["Avg_value"])
        df_agg_total = pd.concat([df_agg_total, df_agg_avg], axis=1)
        q = df_agg_total[(df_agg_total["Year"] == year_df) & (
            df_agg_total["Transaction_type"] == transaction_type)]

        fig = px.bar(q, x='State', y='Transaction_count',
                    hover_data=['State', 'Transaction_count'], height=500, title="Transaction count state wise")
        st.write(fig)

    with col2:
        data = state_list()
        
        fig = px.choropleth(q,
                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                    featureidkey='properties.ST_NM',
                    locations='State',
                    color='Transaction_count',  
                    color_continuous_scale='blues',
                    title="Transaction count state wise",
                    height=500)
        fig.update_geos(fitbounds='locations', visible=False)
        fig.update_layout(width=700)
        st.write(fig)




    year_df_d = st.selectbox(label="Select year for the district wise data",
                                options=(2018, 2019, 2020, 2021, 2022, 2023), index=0)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("#### Top 15 distircts for Transaction Count wise")
        df = get_map_transaction()
        df = df.groupby(["Year", "District"])[
            ["Transaction_count", "Transaction_amount"]].sum().reset_index()
        avg_value = agg_trans_avg(df)
        avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
        df_av_dis = pd.concat([df, avg_value], axis=1)
        k = df_av_dis[df_av_dis["Year"] == year_df_d]
        c = k.sort_values(by=["Transaction_count"],
                            ascending=False).head(15)
        c = c[["Year", "District", "Transaction_count"]]
        c_df = new_frame(c)
        st.table(c_df)
    with col2:
        st.markdown("#### Top 15 distircts for Transaction Amount wise")
        df = get_map_transaction()
        df = df.groupby(["Year", "District"])[
            ["Transaction_count", "Transaction_amount"]].sum().reset_index()
        avg_value = agg_trans_avg(df)
        avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
        df_av_dis = pd.concat([df, avg_value], axis=1)
        k = df_av_dis[df_av_dis["Year"] == year_df_d]
        c = k.sort_values(by=["Transaction_amount"],
                            ascending=False).head(15)
        c = c[["Year", "District", "Transaction_amount"]]
        c_df = new_frame(c)
        st.table(c_df)
    with col3:
        st.markdown("#### Top 15 distircts Per capita Transaction Amount")
        df = get_map_transaction()
        df = df.groupby(["Year", "District"])[
            ["Transaction_count", "Transaction_amount"]].sum().reset_index()
        avg_value = agg_trans_avg(df)
        avg_value = pd.DataFrame(avg_value, columns=["avg_value"])
        df_av_dis = pd.concat([df, avg_value], axis=1)
        k = df_av_dis[df_av_dis["Year"] == year_df_d]
        c = k.sort_values(by=["avg_value"],
                            ascending=False).head(15)
        c = c[["Year", "District", "avg_value"]]
        c_df = new_frame(c)
        st.table(c_df)

    ##########################################
        
if selected == "Users-Data":
    st.markdown("#### :blue[USERS Data]")
    col1,col2,col3=st.columns(3)
    with col1:
        map_year = st.selectbox(label="Select an year",options=(2019, 2020, 2021, 2022, 2023), index=0)
    
    with col2:
        options=["Registered_user", "App_opens","User_Count"]
        select1=st.selectbox("select to view",options)
        if select1 =="Registered_user":
            total_user = get_map_users()
            total_user = total_user.groupby(["State", "Year",])[["Registered_user", "App_opens"]].sum().reset_index()
            mp =total_user[total_user["Year"] == map_year]
            data =state_list()

            fig = px.choropleth(mp,
                                    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                    featureidkey='properties.ST_NM',
                                    locations=data,
                                    color='Registered_user',
                                    color_continuous_scale="greens",
                                    title="Registeres Users state wise",
                                            height=1000, width=750)
            fig.update_geos(fitbounds='locations', visible=False)
            st.write(fig)

        if select1 =="App_opens":
            total_user = get_map_users()
            total_user = total_user.groupby(["State", "Year",])[["Registered_user", "App_opens"]].sum().reset_index()
            mp =total_user[total_user["Year"] == map_year]
            data =state_list()
            fig = px.choropleth(mp,
                                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                featureidkey='properties.ST_NM',
                                locations=data,
                                color='App_opens',
                                color_continuous_scale="blues",
                                title="App opens state wise",
                                        height=750, width=750)
            fig.update_geos(fitbounds='locations', visible=False)
            st.write(fig)

        
        if select1 == "User_Count":
            opt = ["User_Count State-wise", "User_Count Brand-wise"]
            std = st.selectbox("Select To View", opt)
            
            if std == 'User_Count State-wise':
                user_brands = agg_user_brands()
                total_user = user_brands.groupby(["State", "Year",])[["Brands", "User_Count"]].sum().reset_index()
                mp = total_user[total_user["Year"] == map_year]
                data = state_list()
                
                if not mp.empty:
                    fig = px.choropleth(mp,
                                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                                        featureidkey='properties.ST_NM',
                                        locations=data,
                                        color='User_Count',
                                        color_continuous_scale="reds",
                                        title="User-Count state-wise",
                                        height=750, width=750)
                    fig.update_geos(fitbounds='locations', visible=False)
                    st.write(fig)
                else:
                    st.warning("No data available for the selected year.")
       


            if std == 'User_Count Brand-wise':
                user_brands = agg_user_brands()
                total_user = user_brands.groupby(["State", "Year", "Brands"])[["User_Count"]].sum().reset_index()
                mp = total_user[total_user["Year"] == map_year]
                data = state_list()
                colors = sns.color_palette('husl', len(Brand_names()))
                if not mp.empty:
                    fig = px.bar(mp, x="Brands", y="User_Count",
                                color="User_Count", log_y=True,
                                labels={"Brands": "Transaction Type", "User_Count": "User Count"},
                                title="Bar Plot of User-Count by Brands",
                                color_discrete_sequence=colors)  # Use color_discrete_sequence to specify colors
                                
                    fig.update_xaxes(title_text="Transaction Type", tickangle=90)
                    st.write(fig)
                else:
                    st.warning("No data available for the selected year.")

    
