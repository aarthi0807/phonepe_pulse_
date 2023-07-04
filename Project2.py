import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

#setup mysql db connection and creating cursor object
mydb = mysql.connector.connect(**st.secrets["mysql"])

cursor = mydb.cursor()

#function for converting number into indian crores
def format_indian_rupees(number):
    crore = 10000000
    formatted_amount = "₹{:,.2f} Cr".format(number / crore)
    return formatted_amount

st.set_page_config(layout="wide", page_title="My Streamlit App- Phonepe pulse")

st.title("**:white[Phonepe Pulse - Data Visualization and Exploration]**")
st.divider()

col1,col2,col3 = st.columns([0.7,3,1.5])
with col1:
    #creating select boxes with choices
    option0_type = st.selectbox(('Select an option'),("India-All States","Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam","Bihar","Chandigarh","Chhattisgarh","Dadra and Nagar Haveli and Daman and Diu","Delhi","Goa","Gujarat","Haryana","Himachal Pradesh","Jammu & Kashmir","Jharkhand","Karnataka","Kerala","Ladakh","Lakshadweep","Madhya Pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland","Odisha","Puducherry","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura","Uttar Pradesh","Uttarakhand","West Bengal"))
    option1_type = st.selectbox(('Type'),('Transaction','Users'))
    option2_year = st.selectbox(('Year'),('2018','2019','2020','2021','2022'))
    option3_q = st.selectbox(('Quarter'),('Q1','Q2','Q3','Q4'))

with col2:
    if option0_type == 'India-All States':
        if option1_type == 'Transaction':        
            #fetching data from mysql and storing in df to display it in plotly choropleth map
            sql = "SELECT state,sum(transaction_count) as Transaction_count,sum(amount) as Amount,sum(amount)/sum(transaction_count) as Avg_Transaction_Value from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY state"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)

            fig = px.choropleth(df, 
                        geojson='https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson', 
                        locations='state', 
                        color='Transaction_count',
                        hover_data={"state" : True, "Transaction_count" : True, "Amount" : True, "Avg_Transaction_Value" : True},
                        featureidkey="properties.ST_NM",
                        scope='asia',
                        center={'lat':24,'lon':77})
            fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>'
                                    'Transaction count: %{customdata[1]}<br>'
                                    'Transaction amount: %{customdata[2]}<br>'
                                    'Avg_Transaction_Value: %{customdata[3]}<br>')

        elif option1_type == 'Users':
            #fetching data from mysql and storing in df to display it in plotly choropleth map
            sql = "SELECT state,sum(registeredUsers) as registeredUsers,sum(appOpens) as appOpens from district_user where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY state"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)

            fig = px.choropleth(df, 
                        geojson='https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson', 
                        locations='state', 
                        color='registeredUsers',
                        hover_data={"state" : True, "registeredUsers" : True, "appOpens" : True},
                        featureidkey="properties.ST_NM",
                        scope='asia',
                        center={'lat':24,'lon':77})


            fig.update_traces(hovertemplate='<b>%{customdata[0]}</b><br>'
                                            'Registered Users: %{customdata[1]}<br>'
                                            'App Opens: %{customdata[2]}<br>')

        #Focasing only on location so, it would zoom only on India location
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(margin = {"l":0,"r":0,"t":0,"b":0})
    else:
        if option1_type == "Transaction":
            sql = "SELECT district as District,sum(count) as Transaction_count,sum(amount) as Amount, sum(amount)/sum(count) as Avg_Transaction_Value FROM district_trans WHERE year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY district ORDER BY district"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            fig = px.bar(df, y='Transaction_count', x='District', text_auto='.2s',
                        title="Phonepe Transactions", 
                        hover_data=["Amount","Avg_Transaction_Value"],
                        color = "Transaction_count")
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
        elif option1_type == "Users":
            sql = "SELECT district as District,sum(registeredUsers) as Registered_Users,sum(appOpens) as App_Opens FROM district_user WHERE year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY district ORDER BY district"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            fig = px.bar(df, y='Registered_Users', x='District', text_auto='.2s',
                        title="Phonepe Registered Users", 
                        hover_data=["App_Opens",],
                        color = "Registered_Users")
            fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
            
    
    st.plotly_chart(fig,use_container_width=True,theme=None,config={"displayModeBar": False})

with col3:
    #with If-else condition statement, displaying the data according to options which are selected in col1
    if option1_type == 'Transaction':
        st.subheader('Transactions')
        st.caption('**All Phonepe Transactions(UPI+Cards+Wallets)**')
        if option0_type == 'India-All States':
            sql = "SELECT sum(transaction_count) from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"'"
        else:
            sql = "SELECT sum(transaction_count) from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"'"
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            st.header("{:,.2f}".format(i[0]))
        col4,col5 = st.columns(2)
        with col4:
            st.caption('**Total Payment Value**')
            if option0_type == 'India-All States':
                sql = "SELECT sum(amount) from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"'"
            else:
                sql = "SELECT sum(amount) from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"'"
            cursor.execute(sql)
            result = cursor.fetchall()
            for i in result:
                st.subheader(format_indian_rupees(i[0]))
        with col5:
            st.caption('**Average Transaction Value**')
            if option0_type == 'India-All States':
                sql = "SELECT sum(amount)/sum(transaction_count) from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"'"
            else:
                sql = "SELECT sum(amount)/sum(transaction_count) from aggregated_data where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"'"
            cursor.execute(sql)
            result = cursor.fetchall()
            for i in result:
                st.subheader("₹{:,.2f}".format(i[0])) 
        st.divider()
        st.subheader('Categories')
        if option0_type == 'India-All States':
            sql = "SELECT transaction_name as Transaction_name,sum(transaction_count) as Total_transaction from aggregated_data data where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY transaction_name"
        else:
            sql = "SELECT transaction_name as Transaction_name,sum(transaction_count) as Total_transaction from aggregated_data data where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY transaction_name"
        cursor.execute(sql)
        result = cursor.fetchall()
        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        # Display the DataFrame       
        st.dataframe(df,use_container_width=True)
        st.divider()
        if option0_type == 'India-All States':
            tab1,tab2,tab3 = st.tabs(['States','Districts','Pincode'])

            with tab1:
                st.subheader('Top 10 States')
                sql = "SELECT state as State,sum(transaction_count) as Total_transaction from top_trans where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY state ORDER BY sum(transaction_count) DESC LIMIT 10"
                cursor.execute(sql)
                result = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=column_names)
                st.dataframe(df,use_container_width=True)
        else:
            tab2,tab3 = st.tabs(['Districts','Pincode'])
        
        with tab2:
            st.subheader('Top 10 Districts')
            if option0_type == 'India-All States':
                sql = "SELECT district as District,sum(transaction_count) as Total_transaction from top_trans where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY district ORDER BY sum(transaction_count) DESC LIMIT 10"
            else:
                sql = "SELECT district as District,sum(transaction_count) as Total_transaction from top_trans where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY district ORDER BY sum(transaction_count) DESC LIMIT 10"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            st.dataframe(df,use_container_width=True)
        with tab3:
            st.subheader('Top 10 Pincodes')
            if option0_type == 'India-All States':
                sql = "SELECT pincode as Pincode,sum(transaction_count) as Total_transaction from top_pincode_trans where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY pincode ORDER BY sum(transaction_count) DESC LIMIT 10"
            else:
                sql = "SELECT pincode as Pincode,sum(transaction_count) as Total_transaction from top_pincode_trans where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY pincode ORDER BY sum(transaction_count) DESC LIMIT 10"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            st.dataframe(df,use_container_width=True)

    elif option1_type == 'Users':
        st.subheader('Users')
        st.caption('**Registered Phonepe Users**')
        if option0_type == 'India-All States':
            sql = "SELECT sum(registeredUsers) from district_user where year = "+option2_year+" and quarter = '"+option3_q+"'"
        else:
            sql = "SELECT sum(registeredUsers) from district_user where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"'"
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            st.header("{:,.2f}".format(i[0]))       
        st.caption('**Phonepe App Opens**')
        if option0_type == 'India-All States':
            sql = "SELECT sum(appOpens) from district_user where year = "+option2_year+" and quarter = '"+option3_q+"'"
        else:
            sql = "SELECT sum(appOpens) from district_user where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"'"
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            st.header("{:,.2f}".format(i[0]))
        st.divider()
        if option0_type == 'India-All States':
            tab1,tab2,tab3 = st.tabs(['States','Districts','Pincode'])
            with tab1:
                st.subheader('Top 10 States')
                sql = "SELECT state as State,sum(registeredUsers) as Registered_Users from top_user where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY state ORDER BY sum(registeredUsers) DESC LIMIT 10"
                cursor.execute(sql)
                result = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=column_names)
                st.dataframe(df,use_container_width=True)
        else:
            tab2,tab3 = st.tabs(['Districts','Pincode'])
        with tab2:
            st.subheader('Top 10 Districts')
            if option0_type == 'India-All States':
                sql = "SELECT district as District,sum(registeredUsers) as Registered_Users from top_user where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY district ORDER BY sum(registeredUsers) DESC LIMIT 10"
            else:
                sql = "SELECT district as District,sum(registeredUsers) as Registered_Users from top_user where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY district ORDER BY sum(registeredUsers) DESC LIMIT 10"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            st.dataframe(df,use_container_width=True)
        with tab3:
            st.subheader('Top 10 Pincodes')
            if option0_type == 'India-All States':
                sql = "SELECT pincode as Pincode,sum(registeredUsers) as Registered_Users from top_pincode_user where year = "+option2_year+" and quarter = '"+option3_q+"' GROUP BY pincode ORDER BY sum(registeredUsers) DESC LIMIT 10"
            else:
                sql = "SELECT pincode as Pincode,sum(registeredUsers) as Registered_Users from top_pincode_user where year = "+option2_year+" and quarter = '"+option3_q+"' and state = '"+option0_type+"' GROUP BY pincode ORDER BY sum(registeredUsers) DESC LIMIT 10"
            cursor.execute(sql)
            result = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_names)
            st.dataframe(df,use_container_width=True)
cursor.close()
mydb.close()
