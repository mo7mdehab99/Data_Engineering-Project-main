from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import os
import json
import math
import numpy as np

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2022,1,1),
    #'retries':0
}
dag=DAG(
    dag_id="TauTeam_MS3",
 default_args=default_args)
 #schedule_interval='@once')

os.chdir("/c/airflow/dags")

def Extract(**kwargs):
    df_medals = pd.read_csv('Medals.csv',encoding = "ISO-8859-1")
    df_noc_regions=pd.read_csv('noc_regions.csv')
    df_olympics=pd.read_csv('athlete_events.csv')

    #df_olympics=df_olympics.head(100)
    df1=df_medals.to_json(orient='index')
    df2=df_noc_regions.to_json(orient='index')
    df3=df_olympics.to_json(orient='index')
    
    kwargs['ti'].xcom_push(key='medals_dataset',value=df1)
    kwargs['ti'].xcom_push(key='noc_regions_dataset',value=df2)
    kwargs['ti'].xcom_push(key='olympics_dataset',value=df3)
   
######################################## Data Cleaning #########################################################
def Data_Cleaning(**kwargs):
    #df1_json= kwargs['ti'].xcom_pull(key='medals_dataset', task_ids='extract_data')
    #df2_json =kwargs['ti'].xcom_pull(key='noc_regions_dataset', task_ids='extract_data')
    df3_json=kwargs['ti'].xcom_pull(key='olympics_dataset', task_ids='extract_data')
    

    #df_medals=pd.read_json(df1_json, orient ='index',dtype=False)
    #df_noc_regions=pd.read_json(df2_json, orient ='index',dtype=False)
    df_olympics=pd.read_json(df3_json, orient ='index',dtype=False)
    
    #1)Handling Outliers:

    df_olympics_age_outliers_handled=handle_Outliers('Age',df_olympics)
    df_olympics_weight_age_outliers_handled=handle_Outliers('Weight',df_olympics_age_outliers_handled)
    df_olympics=handle_Outliers('Height',df_olympics_weight_age_outliers_handled)
    #print(df_olympics.shape)

    #2)Handling Missing Data:
    #As shown in the visualizations we did before
    #the Age, Weight, Height and Medal attributes were the only ones among other attributes that had missing values.

    #Medals:
    #Encode all NaN values with the value 0
    df_olympics["Medal"].fillna(value=0, inplace=True)
    #Encode all Bronze Medals with the value 1
    df_olympics.replace('Bronze', value=1, inplace=True)
    #Encode all Silver Medals with the value 2
    df_olympics.replace('Silver', value=2, inplace=True)
    #Encode all Gold Medals with the value 3
    df_olympics.replace('Gold', value=3, inplace=True)
    #print(df_olympics.isna().sum())
    
    #Age:
    df_olympics=AgeNan(df_olympics)
    #print(df_olympics.isna().sum())

    #Weight:
    df_olympics=WeightNan(df_olympics)
    #print(df_olympics.isna().sum())

    #Height:
    df_olympics=HeightNan(df_olympics)
    #print(df_olympics.isna().sum())

    df_cleaned_olympics=df_olympics.to_json(orient='index')
    kwargs['ti'].xcom_push(key='cleaned_olympics_dataset',value=df_cleaned_olympics)
    
################################################ Data Integration ##################################################
def Data_Integration(**kwargs):
    df1_json = kwargs['ti'].xcom_pull(key='medals_dataset', task_ids='extract_data')
    df2_json = kwargs['ti'].xcom_pull(key='noc_regions_dataset', task_ids='extract_data')
    df3_json = kwargs['ti'].xcom_pull(key='cleaned_olympics_dataset', task_ids='clean_data')

    df_medals=pd.read_json(df1_json, orient ='index',dtype=False)
    df_noc_regions=pd.read_json(df2_json, orient ='index',dtype=False)
    df_olympics=pd.read_json(df3_json, orient ='index',dtype=False)

    #Dataset 1: NOC_REGIONS
    df_merged_regions = pd.merge(df_olympics, df_noc_regions)

    df_noc_olympics_merged=df_merged_regions.to_json(orient='index')
    kwargs['ti'].xcom_push(key='noc_olympics_merged_dataset',value=df_noc_olympics_merged)
    #print("merged_NOC:")
    #print(df_merged_regions.head())
    #df_merged_regions.head()

    ##Dataset 2: MEDALS
    #first of all we created a new two dataset and equate it with a copy from the original one 
    df_copy_2=df_olympics.copy()
    #since the medal counts are per each player rather than the each team so we drop duplicates
    df_copy_2= df_copy_2.drop_duplicates(subset=['Team', 'NOC', 'Games', 'Year', 'City', 'Sport', 'Event', 'Medal'])
    #then we dropped all the uncessary columns we dont need
    df_copy_2=df_copy_2.drop(["ID","Name","Sex","Age","Height","Weight","Games","Season","City","Sport","Event"],axis=1)

    #Since for now we will not feed the data for an ML Model we replaced the Label encoding we did prevoiusly to make the
    #data more understandable.  
    df_copy_2.replace(0, value='No Medal', inplace=True)
    df_copy_2.replace(1, value='Bronze', inplace=True)
    df_copy_2.replace(2, value='Silver', inplace=True)
    df_copy_2.replace(3, value='Gold', inplace=True)

    #we created a three new columns Gold, Silver, and Bronze with initially all the values equal to NaT
    df_copy_2["Gold"]=pd.NaT
    df_copy_2["Silver"]=pd.NaT
    df_copy_2["Bronze"]=pd.NaT

    #We Looped over the Entries Row wise and replaced all the NaT values with a Real value which is 
    # 1 if the Medal is Gold, Silver, or Bronze and 0 otherwise.
    df_copy_2['Gold'] = np.where(df_copy_2['Medal'] =='Gold' , 1, 0)
    df_copy_2['Silver'] = np.where(df_copy_2['Medal'] =='Silver' , 1, 0)
    df_copy_2['Bronze'] = np.where(df_copy_2['Medal'] =='Bronze' , 1, 0)


    #our target now is to try to make df_copy_2 to look like The Medals dataset to be able merge them on Team/NOC
    #To do that we have to drop the NOC & Medal column and rename the Team column to be Team/NOC
    df_copy_2 = df_copy_2.drop(['NOC','Medal'],axis=1)
    df_copy_2 = df_copy_2.rename(columns={'Team': 'Team/NOC'})

    #After that we need to have the total number of Gold, Silver, and Bronze medals achieved by each country
    df_copy_2=df_copy_2.groupby(['Team/NOC','Year']).sum()[['Gold', 'Silver', 'Bronze']].sort_values(['Gold'], ascending=False).reset_index()

    #Creating a new column called Total which contains the sum of all the medals per Row
    columns_list = list(df_copy_2)
    columns_list.remove("Team/NOC")
    columns_list.remove("Year")
    df_copy_2["Total"] = df_copy_2[columns_list].sum(axis=1)

    #It is time now to load the Medals dataset to integrate it with the df_copy_2 dataset
    #df_medals=pd.read_csv("/content/Medals.csv", encoding='latin-1')

    #since this dataset is for the 2020 Tokyo olympics we have created a new column named it Year and filled it with 2020
    #all over. We also dropped unnecssary columns Rank and Rank by Total.
    df_medals= df_medals.drop(['Rank','Rank by Total'],axis=1)
    df_medals['Year']=2020

    #The next step is clearly is that we have to merge the two datasets.
    #However, there is an issue here USA is called here United States of America and in the df_copy_2 it is called United States only
    #so we have to rename it.
    #the same issue for china and soviet union.

    df_medals.at[0,'Team/NOC']='United States'
    df_medals.at[1,'Team/NOC']='China'
    df_medals.at[4,'Team/NOC']='Soviet Union'

    #we concatted the newly created data set(df_copy_2) with the medals dataset upon the Team/NOC feature
    Olympics_Medals_Data_Merged = pd.concat([df_copy_2,df_medals])
    Olympics_Medals_Data_Merged.reset_index(inplace=True)
    #Olympics_Medals_Data_Merged = pd.merge(df_copy_2, df_medals)


    df_medals_olympics_merged=Olympics_Medals_Data_Merged.to_json(orient='index')
    kwargs['ti'].xcom_push(key='medals_olympics_merged_dataset',value=df_medals_olympics_merged)
    #print("olympics_medals_merged:")
    #print(Olympics_Medals_Data_Merged.head())
    # We will use this Olympics_Medals_Data_Merged dataset to answer our First Research Question below.

############################################# Feature Engineering ##################################################
def Feature_Engineering(**kwargs):
    df3_json = kwargs['ti'].xcom_pull(key='cleaned_olympics_dataset', task_ids='clean_data')
    df_cleaned_olympics=pd.read_json(df3_json, orient ='index',dtype=False)
    df=df_cleaned_olympics.copy()
    #Feature 1: BMI
    df['BMI'] = df['Weight'] / ((df['Height'] / 100) ** 2)

    df['BMI Group'] = np.where(df['BMI'] <= 18.5, 'Underweight',
                            np.where(df['BMI'] < 25, 'Normal',
                                    np.where(df['BMI'] < 30, 'Overweight',
                                            np.where(df['BMI'] >= 30, 'Obese', 'NA'))))
    df = df[['ID', 'Name', 'Sex', 'Age', 'Height', 'Weight', 'BMI', 'BMI Group', 'Team', 'NOC', 'Games', 'Year', 'Season', 'City', 'Sport', 'Event', 'Medal']]
    
    df_BMI_feature_eng=df.to_json(orient='index')
    kwargs['ti'].xcom_push(key='BMI_feature_eng_dataset',value=df_BMI_feature_eng)

    # Feature 2: Number of Medals per Game
    df['Number of Medals per Game'] = 0

    df_copy = df.copy()
    df_copy.drop(df_copy[(df_copy['Medal'] == 0)].index, inplace=True)
    namesdata = df_copy.Name.unique()
    
    for name in namesdata:
        group = (df_copy[df_copy['Name'] == name].groupby(by=['Games']))
        dff = (group['Medal'].count())
        for key, value in group.groups.items():
            for x in range(len(value)):
                df.loc[value[x],'Number of Medals per Game'] = len(value)
    
    df_NumberOfMedalsPerGame_feature_eng=df.to_json(orient='index')
    kwargs['ti'].xcom_push(key='NumberOfMedalsPerGame_feature_eng_dataset',value=df_NumberOfMedalsPerGame_feature_eng)
############################################# Load ##################################################
def Load(**kwargs):
    df1_json = kwargs['ti'].xcom_pull(key='medals_olympics_merged_dataset', task_ids='integrate_data')
    df2_json = kwargs['ti'].xcom_pull(key='noc_olympics_merged_dataset', task_ids='integrate_data')
    df3_json = kwargs['ti'].xcom_pull(key='cleaned_olympics_dataset', task_ids='clean_data')
    df4_json = kwargs['ti'].xcom_pull(key='BMI_feature_eng_dataset', task_ids='feature_eng_data')
    df5_json = kwargs['ti'].xcom_pull(key='NumberOfMedalsPerGame_feature_eng_dataset', task_ids='feature_eng_data')


    df_medals_olympics_merged=pd.read_json(df1_json, orient ='index',dtype=False)
    df_noc_olympics_merged=pd.read_json(df2_json, orient ='index',dtype=False)
    df_cleaned_olympics=pd.read_json(df3_json, orient ='index',dtype=False)
    df_BMI_feature_eng=pd.read_json(df4_json, orient ='index',dtype=False)
    df_NumberOfMedalsPerGame_feature_eng=pd.read_json(df5_json, orient ='index',dtype=False)

    df_cleaned_olympics.to_csv("cleaned_olympics.csv", index=None)
    df_noc_olympics_merged.to_csv("noc_olympics_merged.csv", index=None)
    df_medals_olympics_merged.to_csv("medals_olympics_merged.csv", index=None)
    df_BMI_feature_eng.to_csv("BMI_featureEng.csv", index=None)
    df_NumberOfMedalsPerGame_feature_eng.to_csv("NumberOfMedalsPerGame_feature_eng.csv", index=None)


##Some Usefull Helper Methods
def handle_Outliers(outlier_column,df):
    all_sports=['Alpine Skiing','Alpinism','Archery','Art Competitions','Athletics' ,'Badminton','Baseball','Basketball','Beach Volleyball','Biathlon','Bobsleigh','Boxing','Canoeing','Cricket','Croquet','Cross Country Skiing','Curling','Cycling','Diving','Equestrianism','Fencing','Figure Skating','Football','Freestyle Skiing','Golf','Gymnastics','Handball','Hockey','Ice Hockey','Jeu De Paume','Judo','Luge','Modern Pentathlon','Motorboating','Nordic Combined','Polo','Racquets','Roque','Rowing','Rugby','Sailing','Shooting','Skeleton','Ski Jumping','Snowboarding','Softball','Speed Skating','Swimming','Synchronized Swimming','Table Tennis','Tennis','Trampolining','Triathlon','Tug-Of-War','Volleyball','Water Polo','Weightlifting','Wrestling']
    # loop over each and every sport found in the dataset 
    for sport in all_sports:
        # create a subset "df_specific_sport", containing all record that have Sport s, of the dataset 
        df_specific_sport = df[(df['Sport'] == sport)]

        # In "df_specific_sport" subset, calculate Q1 of each column: Age, Weight, Height 
        Q1 = df_specific_sport[outlier_column].quantile(0.25)

        # calculate Q3 of each column: Age, Weight, Height 
        Q3 = df_specific_sport[outlier_column].quantile(0.75)

        # calculate IQR "length of the box"
        IQR = Q3 - Q1

        # calculate the left whisker position 
        min = Q1 - 1.5 * IQR

        # calculate the right whisker position 
        max = Q3 + 1.5 * IQR  

        # getting another subset "outliers_per_sport" of "df_specific_sport" that 
        # contains all records that have their ages value less than left whisker or
        # greater than right whisker, thus detecting them as outliers
        outliers_per_sport = df_specific_sport[(df_specific_sport[outlier_column] < min) | (df_specific_sport[outlier_column] > max)]
        
        # drop subset "outliers_per_sport" from the original dataset "df"
        df.drop(outliers_per_sport.index, inplace = True)
    return df

#a function that given a specific row value for sex noc and sport
#it returns the mean of all the players with those age weight and height
def Avg_using_Sex_NOC_Sport(sex,noc,sport,df):
  x=df[(df['Sex']==sex) &(df['NOC']==noc) &(df['Sport']==sport)]
  return x["Age"].mean(),x["Weight"].mean(),x["Height"].mean()

#same function as the above but instead using Sex and Sport as our input attributes
def Avg_using_Sex_Sport(sex,sport,df):
  x=df[(df['Sex']==sex) &(df['Sport']==sport)]
  return x["Age"].mean(),x["Weight"].mean(),x["Height"].mean()

#same function as the above two but instead using Sex only as our input attribute
def Avg_using_Sex(sex,df):
  x=df[(df['Sex']==sex)]
  return x["Age"].mean(),x["Weight"].mean(),x["Height"].mean()

#This function is responsible to impute the age missing values
def AgeNan(df):
    MissingAgeValues = df[(df['Age'].isnull())] # contains all the rows with missing age values which are around 9474 values
    index_count = pd.Index(df['ID']).value_counts() # counts the number of occurance of the same ID
    i=0 # start the counter with zero
    while i < (MissingAgeValues['ID'].shape[0]): # loop from 0 to 9474
        row = df[(df['ID'] == MissingAgeValues['ID'].iloc[i])] # all the rows of the same ID with missing Age Value
        age,weight,height=Avg_using_Sex_NOC_Sport(row['Sex'].iloc[0],row['NOC'].iloc[0],row['Sport'].iloc[0],df) # return their mean age weight and height

        #in case their doesnot exist a record for that sex noc and sport together the mean will be nan and in that case we have to use the mean
        #of sex & sport and discard the Noc from our calculations
        if (math.isnan(age)):
            age,weight,height=Avg_using_Sex_Sport(row['Sex'].iloc[0],row['Sport'].iloc[0],df)
            df.loc[df['ID'] == row['ID'].iloc[0], 'Age'] = round(age)
        else:
            df.loc[df['ID'] == row['ID'].iloc[0], 'Age'] = round(age)
        #increase the counter with the number of repeatence of the current ID were are located on
        i=i+index_count.loc[row['ID'].iloc[0]]
    return df     
#This function is responsible to impute the weight missing values
def WeightNan(df):
    MissingAgeValues = df[(df['Weight'].isnull())]
    index_count = pd.Index(df['ID']).value_counts()
    i=0
    while i < (MissingAgeValues['ID'].shape[0]):
        row = df[(df['ID'] == MissingAgeValues['ID'].iloc[i])]
        age,weight,height=Avg_using_Sex_NOC_Sport(row['Sex'].iloc[0],row['NOC'].iloc[0],row['Sport'].iloc[0],df)
        if (math.isnan(weight)):
            age,weight,height=Avg_using_Sex_Sport(row['Sex'].iloc[0],row['Sport'].iloc[0],df)
            if(math.isnan(weight)):
                age,weight,height=Avg_using_Sex(row['Sex'].iloc[0],df)
                df.loc[df['ID'] == row['ID'].iloc[0], 'Weight'] = round(weight)
            else:  
                df.loc[df['ID'] == row['ID'].iloc[0], 'Weight'] = round(weight)
        else:
            df.loc[df['ID'] == row['ID'].iloc[0], 'Weight'] = round(weight) 
            i=i+index_count.loc[row['ID'].iloc[0]]
    return df

#This function is responsible to impute the height missing values
def HeightNan(df):
    MissingAgeValues = df[(df['Height'].isnull())]
    index_count = pd.Index(df['ID']).value_counts()
    i=0
    while i < (MissingAgeValues['ID'].shape[0]):
        row = df[(df['ID'] == MissingAgeValues['ID'].iloc[i])]
        age,weight,height=Avg_using_Sex_NOC_Sport(row['Sex'].iloc[0],row['NOC'].iloc[0],row['Sport'].iloc[0],df)
        if (math.isnan(height)):
            age,weight,height=Avg_using_Sex_Sport(row['Sex'].iloc[0],row['Sport'].iloc[0],df)
            if(math.isnan(height)):
                age,weight,height=Avg_using_Sex(row['Sex'].iloc[0],df)
                df.loc[df['ID'] == row['ID'].iloc[0], 'Height'] = round(height)
            else:  
                df.loc[df['ID'] == row['ID'].iloc[0], 'Height'] = round(height)
        else:
            df.loc[df['ID'] == row['ID'].iloc[0], 'Height'] = round(height) 
        i=i+index_count.loc[row['ID'].iloc[0]]
    return df        

t1 = PythonOperator(
    task_id="extract_data",
    provide_context=True,
    python_callable=Extract,
    dag=dag
)
t2 = PythonOperator(
    task_id="clean_data",
    provide_context=True,
    python_callable=Data_Cleaning,
    dag=dag
)
t3 = PythonOperator(
    task_id="integrate_data",
    provide_context=True,
    python_callable=Data_Integration,
    dag=dag
)
t4 = PythonOperator(
    task_id="feature_eng_data",
    provide_context=True,
    python_callable=Feature_Engineering,
    dag=dag
)
t5 = PythonOperator(
    task_id="Load_data",
    provide_context=True,
    python_callable=Load,
    dag=dag
)

t1>>t2>>t3>>t4>>t5