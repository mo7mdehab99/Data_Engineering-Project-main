# DE-Project

This is a template repository for the CSEN1095-W21 Course

************************Overview of the used dataset*****************************
The dataset is related to Olympic games from the year "1896" to "2016". It has 66 different sports. It also contains which player won which medal in which event. In addition, each olympic game is hosted in  different city. 

******Provide an overview of the project goals and the motivation for it********
The project goal is to visualize, clean and preprocess (transformation) of the dataset in order to input this dataset in an ideal tabular form which is needed for a machine learning algorithm.

**********Descriptive steps used for the work done in this milestone************

Part 1) 
    In the exploration part, we studied and analyzed four attributes (Age, Weight, Height and Medal) since these attributes contain problems, such as missing data, outliers or non-representable column, such as Medal. In this part, we computed the percantage of each attribute which has missing data by using histograms and bar charts. Also, we have analyzed outliers with some visualization techniques using histograms. In addition, we studied the correlation between attributes using heatmap and scatterplot.

Part 2) 
  Outliers handling can only be applied to numerical columns, such as Age, Weight and Height. Here are two steps of outliers handling, which are detection and then removal:

  Outliers Detection: 

  First we have plotted the Age BoxPlot to visualize the outliers. Then, we found that any age greater than 38.5 are considered outliers. We calculated age average mean per each sport and noticed that some of the means are close to 38.5. In order to ensure unbiasedness between sports, we chose to detect outliers per each sport independently.

  Outliers Removal:

   1.   Q1: middle value between smallest number and median
   2.   Q3: middle value between largest number and median
   3.   IQR: difference between Q3 and Q1
   4.   Left Whsiker position: calculated by Q1 - 1.5 * IQR 
   5.   Right Whsiker position: calculated by Q3 + 1.5 * IQR 

   Then, we dropped all records, containing age values greater than right whisker position or less than left whisker position, from original dataset

  Finally, we will repeat again these two methods but this time on Weight and Height columns to detect outliers per each sport and then remove them.

Part 3)

There are only four attributes that have missing values: Age, Weight, Height and Medal. 

  Medal:
      Label encoding the medal attribute so that instead of using NAN for missing values, Bronze, Silver and Gold
      We are preserving the ordinal value ranking by using
      0 -> No Medal | 1 -> Bronze | 2 -> Silver | 3 -> Gold
      
  Age, Weight, Height: 
      Impute missing values in the Age, Height, and Weight by replacing them with mean of non-missing observations Sex, NOC, and Sport.


*************************Research questions******************************

1.   Which country brings most of the medals?
2.   Which country has the oldest player that participates in Olympics?
3.   Which event have the most attendees participated in?
4.   How many Medals were gained by Men and Women of Age>40 who participated throughout the history of the Olympic Games?
5.   Does Height and Weight of an Olympic Gold Medalist holder have a relationship?
6.   Is the athlete's BMI a factor in his or her potential to earn a medal?
7.   Does the Age of a participant have an effect upon gaining a Gold Medal?
8.   For each region with more than one NOC, show which NOC has the most medals.
9.   List all the players, given a sport with different events, who have won 3 medals or more in the same Olympic game.

*************************Data Integration******************************
In this Section We integrated the noc_regions datset and the Medals dataset with our original datset and answerd two questions using each integration(Q1 and Q8)

Description of the integrated datasets:

1) noc_regions dataset: This is a historical dataset on the modern Olympic Games, including all the Games from Athens 1896 to Rio 2016 and it includes all the NOC (National Olympic Committee 3-letter code) for each Region.

2) Medals dataset: This dataset contains the number of Gold, Silver, and Bronze Medals Won by each Country Team Ranked by their Total Number of medals in the latest Olympic Game that occurred in Tokyo 2020.

*************************Feature Engineering******************************
In this Section We created two new features BMI and Medals per game and added them to our dataset and answerd two questions on them(Q6 and Q9)

Description of the features you added:

1) The first engineering feature is the BMI group. This feature has the potential to be quite useful for a variety of reasons. Given a sport, we may assess the chance of winning a medal based on the BMI group (whether underweight, overweight, obese, or normal) and determine which BMI group has the highest probability of winning a medal. This would be ideal for ML algorithms if we want to know if a person entering a specific sports competition with his/her BMI group has a chance of winning a medal, and since we computed the ideal BMI group for three randomly chosen sports, Diving, Golf, and Modern Pentathlon, and this can be generalized for other sports, we can compute the chance that this person wins a medal. As a consequence, given an individual, sport, and this BMI group, the ML can predict his/her chances of winning a medal.

2) The second engineering feature is the number of medals awarded per game. In this section, we are interested in sports that include many events, such that one athlete can win multiple medals in the same Olympic game. This functionality can assist us in displaying sports legends, for example (a sports player winning more than 3 medals for the same sport in different events). Furthermore, it will be great for ML algorithms if we want to estimate how many medals a single athlete will win given a specific Olympic game. As a result, given the number of medals he/she won in the previous Olympic games, the ML algorithm from the new feature (Number of medals per game) can predict how many medals this person can win in the latest Olympic game.

*************************Analysis******************************
In this section we answerd our Reasearch Questions using Visulizations and deduced from them Some Insights which We will list Below respectively:

1) Since The United States is a wealthy country that can devote a lot of money to sports (coaching, facilities, career opportunities for athletes) more than the several European countries, as well as Australia and China. And also, because the USA has a higher population than any other country, it will win more medals, as clearly shown above by our Visualization.

2) As stated above in the insights of the First Question since the United States commits and invests alot in their participants they must clearly have a high Age distribution therefore the oldest participant in the olympics is an American!

3) As we all know, football is played and watched in practically every country on the planet, which is no surprise. The fact that this sport is super fun does not have many complicated rules is one of the main reasons soccer is famous worldwide. Which makes an enormous number of players Participate in it than any other Sport. This is clearly shown above by our Visualization.

4) From the first graph, we can deduce that in the most recent years starting from the era of the 2000s women of age >40 gained more medals than in the previous years. While in the Seconde Graph, the complete opposite occurred with Men participants. This might be because, in the 1900s, women were not interested in or well-known for competing in the Olympic games, hence there were no competitors to compete with males. However, Men had more medals in the past, but now that women are participating more in the Olympic games, there is more competition, so the medal count of males has declined.

5)  Almost all the data samples shown above show a linear relationship between height & weight, i.e. ( the more the weight, the more the height). Hence, we can deduce that participants who won a gold model positively correlate with their Weight and Height.

6)  For the First Sport, Diving, we can deduce that someone who is underweight has the best chance of winning a medal. The number of normal sports players that entered diving sport is 2619, while underweight and overweight players are 101 and 87, respectively, while obese was just 1; nevertheless, to make the comparison valid, we must calculate the ratio, which is % per BMI group. As a consequence, the results demonstrate that in the diving sport, a person who is underweight is more likely to win, followed by a normal person, and finally by an overweight person, while the obese have no chance of winning.

    For the second sport, Golf, we can conclude that someone who is overweight has the best chance of winning a medal. The number of normal sports players that entered golf sport is 116, while overweight players are 127, while underweight and obese were only 1 and 2 respectively, but in order for the comparison to be valid, we must compute the ratio, which is % per BMI group. As a consequence, the results demonstrate that in the golf sport, an overweight person is more likely to win, followed by a normal person, and ultimately, the underweight and obese have no chance of winning.

    For the third sport, Modern Pentathlon, we may infer that any normal person has the best chance of winning a medal. The number of normal sport participants that attended Modern Pentathlon sport is 1580, while overweight participants are 45, while underweight and obese participants are only 4 and 0 correspondingly, but to make the comparison valid, we must compute the ratio, which is % per BMI group. As a result, the results demonstrate that in the Modern Pentathlon sport, a person with a normal BMI is more likely to win than other BMI groups.

7)  As shown in the Visualization above, we have a negative relationship between Age & Medal counts, i.e. ( the higher the age, the less the number of Medals). Hence, we can deduce that younger participants at the peak in the graph above in the Age range 20-30 gain more medals as compared to other Age Ranges.

8)  In China the NOC "CHN" brings more medals than "HKG" with remarkable difference as we seen in the diagram most of NOC recorded for China os "CHN" only few olymics that records China by "HKG". For Russia the most name gained medal with NOC is "URS" however "RUS" and "EUN" is NOC for Russia also but "RUS" is double "EUN". In Syria "SYR"is more used as NOC than "UAR" but they are likely near in gaining medals there are no huge difference. For Germany "GER" is most used NOC than "FRG" and "GDR" with huge differences. For Australia is shown in the diagram that mostly is used NOC is "AUS" than "ANZ" it shown that is not used in many olympics "ANZ" NOC. For Czech Republic "TCH" is most used NOV for most of olympics while "BOC"is rarely used NOC in the Olympics. For Trinidad Region "TTO" is most NOC that represents Trinindad and only few Olympics which they recorded as "WIF". For Serbia Region "YUG" is the most NOC used in most of the Olympics however "SCG" and "SRB" is more less than "YUG" and they are almost near in number of medals.

9) The visualization displays individuals who have won three or more Olympic medals. As we can see, those athletes participate in sports with a number of different events. Based on the outcomes in the table, we can see that those who have previously won medals have a good probability of winning a lot of medals at the upcoming Olympic Games. This demonstrates that these players are talented at the game. For example, in 1996 Summer, Simona Ammar won four medals; four years later, in 2000 Summer, she earned three medals, demonstrating how professional she is in the game.

-------------------------------------------------------------------------------MS3----------------------------------------------------------------------------------------------

In this Milestone, we tried to automate and memic the ETL, i.e.(Extract, Transform, and Load) pipeline using Airflow.

We made a DAG script that consists of 5 python operators, which will be described below:

1) Extract:
    which reads all the 3 CSV datasets (Medals, athlete_events, and noc_regions) and converts them to JSON to be able to push them using Xcom using the appropriate key and value on the pipeline since each python operator or task does not see the other tasks in the pipeline.
    
2) Data_Cleaning:
   In this operator, we Xcom pull the Olympics dataset(athlete_events) (and convert them back to dataframe from JSON) we previously Xcom pushed as JSON in the Extract pipeline    and apply the cleaning process, which consists of handling Outliers and the missing values. Finally, after cleaning, we pushed the cleaned dataset to the pipeline to be        utilized by the upcoming pipelines.
    
3) Data_Integration:
    In this operator, we Xcom pulled the noc_regions, the medals dataset, and Xcom pulled the cleaned dataset from the Data_Cleaning operator. Then, we performed two               integrations the first one is between noc_region and the cleaned data Olympics(athlete_events), and the second one is between the medals and the cleaned data.
    Olympics(athlete_events) have been used to answer some research questions in MS2.
    
4) Feature_Engineering:
    We Xcom pulled the cleaned dataset Olympics from the Data_Cleaning operator in this operator. Then, we used it to instantiate the first and the second engineering feature,     which are the (BMI and BMI group) and Medals per game.
    
5) Load:
    In this operator, we Xcom pulled all the 5 datasets which are:
    1) df_medals_olympics_merged
    2) df_noc_olympics_merged
    3) df_cleaned_olympics
    4) df_BMI_feature_eng
    5) df_NumberOfMedalsPerGame_feature_eng
  
    from the previous 3 pipelines (Data_Cleaning, Data_Integration, Feature_Engineering)
    then we saved them as CSV files.
