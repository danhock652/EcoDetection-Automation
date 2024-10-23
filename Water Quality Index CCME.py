import time 
start = time.time()

import os
import pandas as pd
import numpy as np
import datetime as dt
import math


#This script executes calculations outlined in the CCME (Canadian Council of Ministers of the Environment) WQI methodology.
#The program will export a cleansed data file with WQI and ratings for the months over the period of measurement. This script can be used to append data from multiple files into the same file.
#The data can be distinguished through the reference code defined in the file_params variable
#The file will be exported into the location defined in the 'destination' variable
#Please note that the ratings for WQI (0-100) are as follows:
#A - >=95, B - 80-95, C - 65-79, D 45-64, E - 0-44, F = 0 

#This was done through building a 'parent' function which is called cleanse_data. This contains several 'child' functions to execute processes. Below is a list of the function and their purposes 

#List of functions 

#  cleanse_data - runs through the process of assigning a monthly WQI to a EcoDetection site using the CCME WQI 
#  no_total_test - define the number of total tests carried out over the timeline 
#  no_tests_parameter - defines the number of tests per parameter over the timeline 
#  no_failed_tests - defines the number of failed tests (ERS exceedances) per parameter over the timeline 
#  total_failed_tests - defines the total number of failed tests over the timeline 
#  total_failed_parameters - defines the total number of failed parameters over the timeline
#  no_total_parameters - defines the total number of parameters that were measured in the timeline 
#  assign_F1 - Assign the F1 value according to the CCME WQI methodlogy - F1 = (no. failed parameters/no. total parameters)
#  assign_F2 - Assign the F1 value according to the CCME WQI methodlogy - F2 = (no. failed tests/no. total tests)
#  assign_F3 - This function is built to assign the F3 value according to the CCME WQI methodology. There are several steps in this function:
#	  1. Find the excursion (ratio of difference in an out of range (OOR) value
#	  2. Find the sum of excursion values over each timeline 
#	  3. Find the average excursion value = sum of excursion/no. total tests (NSE value)
#	  4. Create F3 value - F3 = NSE/(NSE*0.01+0.01)
#  This function also incorporates other steps in the process including:
#	  1. Assigning a grade for each parameter over the timeline - grade score = avg. excursion + ratio of failed tests for the parameter (e.g. 0.03 = 3%)
#	  2. Find the biggest contributing parameter that affects the WQI by taking the maximum grade score to have the worst contribution to the WQI
#  assign_WQI - Use F1, F2 and F3 to assign the WQI over each timeline according to the CCME WQI

#Different periods can be analysed if you wish to uncomment the calling of functions within the code calling 'Day', 'Week, 'Season, or 'Year'


#Please define the files, reference code and date format in accordance with the below example. (List of tuples - (file path, reference code, date format))
#See the examples below for some inspiration on date formatting!

#date_format = '%Y-%m-%d %H:%M:%S'
#date_format = '%d/%m/%Y %H:%M'

##file_params=[(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Coliban Water.Kangaroo Creek.csv",'cw_a','%Y-%m-%d %H:%M:%S')
##             ,(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Coliban Water.Little Coliban River.csv",'cw_b','%Y-%m-%d %H:%M:%S')
##             ,(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Greater Western Water.Five Mile Creek - Woodend RWP Site 1.csv",'gww_a','%d/%m/%Y %H:%M')
##             ,(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Greater Western Water.Five Mile Creek - Woodend RWP Site 2.csv",'gww_b','%d/%m/%Y %H:%M')]

file_params=[(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Coliban Water.Kangaroo Creek.csv",'cw_a','%Y-%m-%d %H:%M:%S')
             ,(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Coliban Water.Little Coliban River.csv",'cw_b','%Y-%m-%d %H:%M:%S')
             ,(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Greater Western Water.Five Mile Creek - Woodend RWP Site 1.csv",'gww_a','%d/%m/%Y %H:%M')
             ,(r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\export-ecod72.Greater Western Water.Five Mile Creek - Woodend RWP Site 2.csv",'gww_b','%d/%m/%Y %H:%M')]

#Please define the destination where the file should be created and the name of the file to be created. Please see the below example for inspiration.

##destination = r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\Manipulated Data - tests over 120 - parameters over 2.csv"

destination = r"C:\Users\hockind\Desktop\Hackathon\EcoDetection Data\Testing for param grades.csv"

#Define ERS (Environmental Reference Standard) bounds to count no. of failures (take into consideration units of measurement ppb/1000 = mg/l)
#These are assigned based on the columns in the manipulated data - in this case:
#[Timestamp (keep as NA), Total Nitrogen Indicator, Total Phosphorous Indicator, Conductivity, Nephelo Turbidity, Oxygen (range - calculated at 20 degrees celsius), pH (range)]

tolerance = ['Na',1050,165,2000,15,[5.4,10.1],[6.8,8.0]]


def cleanse_data(data_source,reference,date_format,destination=destination):

    

    #Change working directory

    data_source1 = data_source.split('\\')

    os.chdir('..')

    os.chdir(f"./{data_source1[-2]}")

    #Read in data from csv
        
    data = pd.read_csv(data_source1[-1])
        
    #Create list of headers to be used instead of existing arbitrary codes

    headers = [data['Id'].loc[data.index[0]]]

    for col in range(1,data.shape[1]):

        header_label = data.iat[0,col].rsplit('/',1)

        headers.append(header_label[1])

    #Rename Header at each column

    list_of_names = list(data.columns)

    for col in range(data.shape[1]):
        
        data = data.rename(columns={list_of_names[col]: headers[col]})

    #Remove first row (used to create headers)

    data.drop([0,1], inplace= True)


    #Create index values and columns for daily, weekly, monthly, seasonly and yearly

    
    
    data.dropna(subset=['Timestamp'],inplace=True)

    
    day_index = []
    week_index = []
    month_index= []
    season_index= []
    year_index=[]

    #Pick desired value (day,week,month etc.) from date string and append to applicable index list
    
    data['Timestamp'] = pd.to_datetime(data['Timestamp'], format = date_format)
    
    for i in range(len(data)):

        #data.iat[i,0]=dt.datetime.strptime(str(data.iat[i,0]), date_format)
        
        day_index.append(data.iat[i,0].strftime('%j'))
        week_index.append(data.iat[i,0].strftime('%W'))
        month_index.append(data.iat[i,0].strftime('%m'))
        year_index.append(data.iat[i,0].strftime('%y'))

    for i in range(len(data)):
        season_index.append(int(month_index[i])%12//3 + 1)

    #Add list as columns into dataframe

    data['Day Index']=day_index
    data['Week Index']=week_index
    data['Month Index']=month_index
    data['Season Index']=season_index
    data['Year Index']=year_index

    #Drop Enclosure Temperature column as it is not needed for analysis

    data = data.drop(['Enclosure Temperature'],axis = 1)

    #Remove measurements of Oxygen and pH that = 0, this is not realistic. Reassign these values as NA

    data['Oxygen']=data['Oxygen'].replace('0', np.nan)
    data['pH']=data['pH'].replace('0', np.nan)

    #Assign no. of parameters

    no_parameters = len(tolerance)-1



    

    #Calculate 'Total Nitrogen' as Nitrate + Nitrite

    total_nitrogen = []
    
    for i in range(len(data)):
        if float(data['Nitrate Concentration'].values[i])>= 0 and float(data['Nitrite Concentration'].values[i])>=0:
            total_nitrogen.append(float(data['Nitrate Concentration'].values[i])+float(data['Nitrite Concentration'].values[i]))
        elif float(data['Nitrate Concentration'].values[i])>=0:
            total_nitrogen.append(float(data['Nitrate Concentration'].values[i]))
        elif float(data['Nitrite Concentration'].values[i])>=0:
            total_nitrogen.append(float(data['Nitrite Concentration'].values[i]))
        else:
            total_nitrogen.append(None)


    data.insert(1,'Total Nitrogen Approximation',total_nitrogen)

    #Drop unneccesary columns

    data.drop(['Chloride Concentration','Fluoride Concentration','Sulphate Concentration', 'Nitrate Concentration', 'Nitrite Concentration'],axis=1,inplace=True)


                
    #Count no. of failures for each row - 7 is used to replace the NAs as it is within range for all values, this does not affect total test count (i.e. improve percentage of successful scores)
    #Turn NaN into 0 in THIS dataframe

    failure_data = data.fillna(7)


    failures = [ [] for i in range(len(data))]
    for i in range(0,len(data)):
        for j in range(1,len(tolerance)):
            if j in range(1,len(tolerance)-2):
                if float(failure_data.iat[i,j]) > float(tolerance[j]):
                    failures[i].append(1)
                else:
                    failures[i].append(0)
    #This is a special case for assessing pH and oxygen since it should be within a range if there were another parameter added to be a minimum guideline this would also require a special case
            else:
                
                if float(failure_data.iat[i,j]) < float(tolerance[j][0]):
                    failures[i].append(1)
                elif float(failure_data.iat[i,j]) > float(tolerance[j][1]):
                    failures[i].append(1)
                else:
                    failures[i].append(0)
                    
    #Add failure index to the data frame

    data['Failure Index']=failures

    #Assign total no. tests per timeline

    def no_total_tests(timeline):
        total_tests = []
        for i in range(len(data)):
                    no_tests=0
                    for j in range(1,len(tolerance)):
                        
                        if not math.isnan(float(data.values[i][j])):
                            no_tests += 1
                    total_tests.append(no_tests)
                    
    
        
        for i in range(len(data)):
            if i <= len(data)-2:
                if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                    total_tests[i+1]+=total_tests[i]
                    total_tests[i]=None
                                     
        data[f'Total tests over {timeline}']=total_tests                        
                
    no_total_tests('Day')
    data.drop(data[data['Total tests over Day']==0].index,inplace=True)
    
    #no_total_tests('Week')
    no_total_tests('Month')
    #no_total_tests('Season')
    #no_total_tests('Year')

    #Add total no. of tests per parameter per timeline
    def no_tests_parameter(timeline):
        
        
        tests_row = [ [] for i in range(len(data))]
        for i in range(0,len(data)):
            for j in range(1,len(tolerance)):
                if j in range(1,len(tolerance)):
                    if not math.isnan(float(data.values[i][j])):
                        tests_row[i].append(1)
                    else:
                        tests_row[i].append(0)

        tests_timeline = tests_row
        for i in range(len(data)-1):
            if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                for j in range(len(tolerance)-1):
                    tests_timeline[i+1][j]+=tests_timeline[i][j]
                tests_timeline[i]=None

        data[f'Total tests per parameter over {timeline}']=tests_timeline

    #no_tests_parameter('Day')
    #no_tests_parameter('Week')
    no_tests_parameter('Month')
    #no_tests_parameter('Season')
    #no_tests_parameter('Year')
    

    #Add no. failed test for each timeline

    def no_failed_tests_per_parameter(timeline):

        no_failed_parameters_timeline = [[0 for j in range(len(data['Failure Index'].values[1]))] for i in range(len(data))]

        for i in range(len(data)-1):
            if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                 for j in range(len(data['Failure Index'].values[i])):
                    no_failed_parameters_timeline[i+1][j]=no_failed_parameters_timeline[i][j]+data['Failure Index'].values[i+1][j]
            else:
                no_failed_parameters_timeline[i+1]=data['Failure Index'].values[i+1]

        data[f'Failed tests over {timeline}']=no_failed_parameters_timeline

    #no_failed_tests_per_parameter('Day')
    #no_failed_tests_per_parameter('Week')
    no_failed_tests_per_parameter('Month')
    #no_failed_tests_per_parameter('Season')
    #no_failed_tests_per_parameter('Year')

    #Evaluate number of failed tests over the timeline


    def total_failed_tests(timeline):

        total_failures_list = []
        
        for i in range(len(data)):
            if i <= len(data)-2:
                if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                    total_failures_list.append(None)
                else:
                    val = 0
                    for j in range(len(tolerance)-1):
                        val += data[f'Failed tests over {timeline}'].values[i][j]
                    total_failures_list.append(val)
            else:
                val = 0
                for j in range(len(tolerance)-1):
                    val += data[f'Failed tests over {timeline}'].values[i][j]
                total_failures_list.append(val)
        
        
        data[f'No. failed tests over {timeline}']=total_failures_list

    #total_failed_tests('Day')
    #total_failed_tests('Week')
    total_failed_tests('Month')
    #total_failed_tests('Season')
    #total_failed_tests('Year')

    #Evaluate number of failed parameters over the timeline


    def total_failed_parameters(timeline):

        total_failed_params = []
        
        for i in range(len(data)):
            if i <= len(data)-2:
                if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                    total_failed_params.append(None)
                else:
                    val = np.count_nonzero(data[f'Failed tests over {timeline}'].values[i])
                    total_failed_params.append(val)
            else:
                val = np.count_nonzero(data[f'Failed tests over {timeline}'].values[i])
                
                total_failed_params.append(val)
        
        data[f'No. failed parameters over {timeline}']=total_failed_params

    #total_failed_parameters('Day')
    #total_failed_parameters('Week')
    total_failed_parameters('Month')
    #total_failed_parameters('Season')
    #total_failed_parameters('Year')




    #Assign total no. parameters per timeline

    def no_total_parameters(timeline):
        total_parameters = [[] for i in range(len(data))]
        for i in range(len(data)):
            for j in range(1,len(tolerance)):
                if not math.isnan(float(data.values[i][j])) and not data.values[i][j] == None:
                    total_parameters[i].append(1)
                else:
                    total_parameters[i].append(0)
                    
        col_total_parameters = []

        for i in range(len(data)-1):
            if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                 for j in range(len(data['Failure Index'].values[i])):
                    total_parameters[i+1][j]+=total_parameters[i][j]
            
        
        
        for i in range(len(data)):
            if i <= len(data)-2:
                if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                    total_parameters[i]=None
                else:
                    val = np.count_nonzero(total_parameters[i])
                    total_parameters[i]=val
            else:
                val = np.count_nonzero(total_parameters[i])
                
                total_parameters[i]=val

        data[f'Total parameters over {timeline}']=total_parameters 

        
    #no_total_parameters('Day')
    #no_total_parameters('Week')
    no_total_parameters('Month')
    #no_total_parameters('Season')
    #no_total_parameters('Year')


    #Create F1 (no. failed parameters/no. total parameters) value for each timeline from CCME WQI

    def assign_F1(timeline):
        F1_values = []
        for i in range(len(data)):
            if data[f'No. failed parameters over {timeline}'].values[i] >=0:
                F1_values.append((data[f'No. failed parameters over {timeline}'].values[i]/data[f'Total parameters over {timeline}'].values[i])*100)
            else:
                F1_values.append(None)
        
        data[f'F1 values over {timeline}']=F1_values  

    #assign_F1('Day')
    #assign_F1('Week')
    assign_F1('Month')
    #assign_F1('Season')
    #assign_F1('Year')

    #Create F2 (no. failed tests/no. total tests) value for each timeline from CCME WQI

    def assign_F2(timeline):
        F2_values = []
        for i in range(len(data)):
            if data[f'No. failed tests over {timeline}'].values[i] >=0:
                F2_values.append((data[f'No. failed tests over {timeline}'].values[i]/data[f'Total tests over {timeline}'].values[i])*100)
            else:
                F2_values.append(None)
        
        data[f'F2 values over {timeline}']=F2_values  

    #assign_F2('Day')
    #assign_F2('Week')
    assign_F2('Month')
    #assign_F2('Season')
    #assign_F2('Year')



    ##Define the F3 values for each timeline from CCME WQI
    


    def assign_F3(timeline):

    #First find excursion for every OOR value
        excursion_values=[[] for i in range(len(data))]
        for i in range(len(data)):
                for j in range(1,len(tolerance)):
                    if j in range(len(tolerance)-2):
                            if float(data.iat[i,j]) > float(tolerance[j]):
                               excursion_values[i].append((float(data.iat[i,j])/float(tolerance[j]))-1)
                            else:
                                excursion_values[i].append(0)
    #This is a special case for assessing pH and oxygen since it should be within a range if there were another parameter added to be a minimum guideline this would also require a special case                    
                    else:

                        if float(data.iat[i,j])<float(tolerance[j][0]):
                            excursion_values[i].append((float(tolerance[j][0])/float(data.iat[i,j]))-1)
                        elif float(data.iat[i,j])>float(tolerance[j][1]):
                            excursion_values[i].append((float(data.iat[i,j])/float(tolerance[j][1]))-1)
                        else:
                            excursion_values[i].append(0)


    #Find sum of average excursion value by computing sum of excursions over timeline divided by total no. tests per parameter

        sum_over_timeline = [row[:] for row in excursion_values]
        
        for i in range(len(data)-1):
            if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                for j in range(len(tolerance)-1):
                    
                    sum_over_timeline[i+1][j]+=sum_over_timeline[i][j]
                    
                sum_over_timeline[i]='NaN'


    #Find average excursion over timeline

        avg_excursion = [row[:] for row in sum_over_timeline]
        for i in range(len(data)):
            if not data[f'Total tests per parameter over {timeline}'].values[i]==None:
                    for j in range(len(tolerance)-1):
                        if not data[f'Total tests per parameter over {timeline}'].values[i][j] == 0:
                            avg_excursion[i][j]/=data[f'Total tests per parameter over {timeline}'].values[i][j]
                        

        data[f'Average excursions over {timeline}']=avg_excursion

    #Assign grading for each parameter over timeline to align with grading of WQI (same banding)
    #The grading is based off the average excursion for each parameter + the percentage of failed tests for the parameter over the timeline
        
        tolerance_mapping=['Total Nitrogen Approximation', 'Phosphate','Conductivity', 'Turbidity', 'Oxygen', 'pH']

        def parameter_grading(parameter):

            parameter_metrics=[row[:] for row in avg_excursion]
            for i in range(len(data)):
                if not data[f'Total tests per parameter over {timeline}'].values[i]==None:
                    for j in range(len(avg_excursion[i])):
                        if not data[f'Total tests per parameter over {timeline}'].values[i][j] ==0:
                            parameter_metrics[i][j]+=(float(data[f'Failed tests over {timeline}'].values[i][j])/float(data[f'Total tests per parameter over {timeline}'].values[i][j]))
                
            parameter_grades=[]
            for i in range(len(data)):
                if not data[f'Total tests per parameter over {timeline}'].values[i]==None:
                    if data[f'Total tests per parameter over {timeline}'].values[i][tolerance_mapping.index(parameter)]>0:
                        if parameter_metrics[i][tolerance_mapping.index(parameter)] <= 0.05:
                            parameter_grades.append('A')
                        elif parameter_metrics[i][tolerance_mapping.index(parameter)] <= 0.2:
                            parameter_grades.append('B')
                        elif parameter_metrics[i][tolerance_mapping.index(parameter)] <= 0.35:
                            parameter_grades.append('C')
                        elif parameter_metrics[i][tolerance_mapping.index(parameter)] <= 0.55:
                            parameter_grades.append('D')
                        elif parameter_metrics[i][tolerance_mapping.index(parameter)] <= 1:
                            parameter_grades.append('E')
                        else:
                            parameter_grades.append('F')
                    else:
                        parameter_grades.append("NA")
                else:
                        parameter_grades.append(None)


            data[f'{parameter} Grades over {timeline}']=parameter_grades

        for parameter in tolerance_mapping:
            parameter_grading(parameter)

        
    #Find sum of excursions for every row 
        
        row_total_excursion = [0 for i in range(len(data))]
        for i in range(len(data)):
            for j in range(len(excursion_values[1])):
                row_total_excursion[i]+=excursion_values[i][j]



    #Find biggest contributing variable over the timeline

        
        biggest_contributor=[]
        parameter_metrics=[row[:] for row in avg_excursion]
        for i in range(len(data)):
                if not data[f'Total tests per parameter over {timeline}'].values[i]==None:
                    for j in range(len(avg_excursion[i])):
                        if not data[f'Total tests per parameter over {timeline}'].values[i][j] ==0:
                            parameter_metrics[i][j]+=(float(data[f'Failed tests over {timeline}'].values[i][j])/float(data[f'Total tests per parameter over {timeline}'].values[i][j]))

        for i in range(len(data)):
            if i <= len(data)-2:
                if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                    biggest_contributor.append(None)
                else:            
                    if np.count_nonzero(parameter_metrics[i])>=1:
                        biggest_contributor.append(tolerance_mapping[pd.Series(parameter_metrics[i]).idxmax()])
                    else:                              
                        biggest_contributor.append('All values within range')
            else:
                    if np.count_nonzero(parameter_metrics[i])>=1:
                        biggest_contributor.append(tolerance_mapping[pd.Series(parameter_metrics[i]).idxmax()])
                    else:                              
                        biggest_contributor.append('All values within range')
        
    #Find sum of excursions over the timeline

        for i in range(len(data)-1):
            if data[f'{timeline} Index'].values[i+1]==data[f'{timeline} Index'].values[i]:
                row_total_excursion[i+1]+=row_total_excursion[i]
                row_total_excursion[i]=None
            

    #Create nse value over the timeline nse = sum excursions/total no. tests

        for i in range(len(data)):
            if row_total_excursion[i]:
                row_total_excursion[i]=row_total_excursion[i]/float(data[f'Total tests over {timeline}'].values[i])

     
        
    #Create F3 value over each timeline F3= nse/(0.01*nse+0.01)
        for i in range(len(data)):
            if row_total_excursion[i]:
                row_total_excursion[i]=row_total_excursion[i]/(0.01*row_total_excursion[i]+0.01)
        


    #Append F3 Value to data set

        F3_values = row_total_excursion

        data[f'F3 values over {timeline}']=F3_values
        data[f'Biggest contributor over {timeline}']=biggest_contributor

        return data
        
    #assign_F3('Day')
    #assign_F3('Week')
    assign_F3('Month')
    #assign_F3('Season')
    #assign_F3('Year')

    #Build function to assign WQI over every timeline

    def assign_WQI(timeline,data):

        F3=list(data[f'F3 values over {timeline}'])
        F2=list(data[f'F2 values over {timeline}'])
        F1=list(data[f'F1 values over {timeline}'])
        WQI = []
        rating_WQI = []


        for i in range(len(data)):
            if not math.isnan(F1[i]) and not math.isnan(F2[i]) and not math.isnan(F3[i]):
                WQI.append(100-math.sqrt((F1[i]**2+F2[i]**2+F3[i]**2)/3))
            else:
                WQI.append(None)

        data[f'WQI over {timeline}']=WQI

        
        for i in range(len(data)):
            if WQI[i]:
                if WQI[i]>=95:
                    rating_WQI.append('A')
                elif WQI[i]>=80:
                    rating_WQI.append('B')
                elif WQI[i]>=65:
                    rating_WQI.append('C')
                elif WQI[i]>=45:
                    rating_WQI.append('D')
                else:
                    rating_WQI.append('E')
            else:
                rating_WQI.append(None)
        data[f'WQI rating over {timeline}']=rating_WQI   

        data = data[[col for col in data.columns if col != f'Biggest contributor over {timeline}']+[f'Biggest contributor over {timeline}']]  

    #assign_WQI('Day')
    #assign_WQI('Week')
    assign_WQI('Month',data)
    #assign_WQI('Season')
    #assign_WQI('Year')



    #Remove unneccessary columns - all columns with measurements and used for calculations 
    #Should just have date WQI over timeline and rating
    timeline = ['Day','Week','Month','Season','Year']
    
    #Remove rows with blank WQI for Month

    data.dropna(subset=['WQI over Month'],inplace=True)

    #Add reference column

    ref = []

    for i in range(len(data)):
        ref.append(reference)

    data['Reference']=ref
        
    #Remove rows with WQI where parameters tested < 3 and total tests < 120 (avg. 4 tests per day)

    data.drop(data[data['Total tests over Month']<=120].index,inplace=True)
    data.drop(data[data['Total parameters over Month']<=2].index,inplace=True)
        
    data.drop(data.iloc[:,1:24],axis=1,inplace=True)
    data.drop(data.columns[7],axis=1,inplace=True)

    
    #Export data to destination path

    data.to_csv(destination, mode='a', index=False, header=is_first_file)

#Run the function over each file defined in file_params

    
for i, params in enumerate(file_params):
    is_first_file = (i == 0)   
    cleanse_data(params[0], params[1], params[2])

end = time.time()

print('Run time:', (float(end)-float(start)))
