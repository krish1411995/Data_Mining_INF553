#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 21:10:14 2018

@author: krishmehta
"""
from __future__ import print_function
import os, sys
from pyspark import SparkContext
from operator import add
import time
import math


time1=time.time()
Train_CSV = sys.argv[1] #"/Users/krishmehta/Desktop/DataMining/Krish_Mehta_HW2/hw2/Data/train_review.csv"
Test_CSV = sys.argv[2] #"/Users/krishmehta/Desktop/DataMining/Krish_Mehta_HW2/hw2/Data/test_review.csv"
fileToOpen = open("Krish_Mehta_UserBasedCF.txt", 'w')
fileToOpen.write("UserID,MovieId,Pred_rating" + "\n") 
# sc.stop()
sc = SparkContext(appName="Krish_Mehta").getOrCreate()
trainData = sc.textFile(Train_CSV,None,False)
dataHeader = trainData.first()
trainData = trainData.filter(lambda x: x != dataHeader) 
testData = sc.textFile(Test_CSV,None, False)
dataHeader=testData.first()
testData = testData.filter(lambda x: x!=dataHeader)
trainDataCollectMap1 = trainData.map(lambda x: x.split(','))# just the split with ','
trainDataCollectMap = trainDataCollectMap1.map(lambda x: ((x[0]),((x[1]),float(x[2])))).groupByKey().mapValues(dict)\
.collectAsMap()#a nested dict as {user:{business:value}}
trainDataBusinessMap = trainDataCollectMap1.map(lambda x: ((x[1]),((x[0]),float(x[2])))).groupByKey().mapValues(dict)\
.collectAsMap()#a nested dict as {business:{business:value}}
trainDataCollectMap_bc=sc.broadcast(trainDataCollectMap)# broadcast so that we get the dictionary to all the childs and
trainDataBusinessMap_bc=sc.broadcast(trainDataBusinessMap)# the child wont have to access it from the master everytime
def getthevalue1(user1,item,trainDataCollectMap1, trainDataBusinessMap_bc1):
    trainDataCollectMap=trainDataCollectMap1.value
    trainDataBusinessMap_bc=trainDataBusinessMap_bc1.value
    if trainDataCollectMap.get(user1):#check whether the user is present in th training data
        index_user1=0#pointer to the list of user1 businesses
        sum_user1=0#sum for the user1
        sum_user2=0#sum for user2
        list_of_item1_index=[]#stores the pointer to the list_of_item1
        list_of_item2_index=[]#stores the pointer to the list_of_item2
        value_of_required=-1#this is the value contained of the user2 for the item to be predicted
        user1List=list(trainDataCollectMap.get(user1))#list of items in userA
        userAverage=trainDataCollectMap.get(user1)#fetch all the {business:rating} of the user
        userAverage1=sum(userAverage.values())#summing for the average
        length_of_Item1=len(user1List)#number of items in the userA
        listOfWeightRate=[]#list of tuple with values rateDifference and weight for all the users except userA
        #print(trainDataBusinessMap_bc.get(item))
        if (trainDataBusinessMap_bc.get(item)==None):
            #print(user1+","+item+","+str(2.7)+"\n")
            average_of_current_user=userAverage1/len(list(userAverage))
            return(user1,item,str(average_of_current_user))
        else:
            listOfUsers=list(trainDataBusinessMap_bc.get(item))#get the list of usersB who have rated business(item)
            if(len(listOfUsers)!=0):
                average_of_current_user=userAverage1/len(list(userAverage))# average of the userA
                #print("average of krish", average_of_current_user)
                for i in range(len(listOfUsers)):
                    del list_of_item1_index[:]# clear the list so that it does not append (not necessary but just precausion)
                    del list_of_item2_index[:]# clear the list so that it does not append (not necessary but just precausion)
                    sum_user1=0#sum for the user1
                    sum_user2=0#sum for user2
                    index_user1=0
                    value_of_required=trainDataCollectMap[listOfUsers[i]].get(item)#value of the rating for the business rating for the business to be found
                    #print("This is value of required",value_of_required)
                    while(index_user1<length_of_Item1):#loop until the number of businesses in the userA
                        if(trainDataCollectMap[listOfUsers[i]].get(user1List[index_user1])):
                            sum_user1+=trainDataCollectMap[user1].get(user1List[index_user1])
                            sum_user2+=trainDataCollectMap[listOfUsers[i]].get(user1List[index_user1])
                            list_of_item2_index.append(trainDataCollectMap[listOfUsers[i]].get(user1List[index_user1]))
                            list_of_item1_index.append(trainDataCollectMap[user1].get(user1List[index_user1]))
                        index_user1+=1
                    index_user1=0
                    if len(list_of_item1_index)!=0:
                        averageUser1=sum_user1/len(list_of_item1_index)
                        averageUser2=sum_user2/len(list_of_item2_index)
                        numerator=0
                        denominator=0
                        denomA=0
                        denomB=0
                        weight=0
                        value1=0
                        value2=0
                        for i in range(len(list_of_item1_index)):
                            value1=list_of_item1_index[i]-averageUser1
                            value2=list_of_item2_index[i]-averageUser2
                            numerator+=(value1*value2)
                            denomA+=value1*value1
                            denomB+=value2*value2
                        denominator=math.sqrt(denomA)*math.sqrt(denomB)
                        if denominator !=0:
                            weight=numerator/denominator
                        ratesdifference=0
                        #print("the weight is",weight)
                        ratesdifference=(value_of_required - averageUser2) * weight
                        listOfWeightRate.append((ratesdifference,weight))
                #print(list(listOfWeightRate))
                numerator1 = sum(n for n,_ in listOfWeightRate)
                #print(numerator1)
                denominator1 = sum(abs(n) for _,n in listOfWeightRate)
                #print(denominator1)
                predicted_rating=-1
                if numerator1==0 or denominator1==0:
                    predicted_rating=average_of_current_user#average_of_current_user#left to code
                    #print(user1+","+item+","+str(predicted_rating)+"\n")
                    return(user1,item,str(predicted_rating))
                else:
                    predicted_rating = (numerator1 / denominator1)/2
                    predicted_rating += average_of_current_user
                    if(predicted_rating>5.0):
                        predicted_rating=5.0
                    elif(predicted_rating<0.0):
                        predicted_rating=0.0
                    #print(user1+","+item+","+str(predicted_rating)+"\n") 
                    return(user1,item,str(predicted_rating))
            else:
                return (user1,item,str("3.0"))
    else:
        return (user1,item,str("3.0"))
testData_rdd = testData.map(lambda x: x.split(',')).sortBy(lambda x: ((x[0]),(x[1])))

weight= testData_rdd.map(lambda x: getthevalue1(x[0],x[1],trainDataCollectMap_bc,trainDataBusinessMap_bc)).collect()

length = len(weight)
for i in range(0,length):
    fileToOpen.write(weight[i][0] + "," + weight[i][1] + "," + str(weight[i][2]) +"\n")
fileToOpen.close()
fcalculate = sc.textFile("Krish_Mehta_UserBasedCF.txt",None,False)
fheader = fcalculate.first()
fdata = fcalculate.filter(lambda x: x != fheader)
fSplitValues = fdata.map(lambda x: x.split(',')).map(lambda x: (((x[0]),(x[1])),float(x[2])))
testDataSplitValues = testData.map(lambda x: x.split(',')).map(lambda x: (((x[0]),(x[1])),float(x[2])))
joinTestData = testDataSplitValues.join(fSplitValues).map(lambda x: (((x[0]),(x[1])),abs(x[1][0]-x[1][1])))
num1 = joinTestData.filter(lambda x: x[1]>=0 and x[1]<1).count()
num2 = joinTestData.filter(lambda x: x[1]>=1 and x[1]<2).count()
num3 = joinTestData.filter(lambda x: x[1]>=2 and x[1]<3).count()
num4 = joinTestData.filter(lambda x: x[1]>=3 and x[1]<4).count()
num5 = joinTestData.filter(lambda x: x[1]>= 4).count()
print (">=0 and <1:",num1)
print(">=1 and <2:",num2)
print(">=2 and <3:",num3)
print(">=3 and <4:",num4)
print(">=4:",num5)
rdd1=joinTestData.map(lambda x:x[1]**2).reduce(lambda x,y:x+y)
rmse=math.sqrt(rdd1/fSplitValues.count())
print("RMSE: ",rmse)
time2=time.time()
print("Time: ", str(time2-time1))