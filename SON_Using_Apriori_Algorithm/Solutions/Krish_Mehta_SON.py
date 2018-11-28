#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 22:47:49 2018

@author: krishmehta
"""

from __future__ import print_function
import os, sys
from pyspark import SparkContext
from operator import add
from collections import Counter
from itertools import combinations
import time
import math

start = time.time()
User_Review_CSV = sys.argv[1]#"/Users/krishmehta/Desktop/DataMining/Krish_Mehta_HW3/hw3/Data/yelp_reviews_test.txt"
sc = SparkContext(appName="Krish_Mehta").getOrCreate()
userData = sc.textFile(User_Review_CSV,None,False)
userData = userData.map(lambda x: x.split(','))
userDataRdd = userData.map(lambda x: (x[0], x[1])).groupByKey().mapValues(set)# grouping with index and list of string in index
userDataRdd = userDataRdd.map(lambda x: x[1])# removing the index
count = userDataRdd.count()
support = float(sys.argv[2])#40

def getCandidateItem2(frequentItemSingleton):
    subsetOfSize2 = list()
    frequentItemSingleton = set(frequentItemSingleton)
    for subset in combinations(frequentItemSingleton, 2):
        subset = list(subset)
        subset.sort()
        subsetOfSize2.append(subset)

    return subsetOfSize2
    

def getFilteredItems(baskets, candidatesItem, newthreshold):
    dictCountFrequency = {}
    for candidate in candidatesItem:
        candidate = set(candidate)
        sortedValues = sorted(candidate)
        tupleValues = tuple(sortedValues)
        for basket in baskets:
            if candidate.issubset(basket):
                dictCountFrequency[tupleValues] = dictCountFrequency.get(tupleValues,0)+1
    #print(len(dictCountFrequency))
    frequentValues = {x : dictCountFrequency[x] for x in dictCountFrequency if dictCountFrequency[x] >= newthreshold }
    frequentValues = sorted(frequentValues)
    #print(len(frequentValues))
    return frequentValues


def getCandidateItemMore(frequentItems, lengthOfPair):
    subsetOfSizeMore = list()
    lengthA=len(frequentItems)-1
    lengthB=lengthA+1
    for i in range(lengthA):
        for j in range(i+1,lengthB):
            partA = frequentItems[i]
            partB = frequentItems[j]
            if partA[0:(lengthOfPair-2)] == partB[0:(lengthOfPair-2)]:
                subsetOfSizeMore.append(list(set(partA) | set(partB)))
            else:
                break
                
    return subsetOfSizeMore
    


def apriori(baskets, support, count):
    listOfBaskets = list(baskets)
    length = len(listOfBaskets)
    newthreshold = float(support)*(float(length)/float(count))
    firstItemCounts = Counter()
    #print("here is the basket")
    for basket in listOfBaskets: #did not use dict as basket has a datatype of set
        firstItemCounts.update(basket)
    frequentItemSingleton = {x : firstItemCounts[x] for x in firstItemCounts if firstItemCounts[x] >= newthreshold }
    frequentItemSingleton = sorted(frequentItemSingleton) #sort the dict and then it converts to list
    # here used extends instead of append as extends converts the list to single values and then appends it to the list
    finalFrequentItem=list()
    finalFrequentItem.extend(frequentItemSingleton)
    # now for the value for 2 pair candidate set
    # this returns a list
    candidateItem2=getCandidateItem2(frequentItemSingleton)
    #filter the sorted item
    frequentItemDouble = getFilteredItems(listOfBaskets, candidateItem2, newthreshold)
    finalFrequentItem.extend(frequentItemDouble)
    lengthOfPair = 3
    #print(type(frequentItemDouble))
    frequentItems = frequentItemDouble
    #print (frequentItems)
    
    while len(frequentItems)!=0:
        candidateItemMore = getCandidateItemMore(frequentItems, lengthOfPair)
#         print("THis is the value of candidate", candidateItemMore)
        frequentItems = getFilteredItems(listOfBaskets, candidateItemMore, newthreshold)
        finalFrequentItem.extend(frequentItems)
#         print ("|||||||||||||||||||||||BBBBBBBBBBBBBBBBBBBBBBB||||||||||||||||||||||||||||||||||||||||||||||||")
#         print (frequentItemMore)
#         print ("|||||||||||||||||||||||AAAAAAAAAAAAAAAAAAAAAAA||||||||||||||||||||||||||||||||||||||||||||||||")
#         print (list(set(frequentItemMore)))
        frequentItems.sort()
        lengthOfPair=lengthOfPair+1
        
        
        
        
    
    
    return finalFrequentItem


def candidateCount(buckets, candidates):
    itemCountDict = {}
    buckets = list(buckets)
    for candidate in candidates:
        if type(candidate) is str:
            candidate = [candidate]
            key = tuple(sorted(candidate))
        else:
#             print(candidate)
            key = candidate
            
        candidate = set(candidate)
        for basket in buckets:
            if candidate.issubset(basket):
                itemCountDict[key] = itemCountDict.get(key,0)+1

    return itemCountDict.items()


#Phase 1

outputMap1 = userDataRdd.mapPartitions(lambda baskets : apriori(baskets, support, count)).map(lambda x: (x,1))
outputReduce1 = outputMap1.reduceByKey(lambda x,y: (1)).keys().collect()

#Phase 2
outputMap2 = userDataRdd.mapPartitions(lambda baskets : candidateCount(baskets, outputReduce1))
outputReduce2 = outputMap2.reduceByKey(lambda x,y: (x+y))

final = outputReduce2.filter(lambda x: x[1] >= support).sortBy(lambda x:(len(x[0]),x[0])).map(lambda x: x[0])

#print(final.collect())
outputWrite=final.collect()

initialLength=len(outputWrite[0])

outputFile = sys.argv[3]#"/Users/krishmehta/Desktop/DataMining/Krish_Mehta_HW3/hw3/Data/Krish_Mehta_SON_yelp_reviews_test_40.txt"
fileToOpen = open(outputFile,'w')


if (initialLength ==1):
    fileToOpen.write('('+str(outputWrite[0][0])+')')
elif (initialLength>1):
    fileToOpen.write(str(outputWrite[0][0]))


for x in outputWrite[1:]:
    if len(x)==initialLength:
        if len(x)==1:
            temp = str(x[0])
            temp = temp.replace('\'','')
            temp = temp.replace(' ','')
            fileToOpen.write(',('+temp+')')
        else:
            temp = str(x)
            temp = temp.replace('\'','')
            temp = temp.replace(' ','')
            fileToOpen.write(','+temp)
    else:
        temp = str(x)
        temp = temp.replace('\'','')
        temp = temp.replace(' ','')
        fileToOpen.write('\n\n')
        fileToOpen.write(temp)
        initialLength=len(x)


fileToOpen.close()

print("End time", time.time()-start)