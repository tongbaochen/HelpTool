#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pymongo
import time
import csv
import os
import re
from subprocess import call
from operator import add
import json
import codecs
import numpy as np
from pprint import pprint

def write_to_txt(arr,fileName):
    try:
        f=codecs.open(fileName + '.txt' ,'w','utf-8')
        for item in arr:
            f.write(str(item)+'\r\n')
        f.close()
    except Exception as e:
        print('open error:',e)
    finally:
        print(fileName + '.txt' +'finished')

# def writeArrayToCSV(rows,filename = './' + str(year) + '_' + time.strftime(
#     "%Y-%m-%d-%H-%M-%S") + '.csv'):
def writeArrayToCSV(rows, filename='./' + '_' + time.strftime(
        "%Y-%m-%d-%H-%M-%S") + '.csv'):
    with open(filename, 'w') as w:
        a = csv.writer(w, delimiter=',')
        a.writerows(rows)
        print("writeArrayToCSV finished")
    w.close()

def readcountry_to_list(filename):
    with open(filename,'r') as f:
        data = f.readlines()
        result = []
        for line in data:
            result.append(line.rstrip())
        return result
def readcator_to_list(filename):
    with open(filename,'r') as f:
        data = f.readlines()
        result = []
        for line in data:
            mid = line.split(',')
            result.append((mid[0][2:-1],mid[1][2:-3]))
        return result

def indicators_fetch(mongo_url = "36.26.80.184:27017",database = 'bigdata',collections = "indicator"):
    '''

    :param mongo_url:服务器地址
    :return:   服务器上数据库中的国家列表和指标列表
                countryList,
                indicatorList
    '''

    # Setup database connection
    # mongo_url = "36.26.80.184:27017"
    client = pymongo.MongoClient(mongo_url)
    DATABASE = database
    db = client[DATABASE]
    COLLECTION1 = collections
    collection = db[COLLECTION1]
    #process the country list and indicator list
    countryList = collection.distinct('Country')
    # indicatorList = collection.distinct('indicators')   pymongo.errors.OperationFailure: distinct too big, 16mb cap
    # indicatorList = collection.aggregate([{'$group': {'indicators': "$myIndexedNonUniqueField"}}], [{'$group': {'_id': 1, 'count': {'$sum': 1}}}]).result[0].count()
    # indicatorList = collection.aggregate([{"$group": {"_id": '$Country',"indicators": 1, "_id": 0}}])
    if  os.path.exists('countryList.txt') == True:
        countryList = readcountry_to_list('./countryList.txt')
    else:
        countryList = collection.distinct('Country')
        write_to_txt(countryList,'countryList')
    print(countryList)

    # write_to_txt(test, 'test')
    # test2=readcator_to_list('./test_indicator.txt')
    # print(test2)
    # print(test2[0][0])
    # print(test2[0][1])
    # for i in test2:
    #     print(i[0])
    #     print(i[1])

    if os.path.exists('indicatorList.txt') == True:
        indicatorList = readcator_to_list('./indicatorList.txt')
    else:
        indicatorList = []
        for element in collection.find().batch_size(100):
            try:
                for entry in element['indicators']:
                    if not (entry,element['Data_source']) in indicatorList:
                        indicatorList.append((entry,element['Data_source']))
                    print((entry,element['Data_source']))
                    # print(element['indicators'][entry])
                indicatorList = list(set(indicatorList))
            except Exception as e:
                print(print(element))
        write_to_txt(indicatorList, 'indicatorList')
        print(indicatorList)
    return countryList,indicatorList

def readJsonToDictionary(filePath):
    '''

    :param filePath:
    :return: json对应的字典
    '''
    with open(filePath,'r') as f:
        x_one_dict = json.loads(f.read())
        return x_one_dict

def fileListUnderPath(folderPath):
    '''

    :param folderPath:路径名称
    :return:目录下的文件列表
    '''
    fileList = []
    for root, dirs, files in os.walk(folderPath):
        for afile in files:
            fileList.append(afile)
    return fileList

def writeDictionaryToJson(dictionary,filePath):
    '''

    :param dictionary:字典
    :param filePath:写入本地json文件
    :return:
    '''
    with open(filePath, 'w') as json_file:
        json_file.write(
            json.dumps(dictionary, ensure_ascii=False))  # dumps:把python对象转换成json字符串  loads:将已编码的 JSON 字符串解码为 Python 对象


#将mongodb中处理得到的国家情感得分json文件进行转换（国家的名称转换为与项目https://github.com/plkumjorn/CountryIndicatorsMining中的data files中的国家名称对应，以便将国家得分作为新的一列（变量）写入到csv文件（按年统计）中）
def score_change_json(year='2018',jsonPath='./Data Files/emotion_result/emotion_score.json',folderPath='./Data Files/test6/'):
    '''

    :param year:文件的年份
    :param jsonPath:国家情感得分文件
    :param folderPath:数据文件https://github.com/plkumjorn/CountryIndicatorsMining/tree/master/Data%20Files/Iteration%201
    :return:
    '''
    rows = []
    '''
    遍历谷歌新闻情感得分json文件
    '''
    x_one_dict = readJsonToDictionary(jsonPath)
    fileList = fileListUnderPath(folderPath)
    country_match = {}
    countryListSQL = []
    for file in fileList:#遍历fileList下的每个文件，在每个文件中的每一行中的末尾添加该国家对应的情感得分
        with open(folderPath + '/' + file, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in spamreader:
                rows.append(row)
            csvfile.close()
        for index, item in enumerate(rows):
            if index == 0:
                continue
            else:
                target = item[0][(item[0].find(':') + 2):(item[0].find('(') - 1)]
                country_match[item[0]] = target
                print(target)
                countryListSQL.append(target)
        print(countryListSQL)
        print(country_match)
        writeDictionaryToJson(country_match,'./Data Files/country_match.json')#将两文件中国家对应的dictionary存储在country_match.json中

        with open(folderPath + '/' + file, 'w', newline='') as f:#遍历fileList下的每个文件，在每个文件中的每一行中的末尾添加该国家对应的情感得分
            spamwriter = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for index, row in enumerate(rows):
                if index == 0:
                    row.append("sentiment_score_" + year)
                else:
                    if country_match[row[0]] in x_one_dict.keys():
                        row.append(x_one_dict[country_match[row[0]]][year][0])
                    else:
                        row.append('')
            spamwriter.writerows(rows)
            f.close()
        print(folderPath + '/' + file + " finished")





