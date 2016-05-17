# -*- coding: utf-8 -*-
"""
Created on Fri May 13 17:22:22 2016

@author: xinruyue
"""
from pymongo import MongoClient
import datetime
import collections
import csv


db = MongoClient("10.8.8.111:27017")['eventsV35']
event = db['eventV35']

'''
得到top10的videoId
'''
def get_videos(startTime,endTime,eventKey,num):
    pipeline = [
    {"$match":{"eventKey":eventKey,"serverTime":{"$gte":startTime,"$lte":endTime}}},
    {"$group":{"_id":"None","videoIds":{"$push":"$eventValue.videoId"}}}]
    result = list(event.aggregate(pipeline))[0]['videoIds']
    
    video_count = {}
    for each in result:
        video_count[each] = result.count(each)
    
    video_sort = sorted(video_count.iteritems(), key=lambda d:d[1], reverse = True)
    
    video_tops = []
    for each in video_sort:
        if video_sort.index(each) < num:
            video_tops.append(each[0])
    return(video_tops)

'''
得到每一天的top10视频的每一个的完成量,每天视频完成的总量
'''
def video_visitors(video_tops,beginTime,finishTime,eventKey):
    num_videos = collections.OrderedDict()    
    for each in video_tops:
        pipeline = [
        {"$match":{"eventKey":eventKey,"serverTime":{"$gte":beginTime,"$lte":finishTime},
                   "eventValue.videoId":each}},
        {"$group":{"_id":"None","videoId":{"$push":"$eventValue.videoId"}}}]
        try:
            result = list(event.aggregate(pipeline))[0]['videoId']
            num_videos[each] = len(result)
        except:
            print "Maybe something wrong!"
    sum_videos = event.find({"eventKey":eventKey,"serverTime":{"$gte":beginTime,"$lte":finishTime}}).count()
    return(num_videos,sum_videos)
     

startTime = datetime.datetime(2016,5,5)
endTime = datetime.datetime(2016,5,6)
video_tops = get_videos(startTime,endTime,"finishVideo",10)
print video_tops

timeDelta = datetime.timedelta(days = 1)
for i in range(3):
    beginTime = datetime.datetime(2016,5,i+5)
    finishTime = beginTime + timeDelta
    video_finish = video_visitors(video_tops,beginTime,finishTime,"finishVideo")
    video_ids = video_finish[0]
    total_videos = [video_finish[1]]*10

    video_per = []
    for each in video_ids.values():
        video_per.append(float(each)/total_videos[0])

    result = zip(video_ids,video_ids.values(),total_videos,video_per)
    print result
    csvfile = file(str(beginTime)+".csv","wb")
    writer = csv.writer(csvfile)
    writer.writerow(['Id','videoView','totalVideoView','percent'])
    writer.writerows(result)
    csvfile.close()

    

        
    
        
    
    
    
    
    
    

