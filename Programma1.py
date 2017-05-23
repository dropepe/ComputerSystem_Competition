from pyspark import SparkContext
file= sc.textFile('/home/drope/Scaricati/Progetto/train.csv', minPartitions=10)
header = file.first() #extract header
data = file.filter(lambda x:x !=header) #csv without header
data.take(2)
data = data.map( lambda x: x.replace(',',' '))
file=data.flatMap(lambda x:x.split())
file2=file.collect()
array=[]
for x in file2:
    array.append(int(x))

j=0
userID=[]
itemID=[]
rating=[]
while (j<len(array)):
    userID.append(array[j])
    itemID.append(array[j+1])
    rating.append(array[j+2])
    j=j+3

usrKeyMap=[] #Array which contains all the data of train.csv
usrID=userID[:]
for x in usrID:
    userItem={} #dictionary which key is the userID and the value is the array of items
    vectItem=[] #list that contains the list of couples of item-rating
    rateItem={} #dictionary that contains temporary the items as key and their rating
    k=0
    print usrID.index(x) #desktop control flow, can be remuved
    if (x!=0):
        while (k<len(usrID)):
           if (x==usrID[k]):
               rateItem={}
               rateItem[itemID[k]]=rating[k]
               vectItem.append(rateItem)
               usrID[k]=0 #the user that are just seen and collected in the array are deleted
           k=k+1
        userItem[x]=vectItem
        usrKeyMap.append(userItem)

itemKeyMap=[] #Array which contains all the data of train.csv 
i=0
n=0
e=0
while i<len(itemID):
    a=[]
    temDict={}
    if ((i-n+e)==(itemID[i])):
        rateUser={}
        rateUser[userID[i]]=rating[i]
        a.append(rateUser)
        b=itemID[i]
        while (i+1<len(itemID) and b==itemID[i+1]):
            rateUser={}
            rateUser[userID[i+1]]=rating[i+1]
            a.append(rateUser)
            i=i+1
            n=n+1
        temDict[itemID[i]]=a
        itemKeyMap.append(temDict)
        i=i+1
    else:
        e=e+1

def usrByIndex(i):
    return usrKeyMap[i].keys()[0]

def itemsByUsr(nrUsr):
    i=0
    ret=0
    while (ret==0 and i<len(usrKeyMap)):
        b=usrKeyMap[i].keys()[0]
        if (b==nrUsr):
            ret=usrKeyMap[i].get(nrUsr, 0)
        i=i+1
    return ret

def mostRatedItems(nrUsr):
    itemTemp=[]
    itemTemp=itemsByUsr(nrUsr)
    for x in itemTemp:
         if x.values()[0]<8:
             itemTemp.remove(x)
    return itemTemp

def itemByIndex(i):
    return itemKeyMap[i].keys()[0]

def usersByItem(item):
    i=0
    ret=0
    while (ret==0 and i<len(itemKeyMap)):
        b=itemKeyMap[i].keys()[0]
        if (b==item):
            ret=itemKeyMap[i].get(item, 0)
        i=i+1
    return ret

def userMostRate(item):
    userTemp=[]
    userTemp=usersByItem(item)
    for x in userTemp:
        if x.values()[0]<8:
            userTemp.remove(x)
    return userTemp

def meanRating():
    return sum (rating)/len(rating)

def binsByItem(itemMap):
    i=0
    m=meanRating()
    bins=[]
    while i<len(itemMap):
        somma=0
        j=0
        a=(itemMap[i].values()[0])
        while j<len(a):
            somma=float(somma+a[j].values()[0]-m)
            j=j+1
        bins.append(somma/(float(len(a)+10)))
        i=i+1
    return bins

def binsItemByID(item, binsI, itemMap, lenMap):
   i=0
   ret=0
   c=binsI
   while (ret==0 and i<lenMap):
       b=itemMap[i].keys()[0]
       if (b==item):
          ret=c[i]
       i=i+1
   return ret

def binsUserByID(user, binsU, userMap, lenMap):
   i=0
   ret=0
   c=binsU
   while (ret==0 and i<lenMap):
       b=userMap[i].keys()[0]
       if (b==user):
          ret=c[i]
       i=i+1
   return ret


def binsByUser(userMap):
    croccodile=binsByItem(itemKeyMap)
    i=0
    m=meanRating()
    bins=[]
    while i<len(userMap):
        somma=0
        j=0
        a=(userMap[i].values()[0])
        while j<len(a):
            binsItem=croccodile[j]
            somma=float(somma+a[j].values()[0]-m-binsItem)
            j=j+1
        bins.append(somma/(float(len(a)+10)))
        i=i+1
    return bins

def ratingSample():
    rGlobal=[]
    i=0
    binsU=binsByUser(usrKeyMap)
    lU=len(binsU)
    binsI=binsByItem(itemKeyMap)
    lI=len(binsI)
    m=meanRating()
    while i<len(rating):
        U=userID[i]
        I=itemID[i]
        ratingNuovo=(m+binsUserByID(U, binsU, usrKeyMap, lU)+binsItemByID(I, binsI, itemKeyMap, lI))
        rGlobal.append(ratingNuovo)
        i=i+1
        print (i, ratingNuovo)
    return rGlobal

#    a=[]
#    i=0
#    j=0
#    uM=binsByUser(usrKeyMap)
#    iM=binsByItem(itemKeyMap)
#    while i<len(uM):
#        while j<len(iM):
#            a.append(meanRating()+iM[j])
#           print (i, j, len(a))
#            j=j+1
#        i=i+1
#        print i
#    return a

#def findTopRated():

rSample=[]
rSample=ratingSample()

usrSampleMap=[] #Array which contains all the data of train.csv
usrID=userID[:]
for x in usrID:
    userItem={} #dictionary which key is the userID and the value is the array of items
    vectItem=[] #list that contains the list of couples of item-rating
    rateItem={} #dictionary that contains temporary the items as key and their rating
    k=0
    print usrID.index(x) #desktop control flow, can be remuved
    if (x!=0):
        while (k<len(usrID)):
           if (x==usrID[k]):
               rateItem={}
               rateItem[itemID[k]]=rSample[k]
               vectItem.append(rateItem)
               usrID[k]=0 #the user that are just seen and collected in the array are deleted, the program is farter that n^2
           k=k+1
        userItem[x]=vectItem
        usrSampleMap.append(userItem)

itemSampleMap=[] #Array which contains all the data of train.csv 
i=0
n=0
e=0
while i<len(itemID):
    a=[]
    temDict={}
    if ((i-n+e)==(itemID[i])):
        rateUser={}
        rateUser[userID[i]]=rating[i]
        a.append(rateUser)
        b=itemID[i]
        while (i+1<len(itemID) and b==itemID[i+1]):
            rateUser={}
            rateUser[userID[i+1]]=rSample[i+1]
            a.append(rateUser)
            i=i+1
            n=n+1
        temDict[itemID[i]]=a
        itemSampleMap.append(temDict)
        i=i+1
    else:
        e=e+1

def findTopRated():
    copy=rSample[:]
    i=0
    ex=[]
    maxis=()
    while i<len(copy):
        if copy[i]>8:
            maxis=(copy[i] , itemID[i], i)
            ex.append(maxis)
        i=i+1
    return ex

import random
def TopRatedRandom():
    i=0
    j=0
    a=[]
    top=findTopRated()
    while i<4198:
        b=[]
        while j<5:
            coseCaso=int(random.random()*len(top))
            b.append(top[coseCaso])
            print b
            j=j+1
        a.append(b)
        i=i+1
    return a

def itemsSampleByUsr(nrUsr):
    i=0
    ret=0
    while (ret==0 and i<len(usrSampleMap)):
        b=usrSampleMap[i].keys()[0]
        if (b==nrUsr):
            ret=usrSampleMap[i].get(nrUsr, 0)
        i=i+1
    return ret

def mostRatedSampleItems(nrUsr):
    itemTemp=[]
    itemTemp=itemsSampleByUsr(nrUsr)
    for x in itemTemp:
         if x.values()[0]<5:
             itemTemp.remove(x)
    return itemTemp

def takeTheBestItemForUser():
    i=0
    bestone=[]
    best={}
    while i<len(usrKeyMap):
       a=[]
       a=mostRatedItems(usrKeyMap[i].keys()[0])
       bestone.append(a)
       i=i+1
    return bestone

def takeTheBestItemSampleForUser():
    i=0
    bestone=[]
    best={}
    while i<len(usrSampleMap):
       a=[]
       a=mostRatedSampleItems(usrSampleMap[i].keys()[0])
       bestone.append(a)
       i=i+1
    return bestone

#the same as below, but the rating is the real one
def takeBest5():
    i=0
    ret=[]
    best=takeTheBestItemForUser()
    bestoni=best[:]
    while i<len(usrKeyMap):
        maX=bestoni[i]
        while len(maX)>5:
            bestoni[i].remove(min(bestoni[i]))
        i=i+1
        ret.append(maX)
    return ret

#takes the best5  item considering the sample rating for each user mapping in a list of dictionaries
def takeBest5Sample():
    i=0
    ret=[]
    best=takeTheBestItemSampleForUser()
    bestoni=best[:]
    while i<len(usrSampleMap):
        maX=bestoni[i]
        while len(maX)>5:
            bestoni[i].remove(min(bestoni[i]))
        i=i+1
        ret.append(maX)
    return ret

#this function takes a list of dictionares contained inside the map and for each one takes the keys (items) and makes a string of it
def makeDictionaryString(listOFdictionary):
    i=0
    a=''
    while i<len(listOFdictionary):
        a=a+str(listOFdictionary[i].keys()[0])+' '
        i=i+1
    return a

#this function takes as arguments the map in which are contained the users and th array of item to export and saves the file in my home in a file called Submussion2.csv
def exportCSVfile(mapOfUsers, arrayToExport):
   with open('/home/drope/Submission2.csv', 'wb') as myfile:
       i=0
       wr=csv.writer(myfile,delimiter=',', quoting=csv.QUOTE_NONE)
       wr.writerow(['userId', 'RecommendedItemIds'])
       while i<len(arrayToExport):
          bes=makeDictionaryString(arrayToExport[i])
          string1=str(mapOfUsers[i].keys()[0])
          wr=csv.writer(myfile,delimiter=',', quoting=csv.QUOTE_NONE)
          wr.writerow([string1, bes])
          i=i+1

def affineItemsUsers():
    i=0
    usrAffinity=[]
    while i<len(usrKeyMap):
       items=itemsByUsr(usrKeyMap[i].keys()[0])
       j=0
       usrdict={}
       vect=[]
       while j<len(items):
           users=usersByItem(items[j].keys()[0])
           k=0
           udict={}
           while k<len(users):
               if users[k].keys()[0]!=usrKeyMap[i].keys()[0]:
                   if udict.get(users[k].keys()[0], 0)==0:
                       uvect=[]
                   uvect.append(items[j].keys()[0])
                   udict[users[k].keys()[0]]=uvect
               k=k+1
           vect.append(udict)
           print (i, j, k)
           j=j+1
       usrdict[usrKeyMap[i].keys()[0]]=vect
       print usrdict
       usrAffinity.append(usrdict)
       i=i+1
    return usrAffinity

def affineUsrItems():
   i=0
   itemAffinity=[]
   while i<len(itemKeyMap):
       users=usersByItems(itemKeyMap[i].keys()[0])
       j=0
       itemdict={}
       vect=[]
       while j<len(users):
           items=itemsByUsr(users[j].keys()[0])
           k=0
           userdict={}
           itemvect=[]
           while k<len(items):
               if items[k].keys()[0]!=itemKeyMap[i].keys()[0]:
                   itemvect.append(items[k])
               k=k+1
           userdict[users[j].keys()[0]]=itemvect
           vect.append(userdict)
           print (i, j, k)
           j=j+1
       itemdict[itemKeyMap[i].keys()[0]]=vect
       print itemdict
       itemAffinity.append(itemdict)
       i=i+1
   return itemAffinity
def meanusrrating(usr):
    items=itemsByUsr(usr)
    i=0
    somma=0
    media=0
    while i<len(items):
        somma=somma+items[i].values()[0]
        i=i+1
    media=somma/len(items)
def userAffinity(indexofuser):
    i=0
    while i<len(usrAffinity_usr_item_usersRating[indexofuser]):
        usrAffinity_usr_item_usersRating[indexofuser].values[0]
#correlations(usrAffinity):

