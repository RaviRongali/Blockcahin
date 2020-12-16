import json
# import matplotlib
import matplotlib.pyplot as plt
# jsonfile="test2.json"
def longestchainheight(jsonfile):
    with open(jsonfile) as json_file: 
            data = json.load(json_file)
            data=data.items()
            maxIndex=0
            for i,j in data:
                maxIndex=max(maxIndex,j["index"])
            return maxIndex

def totalblocks(jsonfile):
    with open(jsonfile) as json_file: 
            data = json.load(json_file)
            data=data.items()
            return len(data)

def selfblocks(jsonfile):
    with open(jsonfile) as json_file: 
            data = json.load(json_file)
            data=data.items()
            count=0
            for i,j in data:
                if j["type"]=="me":
                    count+=1
            return count
def getblock(ind,jsonfile):
    with open(jsonfile) as json_file: 
            data = json.load(json_file)
            data=data.items()
            curhash=0
            curtimestamp=0
            for x,v in data:
                if v["index"]==ind:
                    # print(v["index"])
                    if curtimestamp>float(v["time_stamp"]) or curtimestamp==0:
                        curtimestamp = v["time_stamp"]
                        curhash = x
                        # print(x)
            return curhash
def longchainblocks(jsonfile):
    ownblock=0
    curhash=getblock(longestchainheight(jsonfile),jsonfile)
    # print(curhash)
    with open(jsonfile) as json_file: 
            data = json.load(json_file)
            while(data[curhash]["previousHash"]!="null"):
                if data[curhash]["type"]=="me":
                    ownblock+=1
                curhash=data[curhash]["previousHash"]
    return ownblock

# print("longestchain",longestchainheight())
# print("total",totalblocks())
# print("selfmined blocks",selfblocks())
# print("selfmined inside mainchain",longchainblocks())

def dataplot1(x,y,title,safeimg):
    # plt.figure(i)
    plt.plot(x,y)
    plt.xlabel('inter arrival time')
    plt.ylabel('Mining power utilisation')
    plt.title(title)
    plt.savefig(safeimg)
    plt.close()

def dataplot2(x,y,title,safeimg):
    # plt.figure(i)
    plt.plot(x,y)
    plt.xlabel('inter arrival time')
    plt.ylabel('fraction of adversary blocks in main chain')
    plt.title(title)
    plt.savefig(safeimg)
    plt.close()




import os
l=[2,3,4,6]
perc=[10,20,30]
for j in perc:
    mining_util = []
    for i in l:
        longchain=[]
        Totalblocks=[]
        folder=str(i)+"sec_"+str(j)
        # print(folder)
        for root, dirs, files in os.walk("./"+folder, topdown=False):
            for name in files:
                # print(name)
                if name.endswith(".json"):
                    filepath=os.path.join(root, name)
                    height=longestchainheight(filepath)
                    total_blocks=totalblocks(filepath)
                    longchain.append(height)
                    Totalblocks.append(total_blocks)
                    # print(totalblocks(filepath))
        mining_util.append(sum(longchain)/sum(Totalblocks))
    k="Mining power utilization vs inter arrival time where %"+str(j)+" nodes are flooded"     
    dataplot1(l,mining_util,k,"plot1_"+str(j))



for j in perc:
    mining_util = []
    for i in l:
        folder=str(i)+"sec_"+str(j)
        # print(folder)
        for root, dirs, files in os.walk("./"+folder, topdown=False):
            for name in files:
                # print(name)
                if name.endswith(".json") and "adversary" in name:
                    filepath=os.path.join(root, name)
                    height=longestchainheight(filepath)
                    adversaryblocks=longchainblocks(filepath)
                    mining_util.append(adversaryblocks/height)
                    # print(totalblocks(filepath))   
    k="fraction of adversary blocks vs inter arrival time where %"+str(j)+" nodes are flooded"     
    dataplot2(l,mining_util,k,"plot2_"+str(j))

