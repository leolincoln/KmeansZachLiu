import sys,getopt,os
'''
inputPath is requested to be correct
inputFileName is requested to be correct
Other fields does not really matter as they can regenerate themselves

'''
inputFileName="B.txt"
inputPath="B/"
outputPath="Bout/"
threshold = 1.0

#dont change the centroidPath 
centroidPath='clusterCentroids/'
finalCentroidsPath = 'final'+inputFileName.split('.')[0]+'.txt'
OLD="old_centroids.txt"
NEW="new_centroids.txt"

def usage():
    print 'Usage: python '+sys.argv[0]+' -k k'
    print 'k: number of clusters you want to choose'


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "k:", [])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt,arg in opts:
        if opt in ("-k"):
            k = arg
    print 'the K is: '+k
    os.system("rm "+NEW)
    os.system("hdfs dfs -rm -r "+centroidPath)
    os.system("hdfs dfs -mkdir "+centroidPath)
    while not reachThreshold(threshold):
        iteration(k)
    os.system("mv "+NEW+" "+finalCentroidsPath)

def iteration(k):
    if os.path.isfile(NEW):
        os.system("mv "+NEW+" "+OLD)
        os.system("hdfs dfs -rm "+centroidPath+OLD)
        os.system("hdfs dfs -copyFromLocal "+OLD+" "+centroidPath+OLD)
    else:
        os.system("hdfs dfs -rm "+centroidPath+OLD)
        os.system("rm "+OLD)
        os.system("wait")
        os.system("hdfs dfs -cat "+inputPath+inputFileName+" | head -n "+k+" | hdfs dfs -put - "+centroidPath+OLD)
        os.system("wait")
        os.system("hdfs dfs -copyToLocal "+centroidPath+OLD+" "+OLD)
    os.system("hadoop jar kmeans-0.0.1-SNAPSHOT.jar com.hadoop.kmeans.Kmeans "+inputPath+" "+outputPath)
    os.system("wait")
    os.system("hdfs dfs -getmerge " +outputPath+"  "+NEW)

#mapping from new to old
def closestDistanceNewCluster(key,old_c,new_c):
    import numpy as np
    import math
    resultKey = key
    minDis = math.sqrt(np.sum(new_c[key]-old_c[key])**2)
    for key2 in new_c.keys():
        if math.sqrt(np.sum(new_c[key]-old_c[key2])**2)<minDis:
            minDis = math.sqrt(np.sum(new_c[key]-old_c[key2])**2)
            resultKey = key2
    return minDis


#if the new centroids and old centroids are similar then we stop the iterations
def reachThreshold(threshold):
    import numpy as np
    import math
    old_c = {}
    new_c = {}
    if not os.path.isfile(NEW):
        return False
    if not os.path.isfile(OLD):
        return False
    with open(OLD,'r') as f_old:
        lines = f_old.readlines()
        for i in xrange(len(lines)):
            old_c[i] = np.array(lines[i].split(';')[0].split()).astype(np.float32)
    with open(NEW,'r') as f_new:        
        lines = f_new.readlines()
        for i in xrange(len(lines)):
            new_c[i] = np.array(lines[i].split(';')[0].split()).astype(np.float32)
    distortion = 0
    for key in old_c.keys():
        distortion+= closestDistanceNewCluster(key,old_c,new_c)
    print 'distortion: '+str(distortion)
    if distortion<threshold:
        return True
    else:
        return False





if __name__ == "__main__":
    if len(sys.argv)<2:
        usage()
        sys.exit(2)
    main(sys.argv[1:])
