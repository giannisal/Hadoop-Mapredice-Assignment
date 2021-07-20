from mrjobscript import Kmeanshadoop
import os
if __name__ == '__main__':
    '''
    centroids = []
    with open ('hadoopcentroids.txt') as fp:
        for line in fp:
            line = line.strip()
            point = line.split(',')
            centroids.append([ float(point[0]) , float(point[1])])
            print(centroids)
    '''
    #mr_job= Kmeanshadoop(args=['-r', 'local', '--cmdenv', 'CENTROIDS=/home/giannis/Downloads/hadoopcentroids.txt','--output-dir','/home/giannis/Downloads','/home/giannis/Downloads/hadoopdata.txt'])
    #mr_job= Kmeanshadoop(args=['-r', 'hadoop','--cmdenv','CENTROIDS=hdfs:///Data/hadoopcentroids.txt','--output-dir','hdfs:///data/output','hdfs:///Data/hadoopdata.txt'])
    #mr_job= Kmeanshadoop(args=['-r', 'hadoop','--cmdenv','CENTROIDS=hdfs:///Data/hadoopcentroids.txt','hdfs:///Data/hadoopdata.txt'])
    mr_job= Kmeanshadoop(args=['-r', 'inline','--cmdenv','CENTROIDS=/home/giannis/Downloads/hadoopcentroids.txt','--no-bootstrap-mrjob' , '--output-dir','/home/giannis/Downloads', '/home/giannis/Downloads/hadoopdata.txt'])

    with mr_job.make_runner() as runner:
         runner.run()
