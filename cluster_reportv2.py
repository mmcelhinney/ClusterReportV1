#!/Library/Frameworks/Python.framework/Versions/3.11/bin/python3

import datetime
from functools import reduce
import pandas as pd
from glob import glob
import shutil
import os.path  
import subprocess as sp
import sys, getopt
import tarfile
import time
import tableauserverclient




#Options
inputdir = ''
opts, args = getopt.getopt(sys.argv,"hi:o:",["idir="])
for opt, arg in opts:
    if opt == '-h':
        print ('test.py -i <inputfile> -o <outputfile>')
        sys.exit()
    elif opt in ("-i", "--idir"):
        inputfile = arg


#Set Data for folder_name
Date_N = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
start_time = time.time()


#Create output directory
ds_folder=("./Datasource" + Date_N)
root_path=(sys.argv[2])



isExist = os.path.exists(ds_folder) # Check whether the specified path exists or not
if not isExist:
   os.makedirs(ds_folder) # Create a new directory because it does not exist


isExist = os.path.exists(ds_folder + "/Datasources") # Check whether the specified path exists or not
if not isExist:
   os.makedirs(ds_folder + "/Datasources") # Create a new directory because it does not exist


# data_folder = (ds_folder + "/Datasources")
data_folder = ("/Users/mmcelhinney/python_test/Output")
ds_path = (os.path.abspath(ds_folder))

print("ds_folder:" + ds_folder)
print("data_folder:" + data_folder)
print("ds_path:" + ds_path)

file_stats = os.stat(root_path)

# print(file_stats)
# print(f'File Size in Bytes is {file_stats.st_size}')
filesize = str(round(file_stats.st_size / (1024 * 1024 * 1024),2))




# Unpack Cluster Report
with tarfile.open(root_path, "r") as tf: 
    tf.extractall(path=ds_folder)
    print("All files extracted!")

# print("Full DS Directory : " os.path.abspath(ds_folder))


# ds_path = ('/Users/mmcelhinney/Output')



# Set output files
ps_l_out_f=os.path.join(data_folder, "process_list_output.tsv")
pc_out_f=os.path.join(data_folder, "plan_cache_output.tsv")
mv_ev_out_f=os.path.join(data_folder, "mv_events.tsv")
mv_nodes_f=os.path.join(data_folder, "mv_nodes.tsv")
cpu_out_f=os.path.join(data_folder, "cpu_count.csv")
mem_out_f=os.path.join(data_folder, "mem_count.csv")
part_out_f=os.path.join(data_folder, "part_count.csv")
tab_stat_out_f=os.path.join(data_folder, "Table_Stats.tsv")
qry_out_f=os.path.join(data_folder, "Qry_Stats.tsv")


# Set search Directories/
cpu_path=(ds_folder + "/**/proc/cpuinfo")
pslistpath=(ds_folder + "/**/*schema-processlist.tsv")
planCchepath=(ds_folder + "/**/*show_plancache.tsv")
mvEventspath=(ds_folder + "/**/*mv-events.tsv")
mvNodespath=(ds_folder + "/**/*mv-nodes.tsv")
mem_path=(ds_folder + "/**/proc/meminfo")
tab_path=(ds_folder + "/**/*table-statistics.tsv")
qry_path=(ds_folder + "/**/*mv-queries.tsv")
# partitionpath=(ds_folder+ "/**/*show-partitions-recommendation_groupon_us.tsv")
partitionpath=(ds_folder+ "/**/*show-partitions-*.tsv")


def emptyfile(filepath):
    return ((os.path.isfile(filepath) > 0) and (os.path.getsize(filepath) > 0))


def merge_files(pathtofiles, outfile):
    with open(outfile, 'wb') as outfile:
        for i,filename in enumerate(glob(pathtofiles, recursive=True)):
            if filename == pathtofiles:
                # don't want to copy the output into the output
                continue
            with open(filename, 'rb') as readfile:
                if i != 0:
                    readfile.readline()  # Throw away header on all but first file
                shutil.copyfileobj(readfile, outfile)


def Cvrt_csv(pathoffiles):
    tsvfiles = glob(pathoffiles + "/*.tsv") 
    for t in tsvfiles:
        print("Converting the following file : " + t)
        # if os.stat(t).st_size == 0:
        tsv = pd.read_table(t, sep='\t',dtype='unicode')
        tsv.to_csv(t[:-4] + '.csv', index=False)
        os.remove(t)



#MevEvents Join
def mvEvents(outfile1,outfile2):

    mv_ev_out_csv=os.path.join(data_folder, "mv_events.csv")
    mv_nodes_csv=os.path.join(data_folder, "mv_nodes.csv")

# Read into Panda

    df1 = pd.read_csv(mv_nodes_csv)
    df2 = pd.read_csv(mv_ev_out_csv)


    merged_df = df1.merge(df2,left_on="ID",right_on="ORIGIN_NODE_ID").fillna("")

    merged_df.to_csv(os.path.join(data_folder, "merged.csv"), index=False)



#Get Core Count
def cpu_count(pathtofiles, outfile):
    with open(outfile, 'wb') as outfile:
        headerlist = ["Node", "CpuCount"]

        command = "rg -N 'siblings\t:' " + ds_path + " | uniq | awk -F'-' '{print substr($0, index($0,$7))}' | sort -n | sed 's/\/proc\/cpuinfo:siblings//g ; s/\t://g ; s/ /,/g ; 1 s/.*/Node,CPU/g' > " + cpu_out_f
        completed_process = sp.Popen(command, shell=True, text=True, stdout=outfile)


#Get Memory Count
def mem_count(outfile1):
    with open(outfile1, 'wb') as outfile1:
        headerlist = ["Node", "Memory"]

        command1 = "rg -N 'MemTotal:' " + ds_path + " | uniq | awk -F'-' '{print substr($0, index($0,$7))}' | sort -n | sed 's/\/proc\/meminfo:MemTotal://g; s/       /,/g;1 s/.*/Node,Memory(KB)/g ; s/ kB//g' > " + mem_out_f
        completed_process1 = sp.Popen(command1, shell=True, text=True, stdout=outfile1)
        outfile1.close()


#Get Table Statistics


#Get Cluster Partition Info

    print(part_out_f)
    print(partitionpath)

# Function Calls
merge_files(pslistpath,ps_l_out_f)
merge_files(planCchepath,pc_out_f)
merge_files(mvEventspath,mv_ev_out_f)
merge_files(mvNodespath,mv_nodes_f)
merge_files(partitionpath,part_out_f)
cpu_count(cpu_path,cpu_out_f)
mem_count(mem_out_f)
merge_files(qry_path, qry_out_f)
merge_files(tab_path,tab_stat_out_f)
Cvrt_csv(data_folder)

#Merge Events 
mvEvents(mv_nodes_f,mv_ev_out_f)


print('The size of the file is :' + filesize + 'GB')
print("Time taken to execute : " + str(round((time.time() - start_time),2)) + ' Seconds')


