import pyspark

input_file='final_fcc.csv'
conf = pyspark.SparkConf().set('spark.driver.host', '127.0.0.1')
sc = pyspark.SparkContext(master='local', appName='task1', conf=conf)
all_data=sc.textFile(input_file).filter(lambda x:x!=',BlockCode,ProviderName,MaxAdDown,MaxAdUp').\
    map(lambda x:x.split(','))
block_provider_len=all_data.map(lambda x:(x[1],x[2])).groupByKey().mapValues(set).mapValues(list).mapValues(len).collect()
block_max_down=all_data.map(lambda x:(x[1],x[3])).groupByKey().mapValues(max).collect()
block_max_up=all_data.map(lambda x:(x[1],x[4])).groupByKey().mapValues(max).collect()

res='BlockID,#ISP,max_down,max_up\n'
for i in zip(block_provider_len,block_max_down,block_max_up):
    res+=i[0][0][:11]+','+str(i[0][1])+','+i[1][1]+','+i[2][1]+'\n'

output_file='summarize_fcc.csv'
with open(output_file, 'w') as f:
    f.write(res)



