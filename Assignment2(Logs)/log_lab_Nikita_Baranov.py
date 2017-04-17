import sys
from pyspark import SparkContext, SparkConf
import argparse

conf = SparkConf().setAppName("wordcount").setMaster("local")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

parser = argparse.ArgumentParser(description='Log Analisis Lab Nikita Baranov.')
parser.add_argument('-q', help='Number of Question', nargs=1, required=True)
parser.add_argument('dir1', help='File or directory with logfiles')
parser.add_argument('dir2', help='File or directory with logfiles')
args = parser.parse_args()

#print(args.q)
#print(args.dir1)
#print(args.dir2)

# Q1: line counts
if args.q[0] == "1":
	print "* Q1: line counts."
	# Map Reduce
	print "  + {}: {}".format(args.dir1,sc.textFile(args.dir1).map(lambda x: ("Row",1)).reduceByKey(lambda x,y: x+y).collect()[0][1])
	# RDD Properties
	print "  + {}: {}".format(args.dir2,sc.textFile(args.dir2).count()) 

# Q2: sessions of user 'achile'
elif args.q[0] == "2":
	def sessions_maper(log_str):
		if "Started Session" in log_str and "of user achille" in log_str:
			return ["achile",1]
		else:
			return ["not achile",1]
	print "*  Q2: sessions of user 'achile'."
	print "  + {}: {}".format(args.dir1,dict(sc.textFile(args.dir1).map(lambda x: sessions_maper(x)).reduceByKey(lambda x,y: x+y).collect()).get("achile"))
	print "  + {}: {}".format(args.dir2,dict(sc.textFile(args.dir2).map(lambda x: sessions_maper(x)).reduceByKey(lambda x,y: x+y).collect()).get("achile"))

# Q3: unique user names
elif args.q[0] == "3":
	def sessions_maper(log_str):
		if "Started Session" in log_str:
			return [str(log_str.split()[10].rsplit(".",1)[0]),1]
		else:
			return ["None",1]
	print "*  Q3: unique user names."
	print ("  + {}: {}".format(args.dir1,str(dict(sc.textFile(args.dir1).
		map(lambda x: sessions_maper(x)).
		reduceByKey(lambda x,y: x).
		filter(lambda x: x[0] != "None").
		collect()).
		keys())))
	print ("  + {}: {}".format(args.dir2,str(dict(sc.textFile(args.dir2).
		map(lambda x: sessions_maper(x)).
		reduceByKey(lambda x,y: 1).
		filter(lambda x: x[0] != "None").
		collect()).
		keys())))

# Q4: sessions per user
elif args.q[0] == "4":
	def sessions_maper(log_str):
		if "Started Session" in log_str:
			return [str(log_str.split()[10].rsplit(".",1)[0]),1]
		else:
			return ["None",1]
	print "*  Q4: sessions per user."
	print ("  + {}: {}".format(args.dir1,str(sc.textFile(args.dir1).
		map(lambda x: sessions_maper(x)).
		reduceByKey(lambda x,y: x+y).
		filter(lambda x: x[0] != "None").
		collect())))
	print ("  + {}: {}".format(args.dir2,str(sc.textFile(args.dir2).
		map(lambda x: sessions_maper(x)).
		reduceByKey(lambda x,y: x+y).
		filter(lambda x: x[0] != "None").
		collect())))

# Q5: number of errors
elif args.q[0] == "5":
	def sessions_maper(log_str):
		if "error" in log_str.lower():
			return ["error",1]
		else:
			return ["not error",1]
	print "*  Q5: number of errors."
	print "  + {}: {}".format(args.dir1,dict(sc.textFile(args.dir1).
		map(lambda x: sessions_maper(x)).
		reduceByKey(lambda x,y: x+y).
		collect()).
		get("error"))
	print "  + {}: {}".format(args.dir2,dict(sc.textFile(args.dir2).
		map(lambda x: sessions_maper(x)).
		reduceByKey(lambda x,y: x+y).
		collect()).
		get("error"))

# Q6: 5 most frequent error messages
elif args.q[0] == "6":
	def sessions_maper(separator,log_str):
		if "error" in log_str.lower():
			return [str(log_str.partition(separator)[2]) ,1]
			#return ["Nik",1]
		else:
			return ["None",1]
	print "*  Q6: 5 most frequent error messages."
	dir1_result = sc.textFile(args.dir1).\
		map(lambda x: sessions_maper(args.dir1,x)).\
		reduceByKey(lambda x,y: x+y).\
		filter(lambda x: x[0] != "None").\
		takeOrdered(5, key = lambda x: -x[1])

	dir1_result = [(cnt , message) for (message, cnt) in dir1_result]

	print "  + {}:".format(args.dir1)
	for item in dir1_result:
		print("    - {}".format(item))

	dir2_result = sc.textFile(args.dir2).\
		map(lambda x: sessions_maper(args.dir2,x)).\
		reduceByKey(lambda x,y: x+y).\
		filter(lambda x: x[0] != "None").\
		takeOrdered(5, key = lambda x: -x[1])

	dir2_result = [(cnt , message) for (message, cnt) in dir2_result]
	
	print "  + {}:".format(args.dir2)
	for item in dir2_result:
		print("    - {}".format(item))

# Q7: users who started a session on both hosts, i.e., on exactly 2 hosts.
elif args.q[0] == "7":
	def sessions_maper(log_str):
		if "Started Session" in log_str:
			return (str(log_str.split()[10].rsplit(".",1)[0]),str(log_str.split()[3]))
		else:
			return ("None",1)

	def session_maper2(x,y):
		return [x,list(set(y))]

	print "*  Q7: users who started a session on both hosts, i.e., on exactly 2 hosts."
	two_dir = args.dir1 + "," + args.dir2

	res = sc.textFile(two_dir).map(lambda x: sessions_maper(x))
	res = res.filter(lambda x: x[0] != "None" and x[1] != "localhost")
	res = res.groupByKey()
	res = res.map(lambda x : session_maper2(x[0],x[1]))
	print "  + : {} ".format(dict(res.filter(lambda x: len(x[1]) == 2).collect()).keys())

# Q8: users who started a session on exactly one host, with host name.
elif args.q[0] == "8":
	def sessions_maper(log_str):
		if "Started Session" in log_str:
			return (str(log_str.split()[10].rsplit(".",1)[0]),str(log_str.split()[3]))
		else:
			return ("None",1)

	def session_maper2(x,y):
		return (x,list(set(y)))

	print "*  Q8: users who started a session on exactly one host, with host name."
	two_dir = args.dir1 + "," + args.dir2

	res = sc.textFile(two_dir).map(lambda x: sessions_maper(x))
	res = res.filter(lambda x: x[0] != "None" and x[1] != "localhost")
	res = res.groupByKey()
	res = res.map(lambda x : (x[0],list(set(x[1]))))
	print "  + : {} ".format(res.filter(lambda x: len(x[1]) == 1).
		map(lambda x : (x[0],x[1][0])).collect())

# Q9: Anonymization
elif args.q[0] == "9":
	import os
	import re

	def sessions_maper(log_str):
		if "Started Session" in log_str:
			return [str(log_str.split()[10].rsplit(".",1)[0]),1]
		else:
			return [None,None]

	# Solution taken from http://stackoverflow.com/questions/2400504/easiest-way-to-replace-a-string-using-a-dictionary-of-replacements
	def sessions_maper_anonimizer(log_str, dictionary):
		pattern = re.compile(r'\b(' + '|'.join(dictionary.keys()) + r')\b')
		result = pattern.sub(lambda x: dictionary[x.group()], log_str)
		return result

	print "*  Q9: Anonymization."

	two_dir = args.dir1 + "," + args.dir2

	user_dir1 = sc.textFile(args.dir1).\
		map(lambda x: sessions_maper(x)).\
		filter(lambda x: x[0] != None).\
		reduceByKey(lambda x,y: x).\
		sortBy(lambda x: x[0]).\
		collect()

	user_dir2 = sc.textFile(args.dir2).\
		map(lambda x: sessions_maper(x)).\
		filter(lambda x: x[0] != None).\
		reduceByKey(lambda x,y: x).\
		sortBy(lambda x: x[0]).\
		collect()

	user_dir1_anonim = [] 
	user_dir2_anonim = [] 

	for counter in range(0,len(user_dir1)):
		user_dir1_anonim.append((user_dir1[counter][0],"user-{}".format(counter)))

	for counter in range(0,len(user_dir2)):
		user_dir2_anonim.append((user_dir2[counter][0],"user-{}".format(counter)))


	print ("  + {}:".format(args.dir1))
	print ("  . User name mapping:{}".format(user_dir1_anonim))

	if not os.path.exists(args.dir1+"-anonymized-10"):
		user_dir1_anonimized = sc.textFile(args.dir1).\
			map(lambda x: sessions_maper_anonimizer(x,dict(user_dir1_anonim)))
		user_dir1_anonimized.saveAsTextFile(args.dir1+"-anonymized-10")
		print("  . Anonymized files: {}-anonymized-10".format(args.dir1))
	else:
		print "  ! Directory {}-anonymized-10 already exists.".format(args.dir1)


	print ("  + {}:".format(args.dir2))
	print ("  . User name mapping:{}".format(user_dir2_anonim))

	if not os.path.exists(args.dir2+"-anonymized-10"):
		user_dir2_anonimized = sc.textFile(args.dir2).\
			map(lambda x: sessions_maper_anonimizer(x,dict(user_dir2_anonim)))
		user_dir1_anonimized.saveAsTextFile(args.dir2+"-anonymized-10")
		print("  . Anonymized files: {}-anonymized-10".format(args.dir2))
	else:
		print "  ! Directory {}-anonymized-10 already exists.".format(args.dir2)




