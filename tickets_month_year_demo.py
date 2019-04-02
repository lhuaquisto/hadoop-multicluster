from pyspark import SparkContext

def main():
   sc = SparkContext(appName='TicketsStateYear')
   #input_file = sc.textFile('/Tickets/Input/*')
   input_file = sc.textFile('/Tickets/Input/Parking_Violations_Demo.csv')
   #input_file = sc.textFile('/Dataset/Input/*')
   tickets = input_file.map(lambda x: x.split(','))
   
   #Tickets 2013
   tickets_2013 = tickets.filter(lambda x: '2013' in x[4])\
                   .map(lambda x: ( x[4][:2]+ '/'+ x[4][-4:]))\
				   .map(lambda x: (x, 1))\
				   .reduceByKey(lambda x, y: (x+y))\
                   .sortByKey()	
   #save tickets on file				   
   tickets_2013.repartition(1).saveAsTextFile('/Tickets3/tickets_2013')     
   
   #Tickets 2014
   tickets_2014 = tickets.filter(lambda x: '2014' in x[4])\
                   .map(lambda x: ( x[4][:2]+ '/'+ x[4][-4:]))\
				   .map(lambda x: (x, 1))\
				   .reduceByKey(lambda x, y: (x+y))\
                   .sortByKey()	
   #save tickets on file				   
   tickets_2014.repartition(1).saveAsTextFile('/Tickets3/tickets_2014')     
   
   #Tickets 2015
   tickets_2015 = tickets.filter(lambda x: '2015' in x[4])\
                   .map(lambda x: ( x[4][:2]+ '/'+ x[4][-4:]))\
				   .map(lambda x: (x, 1))\
				   .reduceByKey(lambda x, y: (x+y))\
                   .sortByKey()	
   #save tickets on file				   
   tickets_2015.repartition(1).saveAsTextFile('/Tickets3/tickets_2015')   
   
   #Tickets 2016
   tickets_2016 = tickets.filter(lambda x: '2016' in x[4])\
                   .map(lambda x: ( x[4][:2]+ '/'+ x[4][-4:]))\
				   .map(lambda x: (x, 1))\
				   .reduceByKey(lambda x, y: (x+y))\
                   .sortByKey()	
   #save tickets on file				   
   tickets_2016.repartition(1).saveAsTextFile('/Tickets3/tickets_2016')
   
   #Tickets 2017
   tickets_2017 = tickets.filter(lambda x: '2017' in x[4])\
                   .map(lambda x: ( x[4][:2]+ '/'+ x[4][-4:]))\
				   .map(lambda x: (x, 1))\
				   .reduceByKey(lambda x, y: (x+y))\
                   .sortByKey()	
   #save tickets on file				   
   tickets_2017.repartition(1).saveAsTextFile('/Tickets3/tickets_2017')
   
   #stop sparkcontext
   sc.stop()
   
if __name__ == '__main__':
   main() 
  