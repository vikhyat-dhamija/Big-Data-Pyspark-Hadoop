from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
from operator import itemgetter
import re

#WORD_RE = re.compile(r"[\w']+")


class MRmatmult(MRJob):

    INTERNAL_PROTOCOL = JSONProtocol
    

    def mapper(self, _, line):
        
        (mat , row , col , value )=line.split(" ")
        
        if(mat == 'A'):
            for i in range(0,100):
               key= row +" "+ str(i)
               values={'rc' : col , 'value' : value  }
               yield(key,values)
        else:
            for i in range(0,100):
               key= str(i)+" "+col
               values={'rc' : row , 'value' : value  }
               yield(key,values)  
        
        
        
    
    def reducer(self, key , value_list):
        
            value_lists=list(value_list)
            value_lists = sorted(value_lists,key=lambda i: i['rc'])
            i = 0
            result = 0
            while i < len(value_lists) - 1:
                if int(value_lists[i]['rc']) == int(value_lists[i + 1]['rc']):
                    result += int(value_lists[i]['value'])*int(value_lists[i + 1]['value'])
                    i += 2
                else:
                    i += 1

            yield(key,result)
            
          

if __name__ == '__main__':
    MRmatmult.run()