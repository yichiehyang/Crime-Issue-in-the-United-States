import mrjob
from mrjob.job import MRJob
class crime_rate(MRJob):
    
    def mapper(self, _, line):
        data_list = line.split(',')
        county=data_list[0][1:]
        state=data_list[1][1:3]
        crime_rate=data_list[2]
        population = data_list[22]
        if population!="FIPS_ST":
            crime_pop = int(population)*float(crime_rate)/100000
            yield(state, (crime_pop,int(population)))
    def reducer(self,key,counts):
        count = list(counts)
        crime_pop=0
        population=0
        for c in count:
            crime_pop+=c[0]
            population+=c[1]
        result = crime_pop/population
        yield(key, result)

      
    
if __name__ == '__main__':
    crime_rate.run()
