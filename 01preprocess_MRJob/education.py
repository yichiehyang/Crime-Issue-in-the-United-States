import mrjob
from mrjob.job import MRJob
class education(MRJob):
    
    def mapper(self, _, line):
        data_list=line.split("\t")
        state=data_list[1]
        ELevel_pop=data_list[32]
        ELevel=data_list[36]
        if ELevel!="" and ELevel!='"Percent of adults with less than a high school diploma, 2000"'\
            and ELevel_pop!="" and ELevel_pop!='"Less than a high school diploma, 2000"':
            ELevel=float(data_list[36])
            if len(ELevel_pop)>3:
                ELevel_pop=data_list[32][1:-1]
                ELevel_pop = ELevel_pop.replace(",","")
            else:
                ELevel_pop=data_list[32]
            ELevel_pop = int(ELevel_pop)
            #population = ELevel_pop*100/ELevel
            yield(state, (ELevel_pop,ELevel))

    def reducer(self, key, counts):
        count = list(counts)
        
        ELevel_pop=0        
        pop=0
        for c in count:
            pop += ((c[0]*100)/c[1])
            ELevel_pop+=c[0]
        yield(key,ELevel_pop/pop)
        
        
"""    
    def combiner(self, key, counts):
        count = list(counts)
        ELevel_pop=0
        ELevel=0
        for c in count:
            ELevel_pop+=c[0]
            population+=c[1]
        
        yield(key,(ELevel_pop,population))
    def reducer(self,key,counts):
        count = list(counts)
        ELevel_pop=0
        population=0
        for c in count:
            ELevel_pop+=c[0]
            population+=c[1]
        result =ELevel_pop/population
        yield(key, result)
    
        #if ELevel_pop!="Less than a high school diploma, 2015-19":
         #   all_pop = int(ELevel_pop)*100/float(ELevel)
            #yield(state, (all_pop,int(ELevel_pop)))
        #yield (ELevel, ELevel_pop)
    
"""       
    
if __name__ == '__main__':
    education.run()
