from pymarc import MARCReader
import pandas as pd
import csv
import sys
import os

marcFilename = sys.argv[1]
if marcFilename.endswith("mrc") != True:
    print("Not a MARC file")
    sys.exit()
    

def getBottomXPercent(dataFrame,column,percent):

    '''takes a pandas dataframe and returns a filtered dataframe with the bottom X percent of records in a given column'''

    quartileFilter= dataFrame[column].quantile(percent/100)

    return dataFrame[dataFrame[column] < quartileFilter]




dfColumns = [
            "id",
            "ISBN",
            "Authors",
            "AlternativeTitles",
            "Edition",
            "Contributors",
            "Series",
            "TOC",
            "Date008",
            "Date26X",
            "Class",
            "LoC",
            "FAST",
            "Online",
            "LanguageOfResource",
            "CountryOfPublication",
            "languageOfCataloguing",
            "RDA",
            "ProviderNeutral",
            "DashInClass",
            "AllCaps",
            "OCLC",
            "total",
            "labels"
            ]

                
#main dataframe for record
df=pd.DataFrame(columns=dfColumns)


with open(marcFilename, 'rb') as fh:
 
        reader = MARCReader(fh)
        for record in reader:
            scoreDict = {
                "ISBN":0 ,
                "Authors":0 ,
                "AlternativeTitles":0 ,
                "Edition":0 ,
                "Contributors":0,
                "Series": 0,
                "TOC": 0,
                "Date008":0,
                "Date26X":0,
                "Class":0,
                "LoC":0,
                "FAST":0,
                "Online": 0,
                "LanguageOfResource":0,
                "CountryOfPublication":0,
                "languageOfCataloguing":0,
                "RDA":0,
                "ProviderNeutral":0,
                "DashInClass":0,
                "AllCaps":0,
                "OCLC":0,
                "total":0
                }


            print(record)
            

            #ISBN
            for f in record.get_fields('020'):
                scoreDict['ISBN'] += 1

            #Authors
            for f in record.get_fields('100','110','111'):
                scoreDict['Authors'] += 1

            #alternative titles
            for f in record.get_fields('246'):
                scoreDict['AlternativeTitles'] += 1

            #edition
            for f in record.get_fields('250'):
                scoreDict['Edition'] += 1

            #contributors
            for f in record.get_fields('700','710','711','720'):
                scoreDict['Contributors'] += 1

            #series
            for f in record.get_fields('440','490','800','810','830'):
                scoreDict['Series'] += 1

            #TOC
            TOC = record.get_fields('505')
            ContentsNote = record.get_fields('520')

            if len(TOC) > 0:
                scoreDict['TOC'] += 1
            if len(ContentsNote) > 0:
                scoreDict['TOC'] +=1
            

            #Online, LanguageOfResource,Date008
            for f in record.get_fields('008'):
                if f.value()[23] == "o":
                    scoreDict['Online'] += 1
                    
                if f.value()[35:38] in ['eng']:
                    scoreDict['LanguageOfResource'] += 1
                if f.value()[15:18] not in ['xx ']:
                    scoreDict['CountryOfPublication'] += 1
                date008 = f.value()[7:11]
                if date008[0] in ["1","2"] and len(date008) == 4:
                    scoreDict['Date008'] += 1
            
                
            for f in record.get_fields('300'):
                if f.value().find("online resource") != -1:
                    scoreDict['Online'] += 1
                   

            #date26X
            
            for f in record.get_fields('260','264'):
                for s in f:
                    date26X=f.get_subfields("c")
                    if len(date26X)>0:
                            cleanDate26X=""
                            for letter in date26X[0]:
                                if letter.isdigit():
                                    cleanDate26X +=  letter
                            if len(cleanDate26X) == 4:
                                if cleanDate26X[0] in ["1","2"]:
                                    scoreDict['Date26X'] += 1
                            if cleanDate26X == date008:
                                scoreDict['Date26X'] += 1
                            break

            #Class
            if len(record.get_fields('050','055','060','086','099')) > 0:
                scoreDict['Class'] += 1
            for f in record.get_fields('050','055','060','086','099'):
                if f.value().find("-") != -1:
                    scoreDict['DashInClass'] += -1
                

            #LoC, FAST
            for f in record.get_fields('600','610','611','630','650','651','653'):
                if f.indicators[1] == "0" and f.get_subfields("a")[0].find("Electronic books") == -1:
                    scoreDict['LoC'] += 1
                elif f.indicators[1] == "7" and f.get_subfields('2')[0] == "fast" and f.get_subfields("a")[0].find("Electronic books") == -1:
                    scoreDict['FAST'] += 1
            if scoreDict['LoC'] > 10:
                scoreDict['LoC'] = 10
            if scoreDict['FAST'] > 10:
                scoreDict['FAST'] = 10
          

            #LanguageOfCataloging, RDA
            for f in record.get_fields('040'):
                if len(f.get_subfields('b')) > 0:
        
                    if f.get_subfields('b')[0] in ['eng']:
                        scoreDict['languageOfCataloguing'] += 1
                if len(f.get_subfields('e')) > 0:
                    for s in f.get_subfields('e'):
                        if s == "rda":
                            scoreDict['RDA'] += 1
                        if s == "pn":
                            scoreDict['ProviderNeutral'] += -1
            
            #AllCaps
            for f in record.get_fields('100','245'):
                 if any(c.islower() for c in f.value()) is False:
                     scoreDict['AllCaps'] += -1

            #OCLC Number
            for f in record.get_fields('035'):
                if f.value().find("OCoLC") != -1:
                    scoreDict['OCLC'] = 1
                    break

            #calculate total score for record
            total = 0
            for key, value in scoreDict.items():
                total = total + value
            scoreDict['total'] = total
            
            #get id from marc 001 field
            scoreDict['id'] = record['001'].value()
            
            df=df.append(scoreDict,ignore_index=True)
            




standardDeviation = df['total'].std()



#Problematic Records
noLoCs = list((df.loc[df['LoC'] == 0].id).values)
allCaps = list((df.loc[df['AllCaps'] < 0].id).values)
bottomXPercent = list(getBottomXPercent(df,"total",5).id.values)



allProblems = set(noLoCs + allCaps + bottomXPercent)


averages = df.drop('id', axis=1).apply(lambda x: x.mean())
df=df.append(averages, ignore_index=True)
df=df.append({'total': standardDeviation,"labels":"Standard Deviation"},ignore_index=True)
df=df.append({'total': allProblems,"labels":"Potential Problem Records"},ignore_index=True)



csvFilename = os.path.splitext(marcFilename)[0] + ".csv"

df.to_csv(csvFilename, index=False)





print(allProblems)
print(standardDeviation)





                

           
                

                
                        




    
        
            
                
        
        

        
