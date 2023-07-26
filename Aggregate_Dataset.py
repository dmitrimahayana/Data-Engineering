import pandas as pd
from pathlib import Path
from datetime import  datetime


def loadExtractData():
    folderPath = '/mnt/hgfs/00 My Project/Dataset/'
    df = pd.read_csv(folderPath + 'la0730.csv')
    new_df = df.groupby(['Zip code']).sum('Overall score').reset_index()
    return new_df

def ProcceedOutput():
    dateTime = datetime.now().strftime("%d%m%Y_%H%M%S")
    fileName = 'Output_'+dateTime+'.csv'
    filePath = Path('/home/dmahayana/AggregatedData/'+fileName)
    filePath.parent.mkdir(parents=True, exist_ok=True)
    newDf = loadExtractData()
    newDf.to_csv(filePath)
