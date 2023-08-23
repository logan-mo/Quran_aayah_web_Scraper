from multiprocessing import Pool
import pandas as pd
import numpy as np
import requests
import os

def generate_dateset(imam):
    output_dir = 'data'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    output_dir = os.path.join(output_dir, imam)
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for surah_index in range(0, 113):
        for aayah_number in range(1, int(surah_df.iloc[surah_index]['Number of verses (Number of Rukūʿs)'] + 1)):
            file_name = f'{str(surah_index+1).zfill(3)}{str(aayah_number).zfill(3)}'
            url = f"https://everyayah.com/data/{imam}/{file_name}.mp3"

            out_file_path = os.path.join(output_dir, f'{surah_index+1}_{aayah_number}.mp3')
            if os.path.exists(out_file_path):
                continue
            doc = requests.get(url)

            if doc.status_code == 200:
                print(f'{surah_index+1}_{aayah_number}.mp3')
                with open(out_file_path, 'wb') as f:
                    f.write(doc.content)
            else:
                raise Exception(f'Error: {doc.status_code} for {url}')

surah_df = pd.read_csv('surah_aayah_count.csv').dropna()

imam_df = pd.read_csv('audiocats.csv')
imams = list(imam_df.values.reshape(1,-1)[0])

if __name__ == '__main__':
    with Pool(len(imams)) as p:
        print(p.map(generate_dateset, imams))

