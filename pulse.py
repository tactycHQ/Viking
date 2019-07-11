import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from PIL import Image

df = pd.read_csv("..//Database//books_titles.csv",encoding='utf-8-sig')

idx = df[(df['7day'] < 10)].index
df = df.drop(idx)

idx = df[(df['cpd_avg'] < 0.5)].index
df = df.drop(idx)

cpd_mean = df['cpd_avg'].mean()
cpd_std= df['cpd_avg'].std()
_7day_mean = df['7day'].mean()
_7day_std= df['7day'].std()

df['std_cpd'] = df['cpd_avg'].apply(lambda x: (x-cpd_mean)/cpd_std)
df['std_7day'] = df['7day'].apply(lambda x: (x-_7day_mean)/_7day_std)
df['comp_pulse'] = 0.3*df['std_cpd']+0.7*df['std_7day']

comp_pulse_max = df['comp_pulse'].max()
comp_pulse_min= df['comp_pulse'].min()

theo_max = 0.3*(1-cpd_mean)/cpd_std+0.7*(300-_7day_mean)/_7day_std
theo_min = 0.3*(0.8-cpd_mean)/cpd_std+0.7*(0-_7day_mean)/_7day_std
df['pulse'] = df['comp_pulse'].apply(lambda x: (x-theo_min)/(theo_max-theo_min))

pulse = df[['title','release_date','7day','pulse','simple_adj']].copy()

pulse.to_csv("..//Database//pulse.csv",encoding='utf-8-sig')
print("File written")

