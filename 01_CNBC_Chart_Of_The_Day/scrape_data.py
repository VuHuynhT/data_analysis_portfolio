from bs4 import BeautifulSoup
from fastparquet import write
import requests


def movies_gross_scrapper(url):
    # Specify the URL of the website you want to scrape
    url = url

    # Make a request to the website and get the HTML content
    response = requests.get(url)
    html_content = response.content

    # Create a Beautiful Soup object from the HTML content
    soup = BeautifulSoup(html_content, "html.parser")


    table_rows = soup.find_all("tr")

    movies_data = []

    for row in table_rows:
        td_tags = row.find_all('td')
        movie_data = []
        for td_tag in td_tags:
            movie_data.append(td_tag.text)
        
        movies_data.append(movie_data)  

    return movies_data




years  = [2021, 2019, 2017, 2016, 2015, 2014, 2010]
data_all = {}
for year in years:
    data = movies_gross_scrapper(f'https://www.boxofficemojo.com/year/world/{year}/')
    data_all[year] = data



columns = ['rank', 'title', 'worldwide_$', 'domestic_$', 'domestic_growth_td_%', 'foreign_$', 'foreign_growth_td_%']
columns_w_dtype = {'rank':'int32', 'title':'object', 'worldwide_$':'int32', 'domestic_$':'int32', 'domestic_growth_td_%':'object', 'foreign_$':'int32', 'foreign_growth_td_%':'object'}


frames = []

for key in data_all.keys():
    data = data_all[key]
    temp_df = pd.DataFrame(data, columns=columns, )
    temp_df['year'] = key
    frames.append(temp_df)

df = pd.concat(frames).reset_index(drop=True)

# drop rows where title is none, this was result from the first index of data contain nothing
df = df.drop(df[df['title'].isna()].index).reset_index(drop=True)


df['worldwide_$'] = df['worldwide_$'].str.replace('$', '').str.replace(',','').str.replace('-','0')
df['domestic_$'] = df['domestic_$'].str.replace('$', '').str.replace(',','').str.replace('-','0')
df['foreign_$'] = df['foreign_$'].str.replace('$', '').str.replace(',','').str.replace('-','0')


# change columns data type base on preset mapping
df = df.astype(columns_w_dtype)

write('movies_box_office.parq', df)