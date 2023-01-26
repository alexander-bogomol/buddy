from bs4 import BeautifulSoup
import requests
import pandas as pd
from multiprocessing import Pool, Manager

# All movies list
url_all_films = 'https://www.spoilertime.ru/index.php/arkhiv'
req_all_films = requests.get(url_all_films)
soup_all_films = BeautifulSoup(req_all_films.text, 'html.parser')
all_films_titles = soup_all_films.find_all('h3', {"class": "page-header item-title"})
print('There are', len(all_films_titles), 'titles')

# Create full url for each movie
links = []
for movie_title in all_films_titles:
    links.append('https://www.spoilertime.ru' + movie_title.a['href'])
print('There are', len(links), 'links')

exceptions_list = []  # list of possible errors
title2plot_dict = Manager().dict({})  # create proxy-dict for pools


def plot_scraping(url):
    try:
        req = requests.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
    except Exception:
        return f'cannot request this: {url}!'
    
    try:
        title = (soup.section
                     .ul
                     .find_all('li', {"class": "active"}, itemprop="itemListElement")[-1]
                     .get_text()
                     .strip())

        short = (soup.find('div', {"style": "overflow: hidden;padding-bottom: 20px;"})
                     .get_text()
                     .strip())
    
        plot = (soup.find_all('div', {"class": "leading-1"})[0]
                    .get_text()
                    .strip())
    except Exception:
        return f'cannot parse this url: {url}'
    
    if len(title) != 0 and len(short + plot) > 70:
        title2plot_dict[title] = short + ' \n' + plot
        return
    else:
        return f'cannot get title or plot from: {url}'

if __name__ == '__main__':
    with Pool(11) as p:
        exceptions_list.append(p.map(plot_scraping, links))

print('Scrapping done! There are', len(dict(title2plot_dict)), 'scrapped movies')

# Create dataframe and write it to csv
df = pd.DataFrame(columns=['title', 'plot'], data=title2plot_dict.items())
df.to_csv('../dataset_rus/title2plot_py.csv', encoding='utf-8', index=False)
print('Dataframe created!')
