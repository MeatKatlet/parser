from multiprocessing import Pool
import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import mysql.connector
import re


#1.парсим объявления в одном городе вторичка
#для этого надо иметь ссылку-критерий поиска с параметрами

#получаем её содержимое
#получаем число страниц пагинации
#идем по всем страницам пагинации
#получаем список ссылок на страницы -карточки
#переход в каждую карточку, парсинг карточки
    #дата создания объявления
    #все характеристикми
    #цена
    #телефон продавца
    #цена/цены по ипотеке....


    #сохранение результата в бд Mysql


#получить список прокси
#для всех запросов делать подменю user-agent и сделать прокси ip
#задержка по рандому - sleep(....)
def get_page_url(page):
    url = 'https://nn.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat'
    url += "&p="+str(page)
    url += "&region=4733&room1=1&room2=1"
    return url

def get_html(url):
    response = requests.get(url)
    return response.text

def get_total_visible_pages(html):
    soup = BeautifulSoup(html, 'lxml')
    regex = re.compile('.*-list-.*')
    regex2 = re.compile('.*-list-item-.*')
    a = soup.find('div', id='frontend-serp').find('ul', {"class" : regex}).find_all('li', {"class" : regex2})[-1].contents[0]#getText()
    a_text = a.getText()
    page_raw = a.get('href').split("&")[4]
    page_pure = page_raw.split("=")[1]
    total_visible = int(page_raw)
    if a_text=="..":
        total_end = False
    else:
        total_end = True

    return [total_visible, total_end]

def get_all_links(html):
    soup = BeautifulSoup(html, 'lxml')
    tds = soup.find('table', id='currencies-all').find_all('td', class_='currency-name')
    links = []
    for td in tds:
        a = td.find('a', class_='currency-name-container').get('href')
        link = 'https://coinmarketcap.com' + a
        links.append(link)
    return links


def text_before_word(text, word):
    line = text.split(word)[0].strip()
    return line


def get_flat_card_data(html):
    soup = BeautifulSoup(html, 'lxml')
    try:
        name = text_before_word(soup.find('title').text, 'price')
    except:
        name = ''
    try:
        price = text_before_word(soup.find('div',class_='col-xs-6 col-sm-8 col-md-4 text-left').text, 'USD')
    except:
        price = ''
    data = {'name': name,'price': price}
    return data

def save_flat_card_data(data):
    pass


def write_csv(i, data):
    with open('coinmarketcap.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow((data['name'],data['price']))
        print(i, data['name'], 'parsed')



def main():
    start = datetime.now()
    url = 'https://nn.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&region=4733&room1=1&room2=1'

    total_end = False
    current_page = 0
    while total_end==False:
        total_visible, total_end = get_total_visible_pages(get_html(url))
        #total_visible #текущая видимая последняя страница в пагинации
        current_page +=1
        for page in range(current_page,total_visible,1):#начать с порследней посещенной +1, надо по страницу .. включительно!
            #переход по странице пагинации

            all_links = get_all_links(get_html(get_page_url(page)))
            for i, link in enumerate(all_links):
                #заходим в каждую карточку, собираем инфк о квартире

                data = get_flat_card_data(get_html(link))
                save_flat_card_data(data)

        current_page = current_page#последняя посещенная страница

    end = datetime.now()
    total = end - start
    print(str(total))
    #a = input()



if __name__ == '__main__':
    main()





