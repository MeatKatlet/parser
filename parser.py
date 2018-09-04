from multiprocessing import Pool
import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import mysql.connector
import re
from random import choice, uniform
from time import sleep
import json
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
#для всех запросов делать подмену user-agent и сделать прокси ip
#задержка по рандому - sleep(....)
def get_page_url(page):
    url = 'https://nn.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat'
    url += "&p="+str(page)
    url += "&region=4733&room1=1&room2=1"
    return url

def get_html(url,proxies,useragents):

    for i in range(len(proxies)):#по списку прокси!
        sleep(uniform(3, 6))

        proxy = {'https': 'https://' +choice(proxies)}#+ choice(proxies)
        useragent = {'User-Agent': choice(useragents)}
        try:
            response = requests.get(url, headers=useragent, proxies=proxy)
            break
        except:
            continue

    return response.text

def get_total_visible_pages(html):
    soup = BeautifulSoup(html, 'lxml')
    regex = re.compile('.*-list-.*')
    #regex2 = re.compile('.*-list-item-.*')
    a = soup.find('div', id='frontend-serp').find('ul', {"class" : regex}).contents[-1].contents[-1]
    a_text = a.text
    page_raw = a.get('href').split("&")[4]
    page_pure = page_raw.split("=")[1]
    total_visible = int(page_pure)
    if a_text=="..":
        total_end = False
    else:
        total_end = True

    return [total_visible, total_end]

def get_all_links(html):
    soup = BeautifulSoup(html, 'lxml')
    cont = soup.find('div', id='frontend-serp')
    regex = re.compile('.*-card-.*')
    regex2 = re.compile('.*-main-info-.*')
    regex3 = re.compile('.*-header-.*')

    all_cards_containers = cont.find_all('div',{"class" : regex})
    l = len(all_cards_containers.contents)
    card_urls = []

    for card_container_i in range(0,l):
        card_container = all_cards_containers.contents[card_container_i]
        sub_container = card_container.find("div",{"class" : regex2})
        a = sub_container.find("a",{"class" : regex3})
        href = a.get("href")
        card_urls.append(href)

    return card_urls

def text_before_word(text, word):
    line = text.split(word)[0].strip()
    return line


def get_flat_card_data(html):
    #todo доделать получение данных с карточки товара!
    soup = BeautifulSoup(html, 'lxml')
    card_cont = soup.find('div', id='frontend-offer-card')
    data = {}
    try:
        regex = re.compile('.*-header-information-.*')
        regex2 = re.compile('.*-title-.*')
        name = card_cont.find('div', {"class": regex}).find("h1",{"class": regex2}).text
        #заголовок объекта


    except:
        name = ''

    try:
        regex = re.compile('.*-header-information-.*')
        regex2 = re.compile('.*-geo-.*')
        regex3 = re.compile('.*-address-.*')
        adress_elems = card_cont.find('div', {"class": regex}).find("div", {"class": regex2}).find("adress", {"class": regex3}).contents
        l = len(adress_elems)
        result = []
        for i in range(0, l):
            if adress_elems[i].name == "a":
                adress_el = adress_elems[i].text
                result.append(adress_el)
        #географический адрес под заголовком

    except:
        price = ''

    try:
        #
        #цена, цена за кв м
        #todo процентная ставка за ипотеку?
        #
        regex = re.compile('.*-price-container-.*')
        regex2 = re.compile('.*-price_value-.*')
        regex3 = re.compile('.*-price_per_meter-.*')


        price = card_cont.find('div', {"class": regex}).find("span",{"class": regex2}).find("span",{"itemprop": "price"}).text

        val = price.replace("&nbsp;", "").replace(" ", "")[0:-1]
        currency = price.replace("&nbsp;", "").replace(" ", "")[-1]

        raw_ppsm = card_cont.find('div', {"class": regex}).find("div", {"class": regex3}).text
        ppsm = raw_ppsm.replace("&nbsp;", "").replace(" ", "")[0:-4]

    except:
        price = ''

    try:
        #общая, жилая, кухня


        props = card_cont.find('div', id='description').contents[0].contents
        #l = len(props)
        result = []
        for i in range(0, 3):#только 3 , 5 из 5 не берем
            prop_name = props[i].contents[0].text
            prop_raw_value = props[i].contents[1].text
            prop_value = prop_raw_value.replace(" ", "")[0:-2]#без метров ккв


        #
    except:
        price = ''

    try:
        #текстовое описание + код объекта
        description = card_cont.find('div', id='description').find("p",{"itemprop": "description"}).text

        #
    except:
        price = ''


    try:
        #общая информация - лоджия ...
        #
        regex = re.compile('.*-container-.*')

        elems = card_cont.find('ul', {"class": regex}).contents
        l = len(elems)
        result = []
        for i in range(0, l):
            result.append(elems[i].text)

    except:
        price = ''

    try:
        #может быть больше характеристик, надо доплнительные складывать в отдельное поле
        #Количество комнат

        #Этаж

        #Этажей в доме

        #Тип дома

        #Совмещённый санузел
        regex = re.compile('.*-container-.*')
        els = card_cont.find('ul', {"class": regex}).parent.find("article",{"class": regex}).contents[0].contents
        l = len(els)
        result = []
        for i in range(0, l):
            prop_name = els[i].contents[0]
            prop_val = els[i].contents[1]
        ##
    except:
        price = ''

    try:
        #о доме - все поля!
        #Год постройки
        #Материалы стен
        #Этажность
        #Подъездов
        #Квартир
        #
        regex = re.compile('.*-offer_card_page-bti-.*')
        elems = card_cont.find('div', {"class": regex}).contents[0].contents[0].contents[1].contents[0]# 1 колонка
        l = len(elems)
        result = []
        for i in range(0, l):
            prop_name = elems[i].contents[0]
            prop_val = elems[i].contents[1]

        elems = card_cont.find('div', {"class": regex}).contents[0].contents[0].contents[1].contents[1]# 2 колонка
        l = len(elems)
        result = []
        for i in range(0, l):
            prop_name = elems[i].contents[0]
            prop_val = elems[i].contents[1]


    except:
        price = ''

    try:
        # о районе  - все поля!

    except:
        price = ''
    try:
        # имя фамилия продавца
        #тип аккаунта - pro или нет
        #как давно на циане? 5 месяцев на ЦИАН
        #телефон

    except:
        price = ''

    try:
        #дата-время посещения продавцом карточки

    except:
        price = ''

    try:
        #дата создания объявления!!!!
        #количество просмотров за последние 10 дней

    except:
        price = ''

    try:
        #тип объявления - преимиум?


    except:
        price = ''

    try:
        #координаты на карте? координаты соседей?


    except:
        price = ''

    try:
        #есть ли фото, их количество


    except:
        price = ''

    data = {
        'name': name,
        'price': price
    }
    return data

def save_flat_card_data(data):
    #todo доделать сохранение данных в бд, какой она должна быть?
    #сохранять батчами?
    pass


def write_csv(i, data):
    with open('coinmarketcap.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow((data['name'],data['price']))
        print(i, data['name'], 'parsed')

def get_proxies2():
    url = "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list.txt"
    response = False
    for i in range(0,10):
        sleep(5)
        response = requests.get(url)
        if (response.status_code == 502):  # , response.text.find("502 Bad Gateway") > 0
            print("Bad Gateway... REPEAT")
            continue
        else:

            break
    if response != False:
        lines = response.text.split('\n')
        result = []
        for row in range(4,len(lines)):
            line = lines[row]
            if line == "":
                break
            parts = line.split(" ")
            proxie_type = parts[1].split("-")
            if len(proxie_type)==3 and proxie_type[2][0]=="S" and (proxie_type[1]=="A" or proxie_type[1]=="H"):

                result.append(parts[0])

        return result

def get_proxies():
    #url = "http:/http://pubproxy.com/api/proxy?https=true/api.foxtools.ru/v2/Proxy?cp=UTF-8&lang=Auto&type=HTTP&anonymity=High&available=Yes&free=Yes&formatting=1"
    url = "http://pubproxy.com/api/proxy?https=true"
    url = "https://gimmeproxy.com/api/getProxy?supportsHttps=true"

    result = []
    for i in range(0,10):
        while True:
            sleep(5)
            response = requests.get(url)
            if (response.status_code == 502):  # , response.text.find("502 Bad Gateway") > 0
                print("Bad Gateway... REPEAT")
                continue
            else:

                break
        #ip = json.loads(response.text)["data"][0]["ipPort"]
        data = json.loads(response.text)
        ip = data["ip"]+":"+str(data["port"])
        result.append(ip)

    if len(result)==0:
        exit(1)

    return result

def main():
    start = datetime.now()
    url = 'https://nn.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&p=1&region=4733&room1=1&room2=1'

    #proxies = get_proxies()
    proxies = get_proxies2()
    useragents = open('user_agents.txt').read().split('\n')
    #proxies = open('proxies.txt').read().split('\n')

    #url = 'https://whoer.net/ru'
    #u = get_html(url, proxies, useragents)

    total_end = False
    current_page = 0
    while total_end==False:
        total_visible, total_end = get_total_visible_pages(get_html(url,proxies,useragents))
        #total_visible #текущая видимая последняя страница в пагинации
        current_page +=1
        for page in range(current_page,total_visible,1):#начать с порследней посещенной +1, надо по страницу .. включительно!
            #переход по странице пагинации

            all_links = get_all_links(get_html(get_page_url(page),proxies,useragents))
            for i, link in enumerate(all_links):
                #заходим в каждую карточку, собираем инфк о квартире

                data = get_flat_card_data(get_html(link,proxies,useragents))
                save_flat_card_data(data)

        current_page = current_page#последняя посещенная страница

    end = datetime.now()
    total = end - start
    print(str(total))
    #a = input()



if __name__ == '__main__':
    main()





