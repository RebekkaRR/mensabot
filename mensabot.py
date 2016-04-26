import requests
import pandas as pd
from threading import Thread, Event
import re
import datetime as dt
from bs4 import BeautifulSoup
from copy import copy
import logging
from collections import namedtuple
from time import sleep
import sys
from IPython import embed

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.base import JobLookupError, ConflictingIdError

Message = namedtuple(
    'Result', ['chat_id', 'update_id', 'text', 'timestamp']
)

menus = {}

log = logging.getLogger('mensabot')
log.setLevel(logging.INFO)
formatter = logging.Formatter(
    fmt='%(asctime)s|%(levelname)s|%(name)s|%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('mensabot.log', encoding='utf-8')
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.addHandler(file_handler)

ingredients_re = re.compile('[(]([0-9]+,? ?)+[)] *')
WEEKDAYS = [
    'montag',
    'dienstag',
    'mittwoch',
    'donnerstag',
    'freitag',
]

DELTA_T = dt.timedelta(minutes=5)
URL = 'http://www.stwdo.de/gastronomie/speiseplaene/' \
      'hauptmensa/wochenansicht-hauptmensa'

BOT_URL = 'https://api.telegram.org/bot{token}'.format(token=sys.argv[1])



def replace_all(regex, string, repl):
    old = ''
    while old != string:
        old = string
        string = regex.sub(repl, string)

    return string


def get_date(soup, weekday):
    a = soup.find('a', {'href': '#' + weekday})
    date = dt.datetime.strptime(a.text.split()[1], '%d.%m.%Y')
    return dt.date(date.year, date.month, date.day)


def extract_daily_menu(soup, weekday):
    table_div = soup.find('div', {'id': weekday})

    table = table_div.find('table')
    table = parse_counter(table)
    menu, = pd.read_html(
        str(table),
    )
    menu.columns = ['gericht', 'beschreibung', 'counter']
    menu['gericht'] = menu.gericht.apply(
        lambda s: replace_all(ingredients_re, s, '')
    )
    menu.dropna(inplace=True)
    return menu


def parse_counter(table):
    table = copy(table)
    for a in table.select('a[data-tooltip]'):
        counter = BeautifulSoup(a.attrs['data-tooltip'], 'html.parser')
        a.replace_with(counter)

    return table


def fetch_menus():
    ret = requests.get(URL)
    soup = BeautifulSoup(
        ret.content.decode('utf-8'),
        'html.parser',
    )

    menus = {}
    for weekday in WEEKDAYS:
        try:
            date = get_date(soup, weekday)
            menus[date] = extract_daily_menu(soup, weekday)
        except:
            log.exception('Parsing error for {}'.format(weekday))

    return menus


def format_menu(date):
    global menus

    # saturday and sunday
    if date.weekday() >= 5:
        return 'Am Wochenende bleibt die Mensaküche kalt'

    if date not in menus:
        try:
            menus = fetch_menus()
        except:
            log.exception('Parsing Error')
            return 'Kein Menü gefunden'

    if date not in menus:
        return 'Nix gefunden für den {:%d.%m.%Y}'.format(date)

    text = ''
    menu = menus[date].query('counter != "Grillstation"')
    for row in menu.itertuples():
        text += '*{}*: {} \n'.format(row.counter, row.gericht)

    return text


def getUpdates():
    ret = requests.get(BOT_URL + '/getUpdates', timeout=5)
    ret = ret.json()

    if ret['ok']:
        messages = []
        for update in ret['result']:
            message_data = update['message']
            chatdata = message_data['chat']

            message = Message(
                update_id=update['update_id'],
                chat_id=chatdata['id'],
                text=message_data.get('text', ''),
                timestamp=dt.datetime.fromtimestamp(message_data['date'])
            )
            messages.append(message)
        return messages


def confirm_message(message):
    requests.get(
        BOT_URL + '/getUpdates',
        params={'offset': message.update_id + 1},
        timeout=5,
    )


def send_menu(chat_id):
    now = dt.datetime.now()
    date = dt.date.today()
    if now.hour >= 15:
        date += dt.timedelta(days=1)

    menu = format_menu(date)
    send_message(
        chat_id,
        menu,
    )
    log.info('Send menu for {:%Y-%m-%d} to {}'.format(
        date, chat_id
    ))


def send_message(chat_id, message):
    try:
        r = requests.post(
            BOT_URL + '/sendMessage',
            data={
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            },
            timeout=5,
        )
    except requests.exceptions.Timeout:
        log.exception('Telegram "send_message" timed out')
    return r


class MensaBot(Thread):

    def __init__(self):
        self.stop_event = Event()
        self.menu = {}
        self.scheduler = BackgroundScheduler(
            logger=log,
            jobstores={'sqlite': SQLAlchemyJobStore(url='sqlite:///clients.sqlite')},
	    misfire_grace_time=15*60,  # allow jobs to be send up to 15 Minutes after scheduled time
        )
        for job in self.scheduler.get_jobs():
            log.info('Active Job: {}'.format(job))
        self.scheduler.start()
        super().__init__()

    def run(self):
        while not self.stop_event.is_set():
            try:
                messages = getUpdates()
                self.handle_messages(messages)
                self.stop_event.wait(1)
            except:
                log.exception('Error in run()')
                self.stop_event.wait(30)

    def handle_messages(self, messages):
        for message in messages:
            if dt.datetime.now() - message.timestamp < DELTA_T:
                if message.text.startswith('/menu'):
                    send_menu(message.chat_id)

            if message.text.startswith('/start'):
                log.info('Received start message')
                try:
                    self.scheduler.add_job(
                        send_menu,
                        args=(message.chat_id, ),
                        trigger='cron',
                        day_of_week='mon-fri',
                        hour=11,
                        jobstore='sqlite',
                        id=str(message.chat_id),
                    )
                    send_message(
                        message.chat_id,
                        'Ihr bekommt ab jetzt pünktlich um 11:00 das Menü'
                    )
                except ConflictingIdError:
                    send_message(message.chat_id, 'Ihr bekommt das aktuelle Menü schon')

            elif message.text.startswith('/stop'):
                log.info('Received stop message')
                send_message(message.chat_id, 'Ihr bekommt nix mehr.')
                try:
                    self.scheduler.remove_job(
                        str(message.chat_id),
                    )
                except JobLookupError:
                    send_message(message.chat_id, 'Ihr habt /start nicht gesendet')

            confirm_message(message)

    def terminate(self):
        self.stop_event.set()


if __name__ == '__main__':
    bot = MensaBot()
    bot.start()
    log.info('bot running')
    try:
        while True:
            sleep(10)
    except (KeyboardInterrupt, SystemExit):
        bot.terminate()
