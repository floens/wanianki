import asyncio
import csv
import json
import re
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from os import path, makedirs
from time import time

import requests
import shutil
from bs4 import BeautifulSoup
from urllib.parse import quote

from multiprocessing.pool import Pool


class Store:
    def __init__(self):
        self.directory = 'imported/'

        makedirs(self.dir('pages'), exist_ok=True)
        makedirs(self.dir('radical_svgs'), exist_ok=True)
        makedirs(self.dir('audio'), exist_ok=True)

    def dir(self, subdir):
        return self.directory + subdir

    def isfile(self, filepath):
        return path.isfile(filepath)

    def store_json(self, filepath, data):
        with open(self.dir(filepath), 'w') as f:
            json.dump(data, f, indent=2)

    def load_json(self, filepath):
        with open(self.dir(filepath), 'r') as f:
            return json.load(f)

    def has_all_subjects(self):
        return self.isfile(self.dir('all_subjects.json'))

    def store_all_subjects(self, subjects):
        self.store_json('all_subjects.json', subjects)

    def get_all_subjects(self):
        return self.load_json('all_subjects.json')

    def store_lattice_list(self, name, urls):
        self.store_json(name + '_list.json', urls)

    def get_lattice_list(self, name):
        return self.load_json(name + '_list.json')

    def has_lattice_list(self, name):
        return self.isfile(self.dir(name + '_list.json'))

    def has_page(self, url):
        return self.isfile(self.dir('pages/' + url.replace('/', '_')))

    def store_page(self, page, url):
        with open(self.dir('pages/' + url.replace('/', '_')), 'w', encoding='utf-8') as f:
            f.write(page)

    def load_page(self, url):
        with open(self.dir('pages/' + url.replace('/', '_')), 'r', encoding='utf-8') as f:
            return f.read()

    def has_radical_image(self, slug):
        return self.isfile(self.dir('radical_svgs/' + slug + '.svg'))

    def store_radical_image(self, content, slug):
        with open(self.dir('radical_svgs/' + slug + '.svg'), 'w', encoding='utf-8') as f:
            f.write(content)

    def load_radical_image(self, slug):
        with open(self.dir('radical_svgs/' + slug + '.svg'), 'r', encoding='utf-8') as f:
            return f.read()

    def store_audio_list(self, audios):
        self.store_json('audio_list.json', audios)

    def load_audio_list(self):
        return self.load_json('audio_list.json')

    def has_audio_list(self):
        return self.isfile(self.dir('audio_list.json'))

    def has_audio(self, name):
        return self.isfile(self.dir('audio/' + name + '.mp3'))

    def get_audio_path(self, name):
        return self.dir('audio/' + name + '.mp3')

    def get_output_path(self):
        return self.dir('wanikani_export.csv')


class Exporter:
    def __init__(self, store):
        self.store = store

    def run(self):
        item_lists = [
            ('radical', self.store.get_lattice_list('radicals')),
            ('kanji', self.store.get_lattice_list('kanji')),
            ('vocab', self.store.get_lattice_list('vocabulary')),
        ]

        queue = []
        for type_and_list in item_lists:
            item_type, item_list = type_and_list
            for url in item_list:
                queue.append((self.store, item_type, url))

        results = []
        total = sum(len(i[1]) for i in item_lists)
        with Pool(cpu_count() * 2) as pool:
            for i, result in enumerate(pool.imap(self.extract_from_page, queue, 2)):
                dump_progress(i, total, 'Extracting data from pages')
                results.append(result)

        self.write_to_csv(results)

    def write_to_csv(self, results):
        # 'subject': subject,
        # 'level': level,
        # 'item_type': text_item_type,
        # 'part_of_speech': part_of_speech,
        # 'primary_meaning': primary_meaning,
        # 'additional_meanings': additional_meanings,
        # 'primary_reading': primary_reading,
        # 'additional_readings': additional_readings,
        # 'on_reading': on_reading,
        # 'on_muted': on_muted,
        # 'kun_reading': kun_reading,
        # 'kun_muted': kun_muted,
        # 'nanori_reading': nanori_reading,
        # 'nanori_muted': nanori_muted,
        # 'meaning': meaning,
        # 'reading': reading,
        # 'context_sentences': context_sentences,
        # 'audio_path': audio_path,
        # 'link': link

        def add_mute(value):
            return '<span class="reading-muted">' + value + '</span>'

        formatted_results = []

        # noinspection PyUnreachableCode
        if False:
            formatted_results.append((
                0, 'Sort field', 'Subject', 'Level', 'Item type', 'Primary meaning', 'Additional meanings',
                'Part of speech', 'Primary reading', 'Additional readings',
                'Onyomi reading', 'Kunyomi reading', 'Nanori reading',
                'Meaning mnemonic', 'Reading mnemonic', 'Context sentences', 'Audio', 'WaniKani link'
            ))

        for result in results:
            row = []

            order = 1_000_000 * result['level']
            item_type = result['item_type']
            if item_type == 'Kanji':
                order += 330_000
            elif item_type == 'Vocabulary':
                order += 660_000
            row.append(order)

            if item_type == 'Radical':
                sort_field_type = '1 r'
                sort_field_value = result['radical_slug']
            elif item_type == 'Kanji':
                sort_field_type = '2 k'
                sort_field_value = result['subject']
            elif item_type == 'Vocabulary':
                sort_field_type = '3 v'
                sort_field_value = result['subject']

            sort_field = '{0:02d} {1} {2}'.format(result['level'], sort_field_type, sort_field_value)

            row.append(sort_field)
            row.append(result['subject'])
            row.append(result['level'])
            row.append(result['item_type'])
            row.append(result['primary_meaning'])
            row.append(result['additional_meanings'])
            row.append(result['part_of_speech'])
            row.append(result['primary_reading'])
            row.append(result['additional_readings'])

            onyomi_reading = result['on_reading']
            if onyomi_reading and result['on_muted']:
                onyomi_reading = add_mute(onyomi_reading)
            row.append(onyomi_reading)

            kunyomi_reading = result['kun_reading']
            if kunyomi_reading and result['kun_muted']:
                kunyomi_reading = add_mute(kunyomi_reading)
            row.append(kunyomi_reading)

            nanori_reading = result['nanori_reading']
            if nanori_reading and result['nanori_muted']:
                nanori_reading = add_mute(nanori_reading)
            row.append(nanori_reading)

            row.append(result['meaning'])
            row.append(result['reading'])
            row.append(result['context_sentences'])
            audio = result['audio_path']
            if audio:
                row.append('[sound:{}]'.format(audio))
            else:
                row.append('')
            row.append(result['link'])

            formatted_results.append(row)

        formatted_results = sorted(formatted_results, key=lambda i: i[0])
        formatted_results = map(lambda i: i[1:], formatted_results)

        with open(self.store.get_output_path(), 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(formatted_results)

    # multiprocess-safe (does call a few store methods, but those are also multiprocess-safe).
    @staticmethod
    def extract_from_page(args):
        # extract data from the page, given the item type (radical, kanji or vocab)
        store, item_type, url = args

        html = store.load_page(url[1:])
        page = BeautifulSoup(html, 'html.parser')
        link = 'https://www.wanikani.com/' + url[1:]

        pf = '.container .row .span12 '

        # Level, subject, primary and additional meanings
        if item_type == 'radical':
            icon_type = 'radical'
        elif item_type == 'kanji':
            icon_type = 'kanji'
        elif item_type == 'vocab':
            icon_type = 'vocabulary'
        else:
            raise ValueError()

        level = int(page.select_one(pf + 'header h1 a.level-icon').text)

        radical_slug = ''
        if item_type == 'radical':
            radical_slug = link[len('https://www.wanikani.com/radicals/'):]
            radical_is_svg = store.has_radical_image(radical_slug)
            if radical_is_svg:
                subject = store.load_radical_image(radical_slug)
                subject = subject.replace('><defs', ' style="width: 1em; height: 1em;"><defs')
            else:
                subject = page.select_one(pf + 'header h1 .' + icon_type + '-icon').text
        else:
            subject = page.select_one(pf + 'header h1 .' + icon_type + '-icon').text

        primary_meaning = page.select_one(pf + 'header h1').contents[-1].strip()
        additional_meanings = ''
        if item_type == 'kanji' or item_type == 'vocab':
            additional_meanings = page.select_one(pf + 'section#information .alternative-meaning').contents[-2].text

        # Vocab only: part of speech and primary and additional readings
        part_of_speech = ''
        primary_reading = ''
        additional_readings = ''
        if item_type == 'vocab':
            part_of_speech = page.select_one(pf + 'section#information .part-of-speech p').text
            readings = page.select_one(pf + '.vocabulary-reading p').text.strip().split(', ')
            primary_reading = readings[0]
            additional_readings = ', '.join(readings[1:])

        # Kanji only: on, kun and nanori readings and a boolean if they're muted.
        on_reading = ''
        kun_reading = ''
        nanori_reading = ''
        on_muted = False
        kun_muted = False
        nanori_muted = False

        if item_type == 'kanji':
            readings = page.select(pf + 'section')[3]
            on_reading = readings.select('.span4')[0].select_one('p').text.strip()
            if on_reading == 'None':
                on_reading = ''
            on_muted = 'muted-content' in readings.select('.span4')[0]['class']

            kun_reading = readings.select('.span4')[1].select_one('p').text.strip()
            if kun_reading == 'None':
                kun_reading = ''
            kun_muted = 'muted-content' in readings.select('.span4')[1]['class']

            nanori_reading = readings.select('.span4')[2].select_one('p').text.strip()
            if nanori_reading == 'None':
                nanori_reading = ''
            nanori_muted = 'muted-content' in readings.select('.span4')[2]['class']

        # Meaning and reading mnemonic with html filtering
        def filter_mnemonic(el):
            def filter_spans(raw_el):
                for span in raw_el.select('span'):
                    if 'title' in span.attrs:
                        del span.attrs['title']
                    if 'rel' in span.attrs:
                        del span.attrs['rel']

            mnemonic_parts = []
            for p in el.find_all('p', recursive=False):
                filter_spans(p)
                mnemonic_parts.append(p.decode_contents())

            hint_aside = el.select_one('aside')
            if hint_aside:
                hint_parts = []
                for hint in hint_aside.select('p'):
                    filter_spans(hint)
                    hint_parts.append(hint.decode_contents())
                hint = '<br>\n<br>\n'.join(hint_parts)
                mnemonic_parts.append('Hints:<br>\n' + hint)

            return '<br>\n<br>\n'.join(mnemonic_parts)

        meaning = filter_mnemonic(page.select(pf + 'section')[2 if item_type == 'radical' else 4])
        reading = ''
        if item_type == 'kanji' or item_type == 'vocab':
            reading = filter_mnemonic(page.select(pf + 'section')[6])

        # Vocab only: context sentences
        context_sentences = ''
        if item_type == 'vocab':
            context_sentences_elements = page.select(pf + 'section.context-sentence .context-sentence-group')
            sentences = []
            for group in context_sentences_elements:
                sentences.append('<br>\n'.join([i.text.strip() for i in group.select('p')]))

            context_sentences = '<br>\n<br>\n'.join(sentences)

        # Vocab only: audio
        audio_path = ''
        if item_type == 'vocab':
            audio_path = 'wanikani_vocab_audio_' + quote(subject) + '.mp3'

        if item_type == 'radical':
            text_item_type = 'Radical'
        elif item_type == 'kanji':
            text_item_type = 'Kanji'
        elif item_type == 'vocab':
            text_item_type = 'Vocabulary'

        if not subject:
            raise Exception(('Missing subject', item_type, meaning, reading))

        # noinspection PyUnreachableCode
        if False:
            print('subject "{}"'.format(subject))
            print('level "{}"'.format(level))
            print('item type "{}"'.format(item_type))
            print('part of speech "{}"'.format(part_of_speech))
            print('primary meaning "{}"'.format(primary_meaning))
            print('additional meanings "{}"'.format(additional_meanings))
            print('primary reading "{}"'.format(primary_reading))
            print('additional readings "{}"'.format(additional_readings))
            print('on reading "{}"'.format(on_reading))
            print('on reading muted "{}"'.format(on_muted))
            print('kun reading "{}"'.format(kun_reading))
            print('kun reading muted "{}"'.format(kun_muted))
            print('nanori reading "{}"'.format(nanori_reading))
            print('nanori reading muted "{}"'.format(nanori_muted))
            print('meaning "{}"'.format(meaning))
            print('reading "{}"'.format(reading))
            print('context sentences "{}"'.format(context_sentences))
            print('audio path "{}"'.format(audio_path))
            print('link "{}"'.format(link))

        return {
            'subject': subject,
            'level': level,
            'item_type': text_item_type,
            'part_of_speech': part_of_speech,
            'primary_meaning': primary_meaning,
            'additional_meanings': additional_meanings,
            'primary_reading': primary_reading,
            'additional_readings': additional_readings,
            'on_reading': on_reading,
            'on_muted': on_muted,
            'kun_reading': kun_reading,
            'kun_muted': kun_muted,
            'nanori_reading': nanori_reading,
            'nanori_muted': nanori_muted,
            'meaning': meaning,
            'reading': reading,
            'context_sentences': context_sentences,
            'audio_path': audio_path,
            'radical_slug': radical_slug,
            'link': link
        }


class Importer:
    def __init__(self, store: Store, key: str, session_cookie: str):
        self.store = store
        self.key = key
        self.session_cookie = session_cookie

        self.root = 'https://api.wanikani.com/v2'

        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor()
        self.last_request_time = None
        self.rate_limiting_delay = 1.1

    def run(self):
        self.loop.run_until_complete(self.start())

    async def start(self):
        if not self.store.has_all_subjects():
            all_subjects = await self.request_paged('/subjects')
            self.store.store_all_subjects(all_subjects)

        if not self.store.has_lattice_list('radicals'):
            await self.get_lattice('https://www.wanikani.com/lattice/radicals/meaning', 'radicals')

        if not self.store.has_lattice_list('kanji'):
            await self.get_lattice('https://www.wanikani.com/lattice/kanji/combined', 'kanji')

        if not self.store.has_lattice_list('vocabulary'):
            await self.get_lattice('https://www.wanikani.com/lattice/vocabulary/combined', 'vocabulary')

        radical_urls = self.store.get_lattice_list('radicals')
        for i, radical_url in enumerate(radical_urls):
            dump_progress(i, len(radical_urls), 'Downloading radicals\'s')
            if not self.store.has_page(radical_url[1:]):
                page = await self.request_site('https://www.wanikani.com' + radical_url)
                self.store.store_page(page, radical_url[1:])
        print('')

        kanji_urls = self.store.get_lattice_list('kanji')
        for i, kanji_url in enumerate(kanji_urls):
            dump_progress(i, len(kanji_urls), 'Downloading kanji\'s')
            if not self.store.has_page(kanji_url[1:]):
                page = await self.request_site('https://www.wanikani.com' + kanji_url)
                self.store.store_page(page, kanji_url[1:])
        print('')

        vocab_urls = self.store.get_lattice_list('vocabulary')
        for i, vocab_url in enumerate(vocab_urls):
            dump_progress(i, len(vocab_urls), 'Downloading vocabulary')
            if not self.store.has_page(vocab_url[1:]):
                page = await self.request_site('https://www.wanikani.com' + vocab_url)
                self.store.store_page(page, vocab_url[1:])
        print('')

        await self.collect_image_radicals()
        await self.collect_audio()

    async def collect_audio(self):
        vocab_list = self.store.get_lattice_list('vocabulary')

        if not self.store.has_audio_list():
            audios = []
            for i, vocab in enumerate(vocab_list):
                dump_progress(i, len(vocab_list), 'Collecting audio urls')
                html = self.store.load_page(vocab[1:])
                page = BeautifulSoup(html, 'html.parser')
                mp3src = page.select_one('.vocabulary-reading audio source[type=audio/mpeg]')['src']
                subject_name = re.search(r'audio/\d+-([^.]+)', mp3src).group(1)
                subject = 'wanikani_vocab_audio_' + subject_name

                audios.append((subject, mp3src))

            self.store.store_audio_list(audios)

            print('')

        audio_list = self.store.load_audio_list()
        for i, audio in enumerate(audio_list):
            dump_progress(i, len(audio_list), 'Downloading mp3\'s')
            subject, mp3src = audio

            if not self.store.has_audio(subject):
                audio_path = self.store.get_audio_path(subject)
                await self.request_thing(ThingRequest.for_file(mp3src, audio_path))

        print('')

    async def collect_image_radicals(self):
        image_radicals = []

        subject_pages = self.store.get_all_subjects()
        for collection in subject_pages:
            for item in collection['data']:
                if item['object'] == 'radical':  # and not item['data']['characters']:
                    slug = item['data']['slug']
                    csssvg = list(filter(lambda j: j['metadata'].get('inline_styles'),
                                         item['data']['character_images']))[0]['url']
                    image_radicals.append((slug, csssvg))

        for i, image_radical in enumerate(image_radicals):
            dump_progress(i, len(image_radicals), 'Downloading radical svg\'s')
            slug, url = image_radical
            if not self.store.has_radical_image(slug):
                svg_content = await self.request_site(url)

                self.store.store_radical_image(svg_content, slug)
        print('')

    async def get_lattice(self, url, name):
        html = await self.request_site(url)
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.select_one('.lattice-multi-character' if name == 'vocabulary' else '.lattice-single-character')

        urls = []
        for link in container.select('li a'):
            urls.append(link.get('href'))
        self.store.store_lattice_list(name, urls)

    async def request_paged(self, endpoint, filters=None):
        pages = []

        result = await self.request(endpoint, filters)

        while True:
            pages.append(result)

            next_endpoint = None
            if 'pages' in result:
                pages_result = result['pages']
                if 'next_url' in pages_result and pages_result['next_url']:
                    next_endpoint = pages_result['next_url'][len(self.root):]

            if next_endpoint:
                result = await self.request(next_endpoint, filters)
            else:
                break

        return pages

    async def request(self, endpoint, filters=None):
        return await self.request_thing(ThingRequest.for_api(endpoint, filters))

    async def request_site(self, url):
        return await self.request_thing(ThingRequest.for_site(url))

    async def request_thing(self, api_request: 'ThingRequest'):
        skip_cooldown = api_request.type == ThingRequest.TYPE_FILE

        if not skip_cooldown and self.last_request_time is not None:
            now = time()
            if now < self.last_request_time + self.rate_limiting_delay:
                to_sleep = self.last_request_time + self.rate_limiting_delay - now
                # print('[rate limiting] sleep for {}s'.format(to_sleep))
                await asyncio.sleep(to_sleep)

        self.last_request_time = time()

        if api_request.type == ThingRequest.TYPE_API:
            endpoint = api_request.endpoint
            filters = api_request.filters
            if not filters:
                filters = {}

            def run():
                url = self.root + endpoint
                # print('[api] {}'.format(url))
                response = requests.get(url, headers={
                    'Authorization': 'Bearer {}'.format(self.key)
                }, params=filters)

                response_json = response.json()

                if 'error' in response_json:
                    print('[*** api error] {}'.format(response_json['error']))
                    return None

                return response_json
        elif api_request.type == ThingRequest.TYPE_SITE or api_request.type == ThingRequest.TYPE_FILE:
            site = api_request.type == ThingRequest.TYPE_SITE

            def run():
                url = api_request.url
                # print('[{}] {}'.format('page' if site else 'file', url))
                response = requests.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:60.0) Gecko/20100101 Firefox/60.0',
                    'Cookie': '_wanikani_session=' + self.session_cookie
                }, stream=not site)
                if site:
                    return response.text
                else:
                    with open(api_request.path, 'wb') as f:
                        shutil.copyfileobj(response.raw, f)
        else:
            raise ValueError()

        request_future = self.loop.run_in_executor(self.executor, run)
        return await request_future


class ThingRequest:
    TYPE_API = 0
    TYPE_SITE = 1
    TYPE_FILE = 2

    def __init__(self, thing_type):
        self.type = thing_type
        self.endpoint = None
        self.filters = None
        self.url = None
        self.path = None

    @classmethod
    def for_api(cls, endpoint, filters=None):
        t = cls(ThingRequest.TYPE_API)
        t.endpoint = endpoint
        t.filters = filters
        return t

    @classmethod
    def for_site(cls, url):
        t = cls(ThingRequest.TYPE_SITE)
        t.url = url
        return t

    @classmethod
    def for_file(cls, url, path):
        t = cls(ThingRequest.TYPE_FILE)
        t.url = url
        t.path = path
        return t


def dump(data, message=None):
    pretty = json.dumps(data, indent=2, sort_keys=True)
    if message:
        print('{}:\n{}'.format(message, pretty))
    else:
        print(pretty)


def dump_progress(current, total, subject=None):
    current += 1
    percentage = round(100 * (current / total), 2)
    to_print = '{}/{} {}%'.format(current, total, percentage)
    if subject:
        to_print = '[' + subject + '] ' + to_print
    print('\r' + to_print.ljust(22), end='')
