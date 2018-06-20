import configparser

from wanianki.importer import Importer, Exporter, Store


def run():
    config = configparser.ConfigParser()
    config.read('config.ini')

    key = config['app']['key']
    session_cookie = config['app']['session_cookie']

    store = Store()
    importer = Importer(store, key, session_cookie)
    importer.run()
    exporter = Exporter(store)
    exporter.run()


if __name__ == '__main__':
    run()
