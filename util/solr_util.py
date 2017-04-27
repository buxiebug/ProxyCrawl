# encoding:utf8
__author__ = 'brianyang'

from solr import Solr


def add_item(solr_address, doc):
    s = Solr(solr_address)
    resp = s.add(doc, commit=True)


def clean(solr_address):
    s = Solr(solr_address)
    resp = s.select('*:*', fl='id', rows='50000')
    ids = []
    for result in resp.results:
        ids.append(result.get('id'))
    print ids
    s.delete_many(ids, commit=True)


if __name__ == '__main__':
    from config import bt_solr_address

    clean(bt_solr_address)
