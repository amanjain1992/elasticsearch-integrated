# -*- coding: utf-8 -*-
import os
import sys
from elasticsearch import Elasticsearch
from pyes import *

INDEX_NAME = 'javafiles'
INDEX_ALIAS = 'javafiles_alias'
TYPE_NAME = "code"


class IndexFiles(object):
    def __init__(self, root):
        conn = ES('10.4.233.32:9200', timeout=3.5)  # 连接ES
        try:
            conn.indices.delete_index(INDEX_NAME)
            # pass
        except:
            pass
        conn.indices.create_index(INDEX_NAME)  # 新建一个索引

        # 定义索引存储结构
        mapping = {u'content': {'boost': 1.0,
                                'index': 'analyzed',
                                'store': 'yes',
                                'type': u'string',
                                "indexAnalyzer": "ik",
                                "searchAnalyzer": "ik",
                                "term_vector": "with_positions_offsets"},
                   u'name': {'boost': 1.0,
                             'index': 'analyzed',
                             'store': 'yes',
                             'type': u'string',
                             "indexAnalyzer": "ik",
                             "searchAnalyzer": "ik",
                             "term_vector": "with_positions_offsets"},
                   u'dirpath': {'boost': 1.0,
                                'index': 'analyzed',
                                'store': 'yes',
                                'type': u'string',
                                "indexAnalyzer": "ik",
                                "searchAnalyzer": "ik",
                                "term_vector": "with_positions_offsets"},
                   }

        conn.indices.put_mapping(TYPE_NAME, {'properties': mapping}, [INDEX_NAME])  # 定义test-type

        self.addIndex(conn, root)
        conn.indices.add_alias(INDEX_ALIAS, INDEX_NAME)
        conn.default_indices = [INDEX_NAME]  # 设置默认的索引
        conn.indices.refresh()  # 刷新以获得最新插入的文档

    def addIndex(self, conn, root):
        print root
        for root, dirnames, filenames in os.walk(root):
            for filename in filenames:
                if not filename.endswith('.java'):
                    continue
                print "Indexing file ", filename
                try:
                    path = os.path.join(root, filename)
                    file = open(path)
                    contents = unicode(file.read(), 'utf-8')
                    file.close()
                    if len(contents) > 0:
                        conn.index({'name': filename, 'dirpath': root, 'content': contents}, INDEX_NAME, TYPE_NAME)
                    else:
                        print 'no contents in file %s', path
                except Exception, e:
                    print e


if __name__ == '__main__':
    IndexFiles('/Users/anduo/Projects')
