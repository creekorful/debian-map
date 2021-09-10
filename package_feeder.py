import itertools as it
import os
import sys
from email.parser import Parser

from arango import ArangoClient

if __name__ == '__main__':
    packages = []

    with open(sys.argv[1]) as f:
        for key, group in it.groupby(f, lambda line: line == '\n'):
            if not key:
                values = Parser().parsestr(''.join(list(group)))

                package = {
                    '_key': values['Package'],
                }

                for (k, v) in values.items():
                    if k in ['Depends', 'Pre-Depends', 'Tag']:
                        package[k] = v.replace('\n', '').split(', ')
                    else:
                        package[k] = v

                packages.append(package)

    # Push the packages into ArangoDB
    client = ArangoClient(hosts=os.environ.get('ARANGO_HOST'))
    db = client.db(os.environ.get('ARANGO_DB'),
                   username=os.environ.get('ARANGO_USER'),
                   password=os.environ.get('ARANGO_PASS'))

    col = db.collection('packages')
    col.insert_many(packages, overwrite=True)

    print("{} packages indexed.".format(len(packages)))

    # Push the relationships into ArangoDB
    # todo
