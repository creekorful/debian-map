import itertools as it
import os
import sys
from email.parser import Parser

from arango import ArangoClient

if __name__ == '__main__':
    packages = []
    sections = []
    ownerships = []

    with open(sys.argv[1]) as f:
        for key, group in it.groupby(f, lambda line: line == '\n'):
            if not key:
                values = Parser().parsestr(''.join(list(group)))

                package = {
                    '_key': values['Package'],
                }

                for (k, v) in values.items():
                    if k in ['Breaks', 'Depends', 'Pre-Depends', 'Provides', 'Recommends', 'Replaces', 'Suggests',
                             'Tag']:
                        package[k] = v.replace('\n', '').split(', ')
                    else:
                        package[k] = v
                packages.append(package)

                # Process the section
                section = {'_key': values['Section']}
                if section not in sections:
                    sections.append(section)

                # Process the ownerships
                ownerships.append({
                    '_key': 'package-{}-section'.format(package['_key']),
                    '_from': 'packages/{}'.format(package['_key']),
                    '_to': 'sections/{}'.format(section['_key'])
                })

                if 'Depends' in package:
                    for depend in package['Depends']:
                        # Remove version specifier and manage OR.
                        for depend in depend.split('(')[0].replace(' ', '').split('|'):
                            depend = depend.split(':')[0]  # In case we have :any specifier f.e
                            ownerships.append({
                                '_key': 'package-{}-depends-{}'.format(package['_key'], depend),
                                '_from': 'packages/{}'.format(package['_key']),
                                '_to': 'packages/{}'.format(depend)
                            })

    client = ArangoClient(hosts=os.environ.get('ARANGO_HOST'))
    db = client.db(os.environ.get('ARANGO_DB'),
                   username=os.environ.get('ARANGO_USER'),
                   password=os.environ.get('ARANGO_PASS'))

    # Push the packages into ArangoDB
    col = db.collection('packages')
    col.insert_many(packages, overwrite=True)

    # Push the sections into ArangoDB
    col = db.collection('sections')
    col.insert_many(sections, overwrite=True)

    # Push the ownerships into ArangoDB
    col = db.collection('ownerships')
    col.insert_many(ownerships, overwrite=True)

    print("{} packages indexed.".format(len(packages)))
    print("{} sections indexed.".format(len(sections)))
    print("{} ownerships computed.".format(len(ownerships)))
