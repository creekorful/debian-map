import os

import ldap
from arango import ArangoClient

if __name__ == '__main__':
    obj = ldap.initialize('ldap://db.debian.org')

    developers = []
    for (dn, res) in obj.search_s('ou=users,dc=debian,dc=org', ldap.SCOPE_SUBTREE, 'objectClass=debianAccount'):
        developer = {
            '_key': res['uid'][0].decode('utf-8'),
        }

        for (key, values) in res.items():
            developer[key] = list(map(lambda v: v.decode('utf-8'), values))

        developers.append(developer)

    # Push the data into ArangoDB
    client = ArangoClient(hosts=os.environ.get('ARANGO_HOST'))
    db = client.db(os.environ.get('ARANGO_DB'),
                   username=os.environ.get('ARANGO_USER'),
                   password=os.environ.get('ARANGO_PASS'))

    col = db.collection('developers')
    col.insert_many(developers, overwrite=True)

    print("{} developers indexed.".format(len(developers)))
