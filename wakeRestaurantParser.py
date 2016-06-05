import urllib2
import json
import sys
import codecs
import psycopg2


def data_parser(url, table_name, conn, cur):
    response = urllib2.urlopen(url)
    data = json.loads(response.read())
    # print json.dumps(data, indent = 4, sort_keys = True)  # see formatted JSON for full file
    column_keys = data['features'][0]['properties'].keys()
    column_names = ', '.join(column_keys)

    for feature in data['features']:
        # print json.dumps(feature, indent = 4, sort_keys = True)  # see formatted JSON under 'features' node
        properties = feature['properties']
        row = []
        for key in properties.keys():
            row.append(properties[key])
        data_loader(table_name, column_keys, column_names, row, conn, cur)


def data_loader(table_name, column_keys, column_names, row, conn, cur):
    place_holders = ', '.join(['%s'] * len(column_keys))
    insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' % (table_name, column_names, place_holders)
    # print (insert_sql, row)
    cur.execute(insert_sql, row)
    conn.commit()


def main():
    # encode output to utf-8
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout, 'strict')
    if sys.stderr.encoding != 'utf-8':
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr, 'strict')

    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(dbname="wakeRestaurantDB", user="postgres", host="localhost")
    except:
        print "Unable to connect to the database."
    cur = conn.cursor()

    print 'Begin parsing restaurant data.'
    cur.execute('DROP TABLE IF EXISTS restaurants')
    cur.execute(""" CREATE TABLE restaurants (phone_number VARCHAR(40)
                        ,city VARCHAR(100)
                        ,name VARCHAR(150)
                        ,objectid VARCHAR(40)
                        ,business_id VARCHAR(100)
                        ,longitude FLOAT
                        ,state VARCHAR(2)
                        ,postal_code INT
                        ,address VARCHAR(150)
                        ,latitude FLOAT
                        ,PRIMARY KEY (business_id))   """)
    data_parser('http://data.wake.opendata.arcgis.com/datasets/5c68797ce230422d92a9edf72193a04e_0.geojson',
                'restaurants', conn, cur)
    print 'Finished parsing restaurant data.'

    print 'Begin parsing violations data.'
    cur.execute('DROP TABLE IF EXISTS violations')
    cur.execute(""" CREATE TABLE violations (date_ DATE
                        ,code VARCHAR(40)
                        ,business_id VARCHAR(100)
                        ,objectid VARCHAR(40)
                        ,description VARCHAR(150)
                        ,PRIMARY KEY (business_id, objectid)) """)
    data_parser('http://data.wake.opendata.arcgis.com/datasets/c8ea67e7bd03466d9cc35ebb07f8eb8c_1.geojson',
                'violations', conn, cur)
    print 'Finished parsing violations data.'

    print 'Begin parsing inspections data.'
    cur.execute('DROP TABLE IF EXISTS inspections')
    cur.execute(""" CREATE TABLE inspections (description VARCHAR(150)
                        ,objectid VARCHAR(40)
                        ,business_id VARCHAR(100)
                        ,date_ DATE
                        ,score FLOAT
                        ,type VARCHAR(40)
                        ,PRIMARY KEY (business_id, objectid)) """)
    data_parser('http://data.wake.opendata.arcgis.com/datasets/1b08c4eb32f44a198277c418b71b3a48_2.geojson',
                'inspections', conn, cur)
    print 'Finished parsing inspections data.'

    # Disconnect from PostgreSQL database
    conn.commit()
    cur.close()
    conn.close()

    print 'Process complete.'

if __name__ == "__main__":
    main()

