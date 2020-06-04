from lib2to3.fixer_util import p1

from acted_in import ActedIn
from das import Neo4j

uri = "bolt://localhost:7687"

userName = "neo4j"

password = "9932"

p1 = Neo4j(uri, userName, password)


def get_artist_credit(artist_name):
    query = "MATCH(t) - [rel: CREDITED]->(b:Artist{name: \"" + artist_name + "\"}) RETURN t.artist_credit"
    nodes = p1.query(query)
    return nodes


def get_tracks(artist_name):
    r = []
    ola = []
    nodes = get_artist_credit(artist_name)

    for node in nodes:
        query5 = "MATCH(t) - [rel: AC_TRACK]->(b:Artist_Credit{id:\"" + node[0] + "\"}) RETURN t.name as name"
        r.append(p1.query(query5))

    for node in r:
        ola.append(node)

    for node in ola:
        for n in node:
            print(n[0])


def get_tracks_equals(artist_name, artist_name2):
    r = []
    r2 = []
    ola = []
    ola2 = []
    nodes = get_artist_credit(artist_name)
    nodes2 = get_artist_credit(artist_name2)

    for node in nodes:
        query5 = "MATCH(t) - [rel: AC_TRACK]->(b:Artist_Credit{id:\"" + node[0] + "\"}) RETURN t.name as name"
        r.append(p1.query(query5))

    for node in nodes2:
        query5 = "MATCH(t) - [rel: AC_TRACK]->(b:Artist_Credit{id:\"" + node[0] + "\"}) RETURN t.name as name"
        r2.append(p1.query(query5))

    for node in r:
        ola.append(node)

    for node in r2:
        ola2.append(node)

    c = []
    for node in ola:
        for n in node:
            for node2 in ola2:
                for n2 in node2:
                    if n[0] == n2[0]:
                        print(n[0])


def get_artists_with_age(nr):
    x = str(nr)
    y = str(nr + 1)
    query = f"MATCH(n: Artist) WHERE ((toInteger(n.end_date_year) - toInteger(n.begin_date_year)) = {x} AND " \
            "toInteger(n.artist_type) = 1  AND (toInteger(n.end_date_month) - toInteger(n.begin_date_month)) > 0) OR " \
            f"((toInteger(n.end_date_year) - toInteger(n.begin_date_year)) = {y} AND " \
            "toInteger(n.artist_type) = 1  AND (toInteger(n.end_date_month) - toInteger(n.begin_date_month)) < 0) " \
            "RETURN n.name"
    nodes = p1.query(query)
    for node in nodes:
        print(node[0])


def get_artist_same_area():
    cypher = "MATCH(a: Artist) WHERE a.begin_area <> \"\\\\N\" AND a.end_area <> \"\\\\N\" " \
             "AND a.begin_area = a.end_area RETURN a.name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])


def get_artist_diff_area():
    cypher = "MATCH(a: Artist) WHERE a.begin_area <> \"\\\\N\" AND a.end_area <> \"\\\\N\" " \
             "AND a.begin_area <> a.end_area RETURN a.name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])


def get_alias_artist(artist_name):
    cypher = "MATCH(a:Artist {name: \"" + artist_name + "\"}) - [rel: HAS_ALIAS2]->(b) RETURN b.sort_name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])


def get_area_artists(area):
    cypher = "MATCH(b) - [rel: ART_A]-> (a:Area { name : \"" + area + "\"}) return b.name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])


def get_artist_event(artist_name):
    nodes2 = []
    cypher = "MATCH(a:L_Artist_Event) - [rel:ART_ARTEVE]->(b:Artist { name : \"" + artist_name + "\"}) " \
             "RETURN a.entity1"
    nodes = p1.query(cypher)
    for node in nodes:
        cypher2 = "MATCH(a:L_Artist_Event) - [rel:EVE_ARTEVE]-> (e:Event { id : \"" + node[0] + "\"}) RETURN e.name"
        nodes2.append(p1.query(cypher2))

    for node2 in nodes2:
        for n in node2[0]:
            print(n)

# cypher = "MATCH(a:Artist) WHERE a.name IN [ \""+artist_name+"\", \""+artist_name2+"\"]" \
    #          "MATCH(a) <- [rel:ART_ARTEVE] -(b:L_Artist_Event) RETURN b.entity1"
    #
    # nodes = p1.query(cypher)


def get_artists_event(artist_name, artist_name2):
    nodes2 = []
    cypher = "MATCH(a:L_Artist_Event) - [rel:ART_ARTEVE]->(b:Artist { name : \"" + artist_name + "\"}) " \
             ",(c:L_Artist_Event) - [rel2:ART_ARTEVE]->(d:Artist { name : \"" + artist_name2 + "\"}) " \
             "WHERE a.entity1=c.entity1 RETURN a.entity1"
    nodes = p1.query(cypher)
    for node in nodes:
        cypher2 = "MATCH(a:L_Artist_Event) - [rel:EVE_ARTEVE]-> (e:Event { id : \"" + node[0] + "\"}) RETURN e.name"
        nodes2.append(p1.query(cypher2))

    for node2 in nodes2:
        for n in node2[0]:
            print(n)


get_artists_event("Korn", "Nirvana")

    # get_area_artists("Canada")
# get_alias_artist("50 Cent")
# get_artist_same_area()
# get_tracks_equals("Korn", "Little Mix")

# get_artist_event("Amy Winehouse")

# query1 = "MATCH(n: Artist) WHERE(toInteger(n.end_date_year) - toInteger(n.begin_date_year)) = 27 RETURN n.name"
#
# query2 = "MATCH(t: Track) MATCH(n: Track) WHERE(toInteger(t.length) = toInteger(n.length) ) return t.name, n.name"
#
# query3 = "MATCH(t:Track) WHERE (toInteger(t.length) = 3 ) return t.name"
#
# query4 = "MATCH(t) - [rel: CREDITED]->(b:Artist{name: \"Queen\"}) RETURN t.artist_credit"
#
# query6 = "MATCH(a:Artist) WHERE (toInteger(a.begin_date_day) = 6 AND toInteger(a.begin_date_month) = 3) " \
#          "return a.name "
#
# query7 = "MATCH(t:Track) WHERE "

# nodes = p1.query(query4)
# for node in nodes:
#     print(node[0])

# primeira query -> devolve os numeros de credito
# nodes = p1.query(query4)
# print(nodes)
# r = []
# # segunda query -> devolve
# for node in nodes:
#     query5 = "MATCH(t) - [rel: CREDITED_AS]->(b:Artist_Credit{id:\"" + node[0] + "\"}) RETURN t.name as name"
#     r.append(p1.query(query5))
#
# ola = []
# for node in r:
#     ola.append(node)
#     print(ola)
#
# for node in ola:
#     for n in node:
#         print(n[0])
#
# print(node[0])
# if nodes is None:
#     print("NO OCCURRENCES")
#
# else:
#     # print("OCCURENCES " + str(len(list(nodes))))
# for node2 in nodes2:
#   print(node2)


p1.close()
