from lib2to3.fixer_util import p1

from acted_in import ActedIn
from das import Neo4j

uri = "bolt://localhost:7687"

userName = "neo4j"

password = "9932"

p1 = Neo4j(uri, userName, password)


# Metodo Auxiliar privado: Devolve o nome do artist_credit
def get_artist_name(artist_credit):
    cypher = "MATCH(r: Release) - [rel:REL_AC] -> (a: Artist_Credit{ id : \"" + artist_credit + "\"}) RETURN a.name"
    nodes = p1.query(cypher)
    return nodes


# Metodo Auxiliar privado: Devolve o id do artist_credit
def get_artist_credit(artist_name):
    query = "MATCH(t) - [rel: CREDITED]->(b:Artist{name: \"" + artist_name + "\"}) RETURN t.artist_credit"
    nodes = p1.query(query)
    return nodes


# Devolve as tracks de um artista
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


# Devolve as tracks que 2 artistas participaram NOTA: Nao esta funcional
# TODO: Generalizar para n artistas
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


# Devolve artistas com uma determinada idade
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


# Devolve os artistas com inicios e fins de areas iguais
def get_artist_same_area():
    cypher = "MATCH(a: Artist) WHERE a.begin_area <> \"\\\\N\" AND a.end_area <> \"\\\\N\" " \
             "AND a.begin_area = a.end_area RETURN a.name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])


# Devolve os artistas com inicios e fins de area distintos
def get_artist_diff_area():
    cypher = "MATCH(a: Artist) WHERE a.begin_area <> \"\\\\N\" AND a.end_area <> \"\\\\N\" " \
             "AND a.begin_area <> a.end_area RETURN a.name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])
    # return nodes


# Devolve os Alias dos artistas (50 Cent = Curtis Jackson)
def get_alias_artist(artist_name):
    cypher = "MATCH(a:Artist {name: \"" + artist_name + "\"}) - [rel: HAS_ALIAS2]->(b) RETURN b.sort_name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])
    # return nodes


# Devolve os artistas de uma area
def get_area_artists(area):
    cypher = "MATCH(b) - [rel: ART_A]-> (a:Area { name : \"" + area + "\"}) return b.name"
    nodes = p1.query(cypher)
    for node in nodes:
        print(node[0])
    # return nodes


# Devolve os eventos onde o artista atuou
def get_artist_event(artist_name):
    nodes2 = []
    cypher = "MATCH(a:L_Artist_Event) - [rel:ART_ARTEVE]->(b:Artist { name : \"" + artist_name + "\"}) " \
                                                                                                 "RETURN a.entity1"
    nodes = p1.query(cypher)
    for node in nodes:
        cypher2 = "MATCH(a:L_Artist_Event) - [rel:EVE_ARTEVE]-> (e:Event { id : \"" + node[0] + "\"}) RETURN e.name"
        nodes2.append(p1.query(cypher2))

    for node2 in nodes2:
        print(node2[0][0])


# get_artist_event("Elton John")

# Devolve os eventos que 2 artistas atuaram
# TODO: Generalizar para n artistas
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
        print(node2[0][0])


# Devolve os formatos de cada album
def get_medium_formats(rel_name):
    nodes2 = []
    nodes3 = []
    nodes4 = []
    cypher = "MATCH(r:Release) WHERE r.name = \"" + rel_name + "\" RETURN r.id, r.artist_credit"
    nodes = p1.query(cypher)

    for node in nodes:
        cypherr = []
        nodes4.append(get_artist_name(node[1]))
        cypher2 = "MATCH(m:Medium) - [rel:REL_MED] -> (r:Release{ id : \"" + node[0] + "\"}) WHERE m.format <> " \
                                                                                       "\"\\\\N\" RETURN m.format"
        cypherr = p1.query(cypher2)
        if len(cypherr) > 0:
            nodes2.append(cypherr)

    if len(nodes2) > 0:
        for node2 in nodes2:
            cypher3 = "MATCH(m:Medium) - [rel:MED_FOR] -> (mf:Medium_Format { id : \"" + node2[0][0] + "\"}) RETURN " \
                      "mf.name "
            nodes3.append(p1.query(cypher3))

        for (node3, n3) in zip(nodes3, nodes4):
            print(node3[0][0] + ", By: "+n3[0][0])
    else:
        print("Nao existe informacao de formatos desta release.")


def get_artists_that_acted_with(artist_name):
    nodes2 = []
    cypher = "MATCH(a:Artist_Credit_Name) WHERE a.name = \"" + artist_name + "\" RETURN a.artist_credit"
    nodes = p1.query(cypher)

    for node in nodes:
        cypher2 = "MATCH(acn:Artist_Credit_Name) - [rel:CREDITED_AS] -> (ac:Artist_Credit { id: \"" + node[0] + "\"})" \
                  "RETURN ac.name"
        nodes2.append(p1.query(cypher2))

    for node2 in nodes2:
        print(node2[0][0])


get_artists_that_acted_with("Dillaz")

p1.close()
