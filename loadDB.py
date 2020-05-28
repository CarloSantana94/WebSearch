import csv

from das import Neo4j

uri = "bolt://localhost:7687"
user = "neo4j"
password = "9932"

artist_location = "file:///artist.csv"
artist_credit_name_location = "file:///artist_credit_name.csv"
artist_credit_location = "file:///artist_credit.csv"
track_location = "file:///track.csv"
medium_location = "file:///medium.csv"
recording_location = "file:///recording.csv"
medium_format_location = "file:///medium_format.csv"
release_location = "file:///release.csv"
area_location = "file:///area.csv"
label_location = "file:///label.csv"
area_type_location = "file:///area_type.csv"
gender_location = "file:///gender.csv"

load_artist_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{artist_location}'" \
                     " AS line CREATE (a :Artist {id: line.id , name : line.name," \
                     "sort_name : line.sort_name, begin_date_year : line.begin_date_year," \
                     " begin_date_month : line.begin_date_month, begin_date_day : line.begin_date_day, " \
                     "end_date_year : line.end_date_year, end_date_month : line.end_date_month, end_date_day : " \
                     "line.end_date_day, artist_type : line.artist_type, area : line.area, gender : line.gender, " \
                     "ended : line.ended, begin_area : line.begin_area, end_area : line.end_area}) "

load_artist_credit_name_cypher = "USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM " \
                                 f"'{artist_credit_name_location}' AS line CREATE (acn:Artist_Credit_Name " \
                                 "{artist_credit:line.artist_credit , position : line.position, artist : " \
                                 "line.artist, name : line.name, join_phrase : line.join_phrase})"

load_artist_credit_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{artist_credit_location}'" \
                            " AS line CREATE (ac:Artist_Credit {id: line.id ," \
                            " name : line.name, artist_count : line.artist_count, ref_count : line.ref_count, " \
                            "created : line.created})"

load_track_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{track_location}' AS line CREATE (" \
                    "t:Track { id: line.id , recording : line.recording, medium : " \
                    "line.medium, position : line.position, number : line.number, " \
                    "name : line.name, artist_credit : line.artist_credit, length : " \
                    "line.length, is_data_track : line.is_data_track}) "

load_medium_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{medium_location}' AS line CREATE (" \
                     "m:Medium { id: line.id , release : line.release, position : line.position, format : " \
                     "line.format, name : line.name, track_count : line.track_count}) "

load_recording_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{recording_location}'" \
                        "AS line CREATE (r:Recording { id: line.id , name : line.name, artist_credit : " \
                        "line.artist_credit, length : line.length, comment : line.comment, " \
                        "video : line.video}) "

load_release_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{release_location}' AS line CREATE (" \
                      "r:Release { id: line.id, name : line.name, artist_credit : " \
                      "line.artist_credit, release_group : line.release_group, status : line.status, " \
                      "packaging : line.packaging, language : line.language, script : " \
                      "line.script, comment : line.comment, quality : line.quality}) "

load_medium_format_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{medium_format_location}' " \
                            "AS line CREATE (mf:Medium_Format { id: line.id , name : line.name, year : line.year, " \
                            " has_discids : line.has_discids, description : line.description}) "

load_area_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{area_location}' " \
                   "AS line CREATE (a:Area { id: line.id , name : line.name, type : line.type, " \
                   " begin_date_year : line.begin_date_year, begin_date_month : line.begin_date_month, " \
                   " begin_date_day : line.begin_date_day, end_date_year : line.end_date_year, " \
                   " end_date_month : line.end_date_month, end_date_day : line.end_date_day, " \
                   "ended : line.ended})"

load_gender_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{gender_location}' " \
                     "AS line CREATE (g:Gender { id: line.id , name : line.name, description : line.description})"

load_area_type_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{area_type_location}' " \
                        "AS line CREATE (at:Area_Type { id: line.id , name : line.name, description : " \
                        "line.description}) "

load_label_cypher = f"USING PERIODIC COMMIT 100000 LOAD CSV WITH HEADERS FROM '{label_location}'" \
                    " AS line CREATE (l:Label {id: line.id , name : line.name," \
                    " begin_date_year : line.begin_date_year," \
                    " begin_date_month : line.begin_date_month, begin_date_day : line.begin_date_day, " \
                    "end_date_year : line.end_date_year, end_date_month : line.end_date_month, end_date_day : " \
                    "line.end_date_day, type : line.type, area : line.area, comment : line.comment, " \
                    "ended : line.ended}) "

relationship_artist_credit_name = "CALL apoc.periodic.iterate(\"MATCH (a:Artist), (b:Artist_Credit_Name) " \
                                  "WHERE a.id = b.artist RETURN a,b \",\"CREATE (b)-[r:CREDITED]->(a)\", " \
                                  "{batchSize:10000, parallel:false})"

relationship_artist_credit = "CALL apoc.periodic.iterate(\"MATCH (a:Artist_Credit), (b:Artist_Credit_Name) " \
                             "WHERE a.id = b.artist_credit RETURN a,b \",\"CREATE (b)-[r:CREDITED_AS]->(a)\", " \
                             "{batchSize:10000, parallel:false})"

relationship_release_ac = "CALL apoc.periodic.iterate(\"MATCH (a:Artist_Credit), (b:Release) " \
                          "WHERE a.id = b.artist_credit RETURN a,b \",\"CREATE (b)-[r:REL_AC]->(a)\", " \
                          "{batchSize:10000, parallel:false})"

relationship_recording_ac = "CALL apoc.periodic.iterate(\"MATCH (a:Artist_Credit), (b:Recording) " \
                            "WHERE a.id = b.artist_credit RETURN a,b \",\"CREATE (b)-[r:REC_AC]->(a)\", " \
                            "{batchSize:10000, parallel:false})"

relationship_track_ac = "CALL apoc.periodic.iterate(\"MATCH (a:Artist_Credit), (b:Track) " \
                        "WHERE a.id = b.artist_credit RETURN a,b \",\"CREATE (b)-[r:AC_TRACK]->(a)\", " \
                        "{batchSize:10000, parallel:false})"

relationship_medium_release = "CALL apoc.periodic.iterate(\"MATCH (a:Release), (b:Medium) " \
                              "WHERE a.id = b.release RETURN a,b \",\"CREATE (b)-[r:REL_MED]->(a)\", " \
                              "{batchSize:10000, parallel:false})"

relationship_recording_track = "CALL apoc.periodic.iterate(\"MATCH (a:Recording), (b:Track) " \
                               "WHERE a.id = b.recording RETURN a,b \",\"CREATE (b)-[r:REC_TRACK]->(a)\", " \
                               "{batchSize:10000, parallel:false})"

relationship_medium_track = "CALL apoc.periodic.iterate(\"MATCH (a:Medium), (b:Track) " \
                            "WHERE a.id = b.medium RETURN a,b \",\"CREATE (b)-[r:MED_TRACK]->(a)\", " \
                            "{batchSize:10000, parallel:false})"

relationship_medium_format = "CALL apoc.periodic.iterate(\"MATCH (a:Medium_Format), (b:Medium) " \
                             "WHERE a.id = b.format RETURN a,b \",\"CREATE (b)-[r:MED_FOR]->(a)\", " \
                             "{batchSize:10000, parallel:false})"

relationship_artist_area = "CALL apoc.periodic.iterate(\"MATCH (a:Area), (b:Artist) " \
                           "WHERE a.id = b.area RETURN a,b \",\"CREATE (b)-[r:ART_A]->(a)\", " \
                           "{batchSize:10000, parallel:false})"

relationship_artist_gender = "CALL apoc.periodic.iterate(\"MATCH (a:Gender), (b:Artist) " \
                             "WHERE a.id = b.gender RETURN a,b \",\"CREATE (b)-[r:ART_GEN]->(a)\", " \
                             "{batchSize:10000, parallel:false})"

relationship_area_type = "CALL apoc.periodic.iterate(\"MATCH (a:Area_Type), (b:Area) " \
                         "WHERE a.id = b.type RETURN a,b \",\"CREATE (b)-[r:AREA_TYPE]->(a)\", " \
                         "{batchSize:10000, parallel:false})"

relationship_label_area = "CALL apoc.periodic.iterate(\"MATCH (a:Area), (b:Label) " \
                          "WHERE a.id = b.area RETURN a,b \",\"CREATE (b)-[r:ART_LAB]->(a)\", " \
                          "{batchSize:10000, parallel:false})"


class LoadDB:
    def __init__(self):
        self.session = Neo4j(uri, user, password)

    # import artist
    def load_artist(self):
        self.session.query(load_artist_cypher)
        print("Finished importing Artist (1/20)")

    # import artist_credit_name
    def load_artist_credit_name(self):
        self.session.query(load_artist_credit_name_cypher)
        print("Finished importing artist_credit_name (2/20)")

    # import artist_credit
    def load_artist_credit(self):
        self.session.query(load_artist_credit_cypher)
        print("Finished importing artist_credit (3/20)")

    # import track
    def load_track(self):
        self.session.query(load_track_cypher)
        print("Finished importing track (4/20)")

    # import medium
    def load_medium(self):
        self.session.query(load_medium_cypher)
        print("Finished importing medium (5/20)")

    # import recording
    def load_recording(self):
        self.session.query(load_recording_cypher)
        print("Finished importing recording (6/20)")

    # import release
    def load_release(self):
        self.session.query(load_release_cypher)
        print("Finished importing release (7/20)")

    # import medium_format
    def load_medium_format(self):
        self.session.query(load_medium_format_cypher)
        print("Finished importing medium_format (8/20)")

    # import area
    def load_area(self):
        self.session.query(load_area_cypher)
        print("Finished importing area (9/20)")

    # import gender
    def load_gender(self):
        self.session.query(load_gender_cypher)
        print("Finished importing gender (10/20)")

    # import area_type
    def load_area_type(self):
        self.session.query(load_area_type_cypher)
        print("Finished importing area (11/20)")

    # import label
    def load_label(self):
        self.session.query(load_label_cypher)
        print("Finished importing label (12/20)")

    # relationship artist -> artist_credit_name
    def relate_artist_credit_name(self):
        self.session.query(relationship_artist_credit_name)
        print("Finished relationship artist->credit_name (1/20)")

    # relationship credit_name->artist_credit
    def relate_artist_credit(self):
        self.session.query(relationship_artist_credit)
        print("Finished relationship credit_name->artist_credit (2/20)")

    # relationship release->artist_credit
    def relate_release_ac(self):
        self.session.query(relationship_release_ac)
        print("Finished relationship release->artist_credit (3/20)")

    # relationship recording->artist_credit
    def relate_recording_ac(self):
        self.session.query(relationship_recording_ac)
        print("Finished relationship recording->artist_credit (4/20)")

    # relationship medium->release
    def relate_medium_release(self):
        self.session.query(relationship_medium_release)
        print("Finished relationship medium->release (5/20)")

    # relationship track->recording
    def relate_recording_track(self):
        self.session.query(relationship_recording_track)
        print("Finished relationship track->recording (6/20)")

    # relationship medium->medium_format
    def relate_medium_format(self):
        self.session.query(relationship_medium_format)
        print("Finished relationship medium->medium_format (7/20)")

    # relationship artist->area
    def relate_artist_area(self):
        self.session.query(relationship_artist_area)
        print("Finished relationship artist->area (8/20)")

    # relationship artist->gender
    def relate_artist_gender(self):
        self.session.query(relationship_artist_gender)
        print("Finished relationship artist->gender (9/20)")

    # relationship area->area_type
    def relate_area_type(self):
        self.session.query(relationship_area_type)
        print("Finished relationship area->area_type (10/20)")

    # relationship label->area
    def relate_label_area(self):
        self.session.query(relationship_label_area)
        print("Finished relationship label->area (11/20)")

    # relationship track->ac
    def relate_track_ac(self):
        self.session.query(relationship_track_ac)
        print("Finished relationship track->ac (12/20)")

    # relationship track->medium
    def relate_medium_track(self):
        self.session.query(relationship_medium_track)
        print("Finished relationship track->medium (13/20)")

    def close_connection(self):
        self.session.close()

    @staticmethod
    def read_csv(csv_file_name, capture_fields):
        result = []
        with open(csv_file_name) as f:
            items = csv.reader(f, delimiter='\t')
            for item in items:
                yield result.append(item[field] for field in capture_fields)
