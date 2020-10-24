from SPARQLWrapper import SPARQLWrapper
from pandas import DataFrame

from claimskg.util.sparql.sparql_offset_fetcher import SparQLOffsetFetcher

wrapper = SPARQLWrapper("http://localhost:8890/sparql/")

prefixes = """
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>
PREFIX thesoz: <http://lod.gesis.org/thesoz/>
PREFIX unesco: <http://vocabularies.unesco.org/thesaurus/>
PREFIX schema: <http://schema.org/>
PREFIX dct: <http://purl.org/dc/terms/>

"""

same_topic_query_body = """
?claim a schema:CreativeWork.
?claim schema:keywords ?keyword.
?keyword dct:about ?kwc.
?keyword2 dct:about ?kwc.

?claim2 schema:keywords ?keyword2. 

?claim schema:text ?text_r.
?claim2 schema:text ?text2_r.

?kwc skos:prefLabel ?kwcl_r.

FILTER (?keyword != ?keyword2)
FILTER (?claim != ?claim2)
FILTER (lang(?kwcl_r) = 'en')

BIND(str(?text_r) as ?text)
BIND(str(?text2_r) as ?text2)
BIND(str(?kwcl_r) as ?kwcl)
"""

same_sub_topic_query_body = """
?claim a schema:CreativeWork.
?claim schema:keywords ?keyword.
?keyword dct:about ?kwc.
?kwc skos:broader ?kwc_parent.
?keyword2 dct:about ?kwc_parent.

?claim2 schema:keywords ?keyword2. 

?claim schema:text ?text_r.
?claim2 schema:text ?text2_r.

?kwc skos:prefLabel ?kwcl_r.
?kwc_parent skos:prefLabel ?kwcl_parent_r.

FILTER (?keyword != ?keyword2)
FILTER (?claim != ?claim2)
FILTER (lang(?kwcl_r) = 'en')
FILTER (lang(?kwcl_parent_r) = 'en')

BIND(str(?text_r) as ?text)
BIND(str(?text2_r) as ?text2)
BIND(str(?kwcl_r) as ?kwcl)
BIND(str(?kwcl_parent_r) as ?kwcl_parent)
"""

# same_topic_dataframe = DataFrame()
# kwcl = []
# text = []
# text2 = []
# claim = []
# claim2 = []
# kwc = []
#
# print("Topic Query...")
# same_topic_fetcher = SparQLOffsetFetcher(wrapper, 10000, where_body=same_topic_query_body,
#                                          select_columns="distinct ?kwcl ?text ?text2 ?claim ?claim2 ?kwc",
#                                          prefixes=prefixes)
# same_topic_results = same_topic_fetcher.fetch_all()
#
# print("Building dataset...")
# for result in same_topic_results:
#     kwcl.append(result['kwcl']['value'])
#     text.append(result['text']['value'])
#     text2.append(result['text2']['value'])
#     claim.append(result['claim']['value'])
#     claim2.append(result['claim2']['value'])
#     kwc.append(result['kwc']['value'])
#
# same_topic_dataframe['kwcl'] = kwcl
# same_topic_dataframe['text'] = text
# same_topic_dataframe['text2'] = text2
# same_topic_dataframe['claim'] = claim
# same_topic_dataframe['claim2'] = claim2
# same_topic_dataframe['kwc'] = kwc
#
# same_topic_dataframe.to_csv("same_topic.csv")

same_subtopic_dataframe = DataFrame()
kwcl = []
kwcl_parent = []
text = []
text2 = []
claim = []
claim2 = []
kwc = []
kwc_parent = []

print("Sub-Topic Query...")
same_subtopic_fetcher = SparQLOffsetFetcher(wrapper, 10000, where_body=same_sub_topic_query_body,
                                            select_columns="distinct ?kwcl ?kwcl_parent ?text ?text2 ?claim ?claim2 ?kwc ?kwc_parent",
                                            prefixes=prefixes)
same_subtopic_results = same_subtopic_fetcher.fetch_all()

print("Building dataset...")
for result in same_subtopic_results:
    kwcl.append(result['kwcl']['value'])
    kwcl_parent.append(result['kwcl_parent']['value'])
    text.append(result['text']['value'])
    text2.append(result['text2']['value'])
    claim.append(result['claim']['value'])
    claim2.append(result['claim2']['value'])
    kwc.append(result['kwc']['value'])
    kwc_parent.append(result['kwc_parent']['value'])

same_subtopic_dataframe['kwcl'] = kwcl
same_subtopic_dataframe['kwcl_parent'] = kwcl_parent
same_subtopic_dataframe['text'] = text
same_subtopic_dataframe['text2'] = text2
same_subtopic_dataframe['claim'] = claim
same_subtopic_dataframe['claim2'] = claim2
same_subtopic_dataframe['kwc'] = kwc
same_subtopic_dataframe['kwc_parent'] = kwc_parent

same_subtopic_dataframe.to_csv("same_subtopic.csv")
