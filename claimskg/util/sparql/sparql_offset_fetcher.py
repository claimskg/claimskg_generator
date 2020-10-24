import json

import redis
from SPARQLWrapper import JSON

r = redis.StrictRedis()


class SparQLOffsetFetcher:

    def __init__(self, sparql_wrapper, page_size, where_body, select_columns, prefixes=""):
        self.sparql_wrapper = sparql_wrapper
        self.page_size = page_size
        self.current_offset = 0
        self.where_body = where_body
        self.prefixes = prefixes
        self.select_columns = select_columns
        sparql_wrapper.setReturnFormat(JSON)
        self.count = -1
        self.__get_count__()

    def __get_count__(self):
        if self.count == -1:
            query = """{prefixes} SELECT count(distinct *) as ?count WHERE {{
                {where_body}
            }}
            """.format(where_body=self.where_body, prefixes=self.prefixes)
            result = self._fetch_from_cache_or_query(query)
            count = int(result['results']['bindings'][0]['count']["value"])
            self.count = count
            return count
        return self.count

    def next_page(self):
        if self.current_offset < self.count:
            query = """{prefixes} SELECT {select_columns} WHERE {{
                        {where_body}
                    }} LIMIT {page_size} OFFSET {offset}
                    """.format(select_columns=self.select_columns, where_body=self.where_body, page_size=self.page_size,
                               offset=self.current_offset, prefixes=self.prefixes)
            result = self._fetch_from_cache_or_query(query)
            self.current_offset += self.page_size
            return result['results']['bindings']
        return None

    def fetch_all(self):
        result = list()
        page = list()
        while page is not None:
            page = self.next_page()
            if page is not None:
                result.extend(page)
        return result

    def _fetch_from_cache_or_query(self, query):
        result = str()
        found = False
        cache_key = query
        # If redis was successfully initialized
        if r is not None:
            # Get cache value and check whether it exists
            val = r.get(cache_key)
            if val is not None:
                result = val
                found = True
        # If it doesn't exist, query annotator and cache the result
        if not found:
            self.sparql_wrapper.setQuery(query)
            result = self.sparql_wrapper.query().response.read()
            if len(result) == 0:
                result = ""
            r.set(cache_key, result)
        strres = str(result, 'utf-8')
        return json.loads(strres)
