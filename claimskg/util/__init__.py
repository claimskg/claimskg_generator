import requests


class TypedCounter:
    def __init__(self):
        self.counts = dict()

    def count(self, key):
        if key not in self.counts.keys():
            self.counts[key] = 0
        self.counts[key] += 1
        return self.counts[key]


# class TagMeSpotter:
#     def __init__(self, tagme_server_location, token):
#         self.tagme_server_location = tagme_server_location
#         self.token = token
#
#     def spot(self, id):
#         response = requests.get(
#             "{tagme}/spot/{id}?gcube-token={token}".format(tagme=self.tagme_server_location, id=id, token=self.token))
#         return response
