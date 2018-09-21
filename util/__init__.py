class TypedCounter:
    def __init__(self):
        self.counts = dict()

    def count(self, key):
        if key not in self.counts.keys():
            self.counts[key] = 0
        self.counts[key] += 1
        return self.counts[key]
