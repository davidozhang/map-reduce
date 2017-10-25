class MapReduce:
    def __init__(self, lines, output=False, mappings=None):
        self.mappings = {} if not mappings else mappings
        self.results = {}
        self.output = output

        for line in lines:
            mapper = self.mapper(line)
            if not mapper:
                break
            for key, val in mapper:
                if key not in self.mappings:
                    self.mappings[key] = []
                self.mappings[key].append(val)

        for key in self.mappings:
            reducer = self.reducer(key, self.mappings[key])
            if not reducer:
                break
            self.results[reducer[0]] = reducer[1]

        if self.output:
            for key in self.results:
                print key, self.results[key]

    def mapper(self, line):
        return None

    def reducer(self, key, entries):
        return None
