class MapReduce:
    def __init__(self, lines):
        self.mappings = {}
        self.results = {}

        for line in lines:
            mapper = self.mapper(line)
            for key, val in mapper:
                if key not in self.mappings:
                    self.mappings[key] = []
                self.mappings[key].append(val)

        for key in self.mappings:
            reducer = self.reducer(key, self.mappings[key])
            self.results[reducer[0]] = reducer[1]

        for key in self.results:
            print key, self.results[key]

    def mapper(self, line):
        pass

    def reducer(self, key, entries):
        pass
