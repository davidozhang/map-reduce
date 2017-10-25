import multiprocessing as mp

class MapReduce:
    def __init__(self, lines, output=False, mappings=None):
        self.manager = mp.Manager()
        self.mappings = {} if not mappings else mappings
        self.results = self.manager.dict()
        self.output = output

        for line in lines:
            mapper = self.mapper(line)
            if not mapper:
                break
            for key, val in mapper:
                if key not in self.mappings:
                    self.mappings[key] = []
                self.mappings[key].append(val)

        for key in self.mappings.keys():
            p = mp.Process(target=self.__generate_results, args=(key,))
            p.start()
            p.join()

        if self.output:
            for key in self.results.keys():
                print key, self.results[key]

    def __generate_results(self, key):
        reducer = self.reducer(key, self.mappings[key])
        if not reducer:
            return
        self.results[reducer[0]] = reducer[1]

    def mapper(self, line):
        return None

    def reducer(self, key, entries):
        return None
