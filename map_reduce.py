import multiprocessing as mp

# This base class supports reducer multiprocessing through constructor flag.
# The flag can be set either using default args or through subclass explicitly enabling it.
class MapReduce:
    def __init__(self, lines, output=False, mappings=None, multiprocessing=False):
        self.manager = mp.Manager()
        self.mappings = {} if not mappings else mappings
        self.results = self.manager.dict()  # Thread-safe dictionary
        self.output = output
        self.multiprocessing = multiprocessing

        for line in lines:
            mapper = self.mapper(line)
            if not mapper:
                break
            for key, val in mapper:
                if key not in self.mappings:
                    self.mappings[key] = []
                self.mappings[key].append(val)

        for key in self.mappings.keys():
            if self.multiprocessing:
                p = mp.Process(target=self.__generate_results, args=(key,))
                p.start()
                p.join()
            else:
                self.__generate_results(key)

        if self.output:
            for key in self.results.keys():
                print key, self.results[key]

    def __generate_results(self, key):
        reducer = self.reducer(key, self.mappings[key])
        if not reducer:
            return
        self.results[reducer[0]] = reducer[1]

    # Overridden by subclass
    def mapper(self, line):
        return None

    # Overriden by subclass
    def reducer(self, key, entries):
        return None
