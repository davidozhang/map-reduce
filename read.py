def read(path):
    with open(path) as file:
        return [line.strip() for line in file]
