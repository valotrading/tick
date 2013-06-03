import pandas


def read(filename):
    return pandas.read_csv(filename, sep='\t')


def write(table, filename):
    with open(filename, 'w') as outfile:
        table.to_csv(outfile, index=False, sep='\t')
