import docopt
import sys

from . import taq
from . import tsv


def start(args):
    '''
Usage: taq <input-file> <output-file>
    '''

    if not args:
        usage(start.__doc__)

    opts = docopt.docopt(start.__doc__, args)

    input_file  = opts['<input-file>']
    output_file = opts['<output-file>']

    indata  = tsv.read(input_file)
    outdata = taq.from_ob(indata)

    tsv.write(outdata, output_file)


def usage(message):
    print '%s\n' % message.strip()
    sys.exit(2)
