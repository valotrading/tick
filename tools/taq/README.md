Taq
===

Taq produces TAQ files from OB files.


Development
-----------

Developing Taq requires [Python 2.7][] and [Virtualenv][].

  [Python 2.7]: http://python.org/
  [Virtualenv]: http://virtualenv.org/


1. Create a virtual environment:

        virtualenv env

2. Install NumPy to the virtual environment:

        env/bin/pip install -r requirements.txt

3. Install a development version of Taq to the virtual environment:

        env/bin/python setup.py develop

4. Run the development version of Taq from the virtual environment:

        env/bin/taq
