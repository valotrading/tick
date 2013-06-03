import setuptools

setuptools.setup(
    name             = 'taq',
    version          = '0.1.0',
    packages         = setuptools.find_packages(),
    install_requires = [
        'docopt==0.6.1',
        'pandas==0.11.0',
    ],
    entry_points     = {
        'console_scripts': [
            'taq = taq:main',
        ]
    },
)
