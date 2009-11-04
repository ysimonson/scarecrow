from distutils.core import setup

setup(name = 'Scarecrow',
      version = '0.1',
      description = 'A MySQL-based key-value store with custom indexes',
      author = 'Yusuf Simonson',
      url = 'http://github.com/ysimonson/scarecrow',
      packages = ['scarecrow',],
      data_files = [
          ('', ['README', 'LICENSE']),
      ]
     )