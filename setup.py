from setuptools import setup

setup(name='pyBaf2Sql',
      version='0.2.1',
      description='Python wrapper for Bruker Baf2Sql',
      url='https://github.com/gtluu/pyBaf2Sql',
      author='Gordon T. Luu',
      author_email='gtluu912@gmail.com',
      license='Apache License',
      packages=['pyBaf2Sql', 'Baf2Sql'],
      include_package_data=True,
      package_data={'': ['*.dll', '*.so']},
      install_requires=['numpy', 'pandas'])
