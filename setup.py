import os
import sys
from setuptools import setup, find_packages

directory = os.path.abspath(os.path.dirname(__file__))
if sys.version_info >= (3, 0):
    with open(os.path.join(directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
else:
    with open(os.path.join(directory, 'README.md')) as f:
        long_description = f.read()

if not os.path.exists("requirements.txt"):
    print("Make sure that the requirements.txt is located in the working directory.")
    sys.exit()
else:
    with open("requirements.txt", "r") as req:
        requires = []
        for line in req:
            requires.append(line.strip())

setup(name="Geo_419b",
      packages=find_packages(),
      include_package_data=True,
      description="A program for the automatic download of elevation data and orthophotos from the Thuringian Geoportal.",
      version="1.0.0",
      keywords="downloader, elevation data, orthophotos",
      python_requires=">=3.8.0",
      setup_requires=[""],
      install_requires=requires,
      extras_require={
          "docs": ["sphinx>=4.0"],
      },
      classifiers=[
          "Programming Language :: Python",
          "Operating System :: OS Independent",
          "Topic :: Utilities",
      ],
      url="https://github.com/Jon-Fr/Geo_419",
      author="Jonathan Frank",  # Fabian Schreiter und Jonathan Frank; Ist es möglich einen zweiten Autor hinzuzufügen?
      author_email="jonathan.frank@uni-jena.de",
      license="GPL-3",
      zip_safe=False,
      long_description=long_description,
      long_description_content_type="text/markdown")
