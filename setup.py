from distutils.core import setup
import shutil

shutil.copyfile("README.md", "README")

setup(
    name = "Puuul",
    version = "0.1.0",
    description = "Thread pool and dependency graph infrastructure",
    author = "Lee Kamentsky",
    author_email = "leek@broadinstitute.org",
    url = 'http://github.com/LeeKamentsky/puuul',
    classifiers = """
    Programming Language :: Python
    Programming Language :: Python :: 3
    License :: OSI Approved :: MIT License
    Operating System :: OS Independent
    Development Status :: 4 - Beta
    Intended Audience :: Developers""",
    keywords = ["threading", "thread pool", "dependency"],
    packages = ["puuul"])