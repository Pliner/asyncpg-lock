import re
from pathlib import Path

from setuptools import setup

install_requires = [
    "asyncpg>=0.30.0",
]


def read(*parts):
    return Path(__file__).resolve().parent.joinpath(*parts).read_text().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*\"([\d.abrc]+)\"")
    for line in read("asyncpg_lock", "__init__.py").splitlines():
        match = regexp.match(line)
        if match is not None:
            return match.group(1)
    else:
        raise RuntimeError("Cannot find version in asyncpg_lock/__init__.py")


long_description_parts = []
with open("README.md", "r") as fh:
    long_description_parts.append(fh.read())

with open("CHANGELOG.md", "r") as fh:
    long_description_parts.append(fh.read())

long_description = "\r\n".join(long_description_parts)


setup(
    name="asyncpg_lock",
    version=read_version(),
    description="Run long-running work exclusively using PostgreSQL advisory locks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    platforms=["macOS", "POSIX", "Windows"],
    author="Iurii Pliner",
    python_requires=">=3.11",
    project_urls={},
    url="https://github.com/Pliner/asyncpg-lock",
    author_email="yury.pliner@gmail.com",
    license="MIT",
    packages=["asyncpg_lock"],
    package_dir={"asyncpg_lock": "./asyncpg_lock"},
    package_data={"asyncpg_lock": ["py.typed"]},
    install_requires=install_requires,
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Operating System :: OS Independent",
        "Environment :: Web Environment",
        "Development Status :: 5 - Production/Stable",
        "Framework :: AsyncIO",
        "Typing :: Typed",
    ],
)
