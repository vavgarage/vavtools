from setuptools import setup, find_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["numpy==1.21.2", "pandas==1.3.3", "boto3==1.24.66", "tqdm==4.62.3", "requests>=2"]

setup(
    name="vavtools",
    version="0.0.7",
    author="Vladislav Abramov",
    author_email="vavabramov@gmail.com",
    description="A package for easy work in Easycommerce DS team",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/vavgarage/vavtools",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"
    ],
)
