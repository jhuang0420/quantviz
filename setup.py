from setuptools import setup, find_packages

setup(
    name="quantviz",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "alpaca-py>=0.40.0",
        "pandas>=2.2.3",
        "sqlalchemy>=2.0.40",
        "python-dotenv>=1.1.0",
        "schedule>=1.2.2"
    ],
    python_requires=">=3.8",
)