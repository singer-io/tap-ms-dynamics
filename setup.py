from setuptools import find_packages, setup

requirements_file = "requirements.txt"

with open("README.md") as f:
    readme = f.read()

setup(
    name="tap-ms-dynamics",
    version="0.1.0",
    description="Singer.io tap for the Microsoft Dataverse Web API",
    long_description=readme,
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dynamics"],
    install_requires=open(requirements_file).readlines(),
    entry_points="""
    [console_scripts]
    tap-ms-dynamics=tap_dynamics:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data = {
        "schemas": ["tap_dynamics/schemas/*.json"]
    },
    include_package_data=True,
)
