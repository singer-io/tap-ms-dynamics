from setuptools import find_packages, setup

setup(
    name="tap-ms-dynamics",
    version="0.1.1",
    description="Singer.io tap for the Microsoft Dataverse Web API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_dynamics"],
    install_requires=[
        "backoff==1.8.0",
        "certifi==2023.7.22",
        "chardet==4.0.0",
        "ciso8601==2.1.3",
        "idna==2.10",
        "jsonschema==2.6.0",
        "python-dateutil==2.8.1",
        "pytz==2018.4",
        "requests==2.25.1",
        "simplejson==3.11.1",
        "singer-python==5.10.0",
        "six==1.15.0",
        "urllib3==1.26.18",
    ],
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
