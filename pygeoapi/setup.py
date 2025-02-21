import setuptools

VERSION = "1.0.0"
PACKAGE_NAME = "pygeoapi_atrai_plugin"
AUTHOR = "52North, re:edu"
EMAIL = "j.schnell@52north.org, f.erdmann@reedu.de" 
GITHUB = "https://github.com/reedu-reengineering-education/atrai-bikes-platform/"
DESCRIPTION = "An utils functions package"

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    license='MIT',
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    url=GITHUB,
    packages=setuptools.find_packages("src"),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        #
        #   sync with requirements.txt
        #
        "cdsapi",
        "filelock",
        "Flask-Cors==5.0.0",
        "folium",
        "geojson",
        "geopandas",
        "h5netcdf",
        "matplotlib",
        "netcdf4",
        "numpy==1.24.0",
        "osmnx",
        "psycopg2-binary",
        "pygeoapi",
        "python-dotenv",
        "rasterio",
        "rioxarray",
        "s3fs",
        "scikit-learn",
        "seaborn",
        "shapely",
        "shyaml",
        "sqlalchemy",
        "tifffile",
        "xarray==2024.1.0",
        "zarr"
    ]
)
