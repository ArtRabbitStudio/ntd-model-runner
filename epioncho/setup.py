import setuptools

setuptools.setup(
    name='ntd_epioncho_runner',
    version='0.0.1',
    url='https://www.ntdmodelling.org',
    maintainer='ArtRabbit',
    maintainer_email='support@artrabbit.com',
    description='Epioncho simulation model runner',
    long_description='Package to assist running the Epioncho simulation model in cloud instances',
    packages=setuptools.find_packages(),
    python_requires='>=3.10.9',
    install_requires=[
        'pyarrow',
        'pandas',
        'numpy',
        'epioncho-ibm @ git+https://github.com/dreamingspires/EPIONCHO-IBM.git@master', # the Oncho model
    ],
    include_package_data=True
)
