import setuptools

setuptools.setup(
    name='ntd_model_runner',
    version='0.0.1',
    url='https://www.ntdmodelling.org',
    maintainer='ArtRabbit',
    maintainer_email='support@artrabbit.com',
    description='SCH simulation model runner',
    long_description='Package to assist running the SCH simulation model in cloud instances',
    packages=setuptools.find_packages(),
    python_requires='>=3.9',
    install_requires=[
        'sch_simulation @ git+https://github.com/igorclark/ntd-model-sch.git@Endgame_v2', # the SCH model
        'google-cloud-storage', 'gcsfs', 'fsspec', # for cloud storage inc. via pandas/pickle
        'mysql-connector-python' # for storing results
    ],
    include_package_data=True
)
