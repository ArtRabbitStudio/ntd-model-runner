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
        'trachoma @ git+https://github.com/ntd-modelling-consortium/ntd-model-trachoma.git@master', # the Trachoma model
        'google-cloud-storage', 'gcsfs==2022.7.1', 'fsspec', # for cloud storage inc. via pandas/pickle
        'mysql-connector-python==8.0.28' # for storing results
    ],
    include_package_data=True
)
