import setuptools

setuptools.setup(
    name='ntd_model_runner',
    version='0.0.1',
    url='https://www.ntdmodelling.org',
    maintainer='ArtRabbit',
    maintainer_email='support@artrabbit.com',
    description='SCH/STH/Trachoma simulation model runner',
    long_description='Package to assist running the SCH/STH/Trachoma simulation model in cloud instances',
    packages=setuptools.find_packages(),
    python_requires='>=3.10.9',
    install_requires=[
        'sch_simulation @ git+https://github.com/ntd-modelling-consortium/ntd-model-sch.git@Endgame_v2', # the SCH model
        'trachoma @ git+https://github.com/ntd-modelling-consortium/ntd-model-trachoma.git@add_vaccination_dev_solve_issues', # the Trachoma model
        # 'epioncho-ibm @ git+https://github.com/dreamingspires/EPIONCHO-IBM.git@master', # the Oncho model
        # 'lf @ git+https://github.com/ntd-modelling-consortium/LF.git@master', # the LF model
        'google-cloud-storage', 'gcsfs==2022.7.1', 'fsspec', # for cloud storage inc. via pandas/pickle
        'psycopg2-binary', # for checking IUs/groups and storing results
        'python-slugify', # for creating friendly run-name-output-folders
        'requests', # for fetching github API URLs
        'mysql-connector-python==8.0.28' # for storing results
    ],
    include_package_data=True
)
