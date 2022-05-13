### What is this?

Scripts & db for running NTD SCH model with various scenarios, IUS, groups, simulation counts.

### How do I set it up?

- Create a MySQL db:

```bash
$ docker run --detach --name mariadb -p 3306:3306 --env MARIADB_USER=ntd --env MARIADB_PASSWORD=ntd --env MARIADB_ROOT_PASSWORD=ntd  mariadb:latest
$ echo 'create user "ntd"@"%" identified by "ntd"; grant all privileges on ntd.* to "ntd"@"%"; flush privileges;' | mysql -u root -pntd -h 127.0.0.1 -P 3306
$ mysql -u ntd -pntd -h 127.0.0.1 -P 3306  ntd < results.sql
```

- Add a `.env` file containing the GCP credentials:

```bash
cat > .env << EOF
GOOGLE_APPLICATION_CREDENTIALS=${PWD}/<your-gcp-service-account-key>.json
EOF
```

- Run it in a python `virtualenv`:

```bash
$ pipenv shell
[ ... ]
(venv)$ pip install .
```

### How do I run it?

```
$ python run.py -d Man -i COD14280 -n 1 -l
```

- `-d` disease (`Man`, `Hook`, `Asc`, `Tri`)
- `-i` IU
- `-n` number of simulations
- `-l` load `.p` and `Input_RK_[...].csv` from `./local-data`, rather than from GCS, which is default
