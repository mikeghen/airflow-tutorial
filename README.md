# Airflow Tutorial
This documents some of the work I did getting started with Airflow on Google Cloud Platform.

:warning: Work in progress :pencil:

## About this Tutorial
I found the tutorial within the Airflow Documentation to be sparse and I also found that in order to achieve what I was trying to do, I'd have to just read all the documentation. The purpose of this tutorial is to help others get started with Airflow without reading all the documentation. _I'd still recommend reading all the documentation at some point_ but if all you're trying to do is use Airflow to move data from an RDBMS like MySQL or Postgres, this is a great place to start.

In this tutorial, I will walk you through setting up Airflow on Google Cloud Platform. I will cover creating a data flow that moves data from MySQL to BigQuery. My goal is to make this tutorial comprehensive enough so that it can be used to configure a production Airflow deployment.  

# Setup
I'm using Google Cloud Platform for hosting. The end goal here is to take data from 5 MySQL databases and load it into Google BigQuery.

## Installation
I installed Airflow on an instance using Compute Engine (using Ubuntu 16 OS). The installation was pretty trivial simply:
```
export AIRFLOW_HOME=~/airflow
pip install airflow
airflow initdb
airflow webserver -p 8080
```
and I was up and running. You can find [more on the installation in the Airflow Documentation](http://pythonhosted.org/airflow/installation.html).

### About the Installation
Airflow is install as a Python package and all the configuration files are stored in `~/airflow`.

The primary file you need to know of is `~/airflow/airflow.cfg` which stores the configuration information for Airflow. I will edit those in the next section to setup Security.

Airflow is a [Flask](http://flask.pocoo.org/) application by the way.

## Database Setup
According to the Airflow Documentation:
>If you want to take a real test drive of Airflow, you should consider setting up a real database backend and switching to the LocalExecutor.

I decided I would just install Postgres on my Airflow instance (Ubuntu 16):
```
sudo apt-get install postgresql postgresql-contrib
pip install psycopg2
```
Then to create a user for airflow:
```
$ sudo -u postgres createuser --interactive
Enter name of role to add: airflow
Shall the new role be a superuser? (y/n) n
Shall the new role be allowed to create databases? (y/n) n
Shall the new role be allowed to create more new roles? (y/n) n
```
Then set the user's password and create the database:
```
sudo -u postgres psql
psql (9.5.7)
Type "help" for help.

postgres=# ALTER USER airflow WITH PASSWORD 'airflow_password';
ALTER ROLE
postgres=# CREATE DATABASE airflow;
CREATE DATABASE
```
Next, edit the `airflow.cfg` to use Postgres by adding:
```
# The Postgres connection string
sql_alchemy_conn = postgresql://airflow:airflow_password@localhost/airflow
```
and comment out the SQLite config.

Finally, reinitialize the database:
```
airflow initdb
```
### Restarting Airflow
 I had to restart Airflow which wasn't as simple as I expected. I ended up using `kill -9` to kill all the airflow processes. I tried other solutions posted on Stack Overflow, but eventually just killed the processes using `-9` and restarted using:
```
airflow webserver -p 8080 -D
```
:warning: You should really configure systemd

## Starting the Scheduler
The scheduler needs to be running in order for jobs and tasks to be executed. To start the scheduler, run:
```
airflow scheduler
```

## Integration with systemd
:pencil: http://pythonhosted.org/airflow/configuration.html#integration-with-systemd

# Sponsorship Message
Excuse this interruption but, future improvements on this tutorial are sponsored by:
<a href="https://tracking.gitads.io/?campaign=gitads&repo=airflow-tutorial&redirect=gitads.io" rel="gitads">![GitAds](./images/sponser_banner.png)</a>


## Security
### User Access Control
One of my concerns was user access controls so after the install I jumped down to the [Security](http://pythonhosted.org/airflow/security.html?highlight=users) portion of the Airflow Documentation. Per the docs:
> By default, all gates are opened. An easy way to restrict access to the web application is to do it at the network level, or by using SSH tunnels.

I plan on setting up SSH tunneling in production (until we have a VPN in place) but I still want my users to have to authenticate.

To setup Airflow to require usernames and password, I edited my `aiflow.cfg` file under the `[webserver]` section per the documentation:

```
authenticate = True
auth_backend = airflow.contrib.auth.backends.password_auth
```
Next, you'll need to install `flask_bcrypt`. Since Airflow is a Flask application it will need this package to manage authentication encryption:
```
pip install flask_bcrypt
```
Finally, I needed to create the initial user by running Python on the command line first:
```
cd ~/airflow
python
```
Then in the Python interpreter:
```python
import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'admin'
user.email = 'admin@example.com'
user.password = 'admin_password'
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
```
Finally, restart airflow.

:pencil: I will probably come back later and setup [GitHub Enterprise Auth](http://pythonhosted.org/airflow/security.html#github-enterprise-ghe-authentication)

### Encryption
When clicking around, I ran into this nasty message on `/admin/connections`:
> Warning: Connection passwords are stored in plaintext until you install the Python "cryptography" library. You can find installation instructions here: https://cryptography.io/en/latest/installation/. Once installed, instructions for creating an encryption key will be displayed the next time you import Airflow.

So I did a simple install of two encryption packages, both recommended in the Airflow Documentation:
```
pip install cryptography
pip install crypto
```
and the message went away. I will revisit this when I start setting up connections.

:pencil: Still figuring this out...

## Clearing out the Default DAGs and Connections
Clearing out the default connections was easy. I just selected all and then `With Selected > Delete` them.

I added back a single connection to the local Airflow Postgres database.

I made the mistake of trying to delete the DAGs from Postgres CLI, :warning: bad idea... Just edit `airflow.cfg` and set:

```
load_examples = False
```

:pencil: I had a problem where connections showed by up after I restarted Airflow, still figuring this out...

## Final Notes on Setup
There are probably a few other setup related things to do like workers and backups but I'm all set for now. Next, I move onto setting up for my data flows.

# Working with DAGs
The Airflow Documentation talks a lot about "DAGs" but I found the documentation spread out all over the place. This section will walk you through configuring Airflow to move data from MySQL databases into BigQuery.

## Operators and Hooks
Before I get into coding up some things, I think it's important to understand what Operators and Hooks are within Airflow.

**Operators** allow for generation of certain types of tasks that become nodes in the DAG when instantiated.

There are 3 main types of operators:
* Operators that performs an action, or tell another system to perform an action
  * e.g. `BashOperator`
* Transfer operators move data from one system to another
  * e.g. `MySqlToGoogleCloudStorageOperator` or `MySqlToGoogleCloudStorageOperator`
* Sensors are a certain type of operator that will keep running until a certain criterion is met.
  * e.g. `GoogleCloudStorageObjectSensor`

**Hooks** manage the interaction between systems (e.g. BigQuery) and Airflow. Hooks provide methods like `get_conn` and `get_pandas_df` which interact with systems to access data.

## Creating and Testing the First DAGs
DAGs are created using Python code. To make a DAG, you can create a Python script and save it into `dag_folder` as specified in `airflow.cfg` (defaults to `~/airflow/dags`). I'm going to create a simple DAG to test that Airflow is finding DAGs correctly. So I'm creating a file call `tutorial.py` in `~/airflow/dags`.

To make sure Airflow finds the DAG I ran:
```
airflow list_dags
```
form the command line.

# Hands On Exercises :weight_lifting_man:
The first real DAG I want to make is one that exports data from a MySQL Database and dumps it into Google Cloud Storage.

## Creating a MySQL to Google Cloud Storage DAG
### Setup
To setup for this exercise, I first needed to create some infrastructure to simulate an operational system I could do some ETL on. My setup looked like this:

1. A MySQL database using GCP's SQL which I loaded MySQL's "sakila" database into (see `sakila-database.sql` for the dump file I imported into my instance)
2. A Google Cloud Storage Bucket I could dump some data into

### Instructions
The first task is to demonstrate that I could use the `MySqlToGoogleCloudStorageOperator` to export data from MySQL to a GCS bucket. I crafted this simple DAG `mysql_to_gcs.py`.

There is some backend configuration work to do before this DAG will run.

First, setup connection in Airflow.

1. Create a MySQL connection:
```
Conn Id: sakila_test
Conn Type: MySQL
Host: 10.10.10.10
Schema: sakila
Login: airflow
Password: airflow_password
```
2. Create a GCP connection
  1. Create a Service Account and download the credentials you need, save them to somewhere on the airflow instance. I put mine in `/etc/gcp/creds.json`
  2. Setup the connections:
  ```
  Conn Id: gcp_test
  Conn Type: Google Cloud Platform
  Project Id: my-gcp-project-id-00000
  Keyfile Path: /etc/gcp/creds.json
  Scopes (comma seperated): https://www.googleapis.com/auth/cloud-platform
  ```

3. Install MySQL dependacies on the airflow instance:
```
sudo apt-get install python-mysqldb
pip install pymysql
```

4. Create the `mysql_to_gcs.py` DAG in `~/airflow/dags` (find code in `./dags`)

5. Test for python compilation to make sure you don't have any syntax errors:
```
cd ~/airflow/dags
python mysql_to_gcs.py
```

6. Now test run the task using airflow. This will actually execute a DAG task as if it were running in airflow so expect to see a file created in the bucket you're using:
```
airflow test mysql_to_gcs.py extract_actor 08-11-2017
```

7. Once you've tested it, you're all set!

## Creating a DAG to Extract multple tables from multiple MySQL databases to BigQuery
In this exercise, we'll pull data from two MySQL databases and dump it to GCS then load it from GCS to BigQuery.
### Setup
To setup for this exercise, I first needed to create some infrastructure to simulate an operational system I could do some ETL on. My setup looked like this:

1. Create your first MySQL database (`sakila_1`) using GCP's SQL which you will loaded MySQL's "sakila" database into (see `sakila-database.sql` for the dump file I imported into my instance)
1. Create your second MySQL database (`sakila_2`) using GCP's SQL which you will loaded MySQL's "sakila" database into (see `sakila-database.sql` for the dump file I imported into my instance)
2. Create a Google Cloud Storage Bucket you could dump some data into
4. Create a BigQuery Dataset for `sakila_1` and `sakila_2` (could probably make one dataset now that I think about it)

### Instructions
1. Create two  MySQL connection:
```
Conn Id: sakila_1
Conn Type: MySQL
Host: 10.10.10.10
Schema: sakila
Login: airflow
Password: airflow_password
```
```
Conn Id: sakila_2
Conn Type: MySQL
Host: 10.10.10.11
Schema: sakila
Login: airflow
Password: airflow_password
```
2. Create a GCP connection
  1. Create a Service Account and download the credentials you need, save them to somewhere on the airflow instance. I put mine in `/etc/gcp/creds.json`
  2. Setup the connections:
  ```
  Conn Id: gcp_test
  Conn Type: Google Cloud Platform
  Project Id: my-gcp-project-id-00000
  Keyfile Path: /etc/gcp/creds.json
  Scopes (comma seperated): https://www.googleapis.com/auth/cloud-platform
  ```
3. Create the `sakila_main_tables.py` DAG in `~/airflow/dags` (find code in `./dags`)

4. Test for python compilation to make sure you don't have any syntax errors:
```
cd ~/airflow/dags
python sakila_main_tables.py
```

5. Now test run the two main tasks using airflow. This will actually execute a DAG task as if it were running in airflow so expect to see a file created in the bucket and a table created in BigQuery when you get a success:
```
airflow test sakila_main_tables.py extract_mysql_sakila_1_actor 08-11-2017
airflow test sakila_main_tables.py load_bq_sakila_1_actor 08-11-2017
```

# Future Works, To Do Items
- [ ] Add `eth_rates.py` exercise with an example showing how to use `plugins`
- [ ] Figure out Encryption for connections
- [ ] Document setting up for `CeleryExecutor`
- [ ] Include instructions for setting up `systemd`

# Sponsorship Message
Future works coming soon thanks to my sponsor:
<a href="https://tracking.gitads.io/?campaign=gitads&repo=airflow-tutorial&redirect=gitads.io" rel="gitads">![GitAds](./images/sponser_banner.png)</a>
