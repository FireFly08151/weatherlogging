# weatherlogging
Project that uses [weatherapi.com](https://www.weatherapi.com/) to collect weather data which gets written into a database for future analysis.

This project was designed to run on a Raspberry Pi so you have to change the paths for commands of you use it on another machine.

---
# Requirements
```shell
sudo apt install mariadb-server

sudo mysql_secure_installation

cd /home/pi
python3 -m venv myenv
source myenv/bin/activate
pip install mysql-connector-python
pip install pandas
pip install requests
pip install dotenv
# There might be modules required for the analysis that are not listed here
deactivate
```
Execute scripts with `/home/pi/myenv/bin/python3` instead of `python3`

Create a `.env`-file containing
- `API_KEY` - Your personal [API-Key](#API-Key)
- `HOST` - `localhost` or the ip for the database server
- `DB_USER` - username for database
- `DB_PW` - password for database
- `DB` - database name

## API-Key

The weather data is retrieved via an API call from [weatherapi.com](https://www.weatherapi.com). For this reason, an API-Key is required.

Depending on the volume of your API calls, you may need to purchase a subscription for your required number of API calls.

In my case, I don't need a subscription as I have under a million API calls per month.
- ~1100 coordinates
- 8 times per day ([Crontab](#Crontab))
- ~31 days a month

1100 * 8 * 31 = 272800 API-Calls/Month

You can generate your own API key at [weatherapi.com](https://www.weatherapi.com/pricing.aspx).

## Creating the database
```sql
create database weatherlogging;

use weatherlogging;

CREATE TABLE `coords` (
  `id` int(11) NOT NULL,
  `lat` decimal(4,2) NOT NULL,
  `lon` decimal(4,2) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `data` (
  `id` int(11) NOT NULL,
  `time` datetime NOT NULL,
  `temp` decimal(6,2) DEFAULT NULL,
  `humidity` int(11) DEFAULT NULL,
  `clouds` int(11) DEFAULT NULL,
  `rain` decimal(6,2) DEFAULT NULL,
  `wind` decimal(6,2) DEFAULT NULL,
  `wind_dir` int(11) DEFAULT NULL,
  `gusts` decimal(6,2) DEFAULT NULL,
  PRIMARY KEY (`id`,`time`),
  KEY `data_time` (`time`),
  CONSTRAINT `data_id_fk` FOREIGN KEY (`id`) REFERENCES `coords` (`id`)
);

-- Create the user for local operation
create user 'weatherlogging'@'localhost' identified by 'pw';
grant all privileges on weatherlogging.* to 'weatherlogging'@'localhost';

-- Create a user for remote access to the database (only has selection rights)
create user 'remote'@'<ip>' identified by 'pw';
grant select on weatherlogging.* to 'remote'@'ip';

flush privileges;
```

## Importing the database from a backup
<https://mariadb.com/kb/en/restoring-data-from-dump-files/>

`sudo mariadb < backup.sql`

**In Mariadb:**
```sql
-- Create the user for local operation
create user 'weatherlogging'@'localhost' identified by 'pw';
grant all privileges on weatherlogging.* to 'weatherlogging'@'localhost';

-- Create a user for remote access to the database (only has selection rights)
create user 'remote'@'<ip>' identified by 'pw';
grant select on weatherlogging.* to 'remote'@'ip';

flush privileges;

use weatherlogging;

-- Might already be present from the backup
create index data_time on data(time);
```

## Adding coordinates to the database
Make sure you have `grid.csv` filled with the coordinates you want to collect data on.

[grid.csv](./grid.csv) contains coordinates forming a grid over germany with a resolution of 50km.

Execute [fill_db_with_grid.py](./fill_db_with_grid.py) to add these coordinates to the database.

# Execution
## Manual Execution
`/home/pi/myenv/bin/python3 /home/pi/weatherlogging/weatherlogging.py`
## Crontab
Using crontab to set up automatic execution of the script and backups

`sudo crontab -e`
```
# Variables for weatherlogging
DATE=date +%Y-%m-%d
PW=123456
```
### Automatic execution every 3rd hour
```
0 */3 * * * /home/pi/myenv/bin/python3 /home/pi/weatherlogging/weatherlogging.py
```
### Backup every Sunday at 03:40am
```
40 3 * * */3 sudo mysqldump -u weatherlogging -p$PW --databases weatherlogging > /home/pi/weatherlogging/backup/$($DATE).sql
```
### Additional copy of the backup to a USB-Stick for backup redundancy
```
40 3 * * */3 sudo mysqldump -u weatherlogging -p$PW --databases weatherlogging > /home/pi/weatherlogging/backup/$($DATE).sql && sudo mount /dev/sdb1 /media/usb && sudo cp /home/pi/weatherlogging/backup/$($DATE).sql /media/usb/backup/ && sudo umount /dev/sdb1
```
General commands related to this:
```
sudo mkdir -p /media/usb
fdisk -l
sudo mount /dev/sdb1 /media/usb
sudo umount /dev/sdb1
```

## Data Analysis

Execute [data_analysis.py](./data_analysis.py) for basic data analysis and plot creation.

# MariaDB
General commands related to MariaDB:
```sql
show databases;
use weatherlogging;
show tables;
describe data;
select * from data;
select * from data where id=0;
delete from data where time='2025-01-01 00:00:00';
insert into coords values (0, 52.5, 13.4);
insert into data values (0, '2025-01-01 00:00:00', 21, 60, 5, 0.0, 3.7, 120, 5.3);
```