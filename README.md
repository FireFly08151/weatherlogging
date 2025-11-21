# weatherlogging
Project that uses [weatherapi.com](https://www.weatherapi.com/) to collect weather data which gets written into a database for future analysis.

This project was designed to run on a Raspberry Pi so you have to change the paths for commands of you use it on another machine.

---
## Requirements
```shell
sudo apt install mariadb-server

sudo mysql_secure_installation

cd /home/pi
python3 -m venv myenv
source myenv/bin/activate
pip install mysql-connector-python
pip install pandas
pip install requests
deactivate
```
Execute scripts with `/home/pi/myenv/bin/python3` instead of `python3`

Run [weatherlogging.py](weatherlogging.py) once to create `config.json` with defaults, then replace them with your credentials.

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

## Crontab
Using crontab to set up automatic execution of the script and backups

`sudo crontab -e`
```
# Variables for weatherlogging
DATE=date +%Y-%m-%d
PW=123456

# Schedules for weatherlogging and backup
0 */3 * * * /home/pi/myenv/bin/python /home/pi/weatherlogging/weatherlogging.py
40 3 * * */3 sudo mysqldump -u weatherlogging -p$PW --databases weatherlogging > /home/pi/weatherlogging/backup/$($DATE).sql
```
## Additional copy of the backup to a USB-Stick for backup redundancy
Crontab addition:
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

## MariaDB
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