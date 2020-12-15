#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Sébastien Reuiller"
# __licence__ = "Apache License 2.0"
"""Send teleinfo history to influxdb."""

# Python 3, prerequis : pip install pySerial influxdb
#
# Exemple de trame:
# {
#  'BASE': '123456789'       # Index heure de base en Wh
#  'OPTARIF': 'HC..',        # Option tarifaire HC/BASE
#  'IMAX': '007',            # Intensité max
#  'HCHC': '040177099',      # Index heure creuse en Wh
#  'IINST': '005',           # Intensité instantanée en A
#  'PAPP': '01289',          # Puissance Apparente, en VA
#  'MOTDETAT': '000000',     # Mot d'état du compteur
#  'HHPHC': 'A',             # Horaire Heures Pleines Heures Creuses
#  'ISOUSC': '45',           # Intensité souscrite en A
#  'ADCO': '000000000000',   # Adresse du compteur
#  'HCHP': '035972694',      # index heure pleine en Wh
#  'PTEC': 'HP..'            # Période tarifaire en cours
# }

import logging
import time
from datetime import datetime
import requests
import serial
from influxdb import InfluxDBClient

# clés téléinfo
INT_MEASURE_KEYS = ['BASE', 'IMAX', 'HCHC', 'IINST', 'PAPP', 'ISOUSC', 'ADCO', 'HCHP']
NO_CHECKSUM = ['MOTDETAT']

# création du logguer
logging.basicConfig(filename='/var/log/teleinfo/releve.log',
                    level=logging.INFO, format='%(asctime)s %(message)s')
logging.info("Teleinfo starting..")

# connexion a la base de données InfluxDB
CLIENT = InfluxDBClient('localhost', 8086)
DB = "teleinfo"
CONNECTED = False
while not CONNECTED:
    try:
        logging.info("Database %s exists?", DB)
        if {'name': DB} not in CLIENT.get_list_database():
            logging.info("Database %s creation..", DB)
            CLIENT.create_database(DB)
            logging.info("Database %s created!", DB)
        CLIENT.switch_database(DB)
        logging.info("Connected to %s!", DB)
    except requests.exceptions.ConnectionError:
        logging.info('InfluxDB is not reachable. Waiting 5 seconds to retry.')
        time.sleep(5)
    else:
        CONNECTED = True


def add_measures(measures):
    """Add measures to array."""
    points = []
    for measure, value in measures.items():
        point = {
            "measurement": measure,
            "tags": {
                # identification de la sonde et du compteur
                "host": "raspberry",
                "region": "linky"
            },
            "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fields": {
                "value": value
                }
            }
        points.append(point)

    CLIENT.write_points(points)


def verif_checksum(data, checksum):
    """Check data checksum."""
    data_unicode = 0
    for caractere in data:
        data_unicode += ord(caractere)
    sum_unicode = (data_unicode & 63) + 32
    sum_chain = chr(sum_unicode)
    return bool(checksum == sum_chain)

# def analyse_ADCO(ADCO):

def main():
    """Main function to read teleinfo."""
    with serial.Serial(port='/dev/ttyS0', baudrate=1200, parity=serial.PARITY_NONE,
                       stopbits=serial.STOPBITS_ONE,
                       bytesize=serial.SEVENBITS, timeout=1) as ser:

        logging.info("Teleinfo is reading on /dev/ttyS0..")

        trame = dict()

        # boucle pour partir sur un début de trame
        line = ser.readline()
        while b'\x02' not in line:  # recherche du caractère de début de trame
            line = ser.readline()

        # lecture de la première ligne de la première trame
        line = ser.readline()

        while True:
            line_str = line.decode("utf-8")
            logging.debug(line)
            # separation sur espace /!\ attention le caractere de controle 0x32 est un espace aussi
            ar_split = line_str.split(" ")
            # preparation données pour verification checksum
            data = ar_split[0] + " " + ar_split[1]
            # supprimer les retours charriot et saut de ligne puis
            # selectionne le caractere de controle en partant de la fin
            checksum = (line_str.replace('\x03\x02', ''))[-3:-2]
            return_checksum = verif_checksum(data, checksum)

            try:
                key = ar_split[0]
                if key in INT_MEASURE_KEYS:  # typer les valeurs numériques en "integer"
                    value = int(ar_split[1])
                else:
                    value = ar_split[1]   # typer les autres valeurs en "string"

                trame[key] = value   # creation du champ pour la trame en cours

                # si caractère de fin dans la ligne, on insère la trame dans influx
                if b'\x03' in line:
                    if trame['ADCO']:
                        del trame['ADCO']  # adresse du compteur : confidentiel!
                    time_measure = time.time()

                    # insertion dans influxdb
                    if return_checksum:
                        add_measures(trame)

                    # ajout timestamp pour debugger
                    trame["timestamp"] = int(time_measure)
                    logging.debug(trame)

                    trame = dict()  # on repart sur une nouvelle trame
            except Exception as error:
                logging.error("Exception : %s", error, exc_info=True)
                logging.error("%s %s", key, value)
            line = ser.readline()


if __name__ == '__main__':
    if CONNECTED:
        main()
