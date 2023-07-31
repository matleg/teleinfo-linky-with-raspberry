#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = "Sébastien Reuiller"
# __licence__ = "Apache License 2.0"
"""Send teleinfo standard to influxdb."""

# Python 3, prerequis : pip3 install -r requirements.txt
#
# Exemple de trame:
# {
#     "ADSC": 111111111111,
#     "VTIC": 2,
#     "NGTF": "HC SEM WE MERCR ",
#     "LTARF": "   HP MERCREDI  ",
#     "EAST": 24779773,
#     "EASF01": 23785769,
#     "EASF02": 762043,
#     "EASF03": 23948,
#     "EASF04": 137596,
#     "EASF05": 10937,
#     "EASF06": 59480,
#     "EASF07": 0,
#     "EASF08": 0,
#     "EASF09": 0,
#     "EASF10": 0,
#     "EASD01": 24418276,
#     "EASD02": "000361497",
#     "EASD03": 0,
#     "EASD04": 0,
#     "IRMS1": 4,
#     "URMS1": 242,
#     "PREF": 9,
#     "PCOUP": 9,
#     "SINSTS": 857,
#     "STGE": "000A0000",
#     "MSG1": "     PAS DE          MESSAGE    ",
#     "PRM": 00000000000000,
#     "RELAIS": "000",
#     "NTARF": 6,
#     "NJOURF": "01",
#     "NJOURF+1": "00",
#     "PJOURF+1": "0000C001 053A8002 153AC001 NONUTILE NONUTILE NONUTILE NONUTILE NONUTILE NONUTILE NONUTILE NONUTILE",
# }

import os
import sys
import logging
import time
import pathlib
from datetime import datetime
from configparser import ConfigParser
import requests
import serial
from influxdb import InfluxDBClient

MODE = "INFO"  # DEBUG, INFO

# clés téléinfo
CHAR_MEASURE_KEYS = [
    "DATE",
    "NGTF",
    "LTARF",
    "MSG1",
    "NJOURF",
    "NJOURF+1",
    "PJOURF",
    "PJOURF+1",
    "EASD02",
    "STGE",
    "RELAIS",
]

CURDIR = pathlib.Path(__file__).parent
LOGFOLDER = "/var/log/teleinfo/"
LOGFILE = pathlib.Path(LOGFOLDER, "releve.log")
TELEINFO_INI = pathlib.Path(CURDIR, "./teleinfo.ini")
KEYS_FILE = pathlib.Path(CURDIR, "./liste_champs_mode_standard.txt")
DICO_FILE = pathlib.Path(CURDIR, "./liste_fabriquants_linky.txt")

# Check if log folder exist
if not pathlib.Path(LOGFOLDER).exists():
    os.mkdir(LOGFOLDER)

if not pathlib.Path(TELEINFO_INI).exists():
    print("Ini {} not found!".format(TELEINFO_INI))
    sys.exit(1)

# Read teleinfo.ini
CONFIG = ConfigParser()
CONFIG.read(TELEINFO_INI)
TELEINFO_DATA = CONFIG["teleinfo"]
SERIALPORT = TELEINFO_DATA["serial_port"]
DB_SERVER = TELEINFO_DATA["influxdb_server"]
DB_PORT = TELEINFO_DATA["influxdb_port"]
DB_DATABASE = TELEINFO_DATA["influxdb_database"]


# création du logguer
logging.basicConfig(
    filename=LOGFILE, level=logging.INFO, format="%(asctime)s %(message)s"
)
logging.info("Teleinfo starting..")

# connexion a la base de données InfluxDB
CLIENT = InfluxDBClient(DB_SERVER, DB_PORT)
CONNECTED = False
while not CONNECTED:
    try:
        logging.info("Database %s exists?", DB_DATABASE)
        if {"name": DB_DATABASE} not in CLIENT.get_list_database():
            logging.info("Database %s creation..", DB_DATABASE)
            CLIENT.create_database(DB_DATABASE)
            logging.info("Database %s created!", DB_DATABASE)
        CLIENT.switch_database(DB_DATABASE)
        logging.info("Connected to %s!", DB_DATABASE)
    except requests.exceptions.ConnectionError:
        logging.info("InfluxDB is not reachable. Waiting 5 seconds to retry.")
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
                "region": "linky",
            },
            "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fields": {"value": value},
        }
        points.append(point)

    CLIENT.write_points(points)


def verif_checksum(line_str, checksum):
    """Check data checksum."""
    data_unicode = 0
    data = line_str[0:-2]  # chaine sans checksum de fin
    for caractere in data:
        data_unicode += ord(caractere)
    sum_unicode = (data_unicode & 63) + 32
    sum_chain = chr(sum_unicode)
    return bool(checksum == sum_chain)


def keys_from_file(file):
    """Get keys from file."""
    labels = []
    # "available_linky_standard_keys.txt"
    with open(file) as keys_file:
        for line in keys_file:
            information = line.split("\t")
            labels.append(information[1])
    return labels


def dico_from_file(file):
    """Get info from file."""
    information = {}
    with open(file) as dico_file:
        for line in dico_file:
            line = line.replace("\n", "")
            decoupage = line.split("\t")
            code_fabricant = int(decoupage[0])
            nom_fabricant = decoupage[1]
            information[code_fabricant] = nom_fabricant
    return information


def main():
    """Main function to read teleinfo."""
    with serial.Serial(
        port=SERIALPORT,
        baudrate=9600,
        parity=serial.PARITY_EVEN,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.SEVENBITS,
        timeout=1,
    ) as ser:
        # stopbits=serial.STOPBITS_ONE,
        logging.info(f"Teleinfo is reading on {SERIALPORT}..")
        logging.info("Mode standard")

        labels_linky = keys_from_file(KEYS_FILE)
        liste_fabriquants = dico_from_file(DICO_FILE)
        # liste_modeles = keys_from_file("/opt/teleinfo-linky-with-raspberry/modeles_linky.txt")

        trame = dict()

        # boucle pour partir sur un début de trame
        line = ser.readline()
        while b"\x02" not in line:  # recherche du caractère de début de trame
            line = ser.readline()

        # lecture de la première ligne de la première trame
        line = ser.readline()

        while True:
            # logging.debug(line)
            line_str = line.decode("utf-8")
            ar_split = line_str.split("\t")  # separation sur tabulation
            try:
                key, value, checksum = ar_split
                # checksum = ar[-1] #dernier caractere
                # verification = verif_checksum(line_str,checksum)
                # logging.debug("verification checksum :  s%" % str(verification))
                if key in labels_linky:
                    # typer les valeurs connus sous forme de chaines en "string"
                    if key not in CHAR_MEASURE_KEYS:
                        try:
                            value = int(value)  # typer les autres valeurs en "integer"
                        except Exception:
                            logging.info("erreur de conversion en nombre entier")
                            value = 0

                    trame[key] = value  # creation du champ pour la trame en cours
                else:
                    trame["verification_error"] = "1"
                    logging.debug("erreur etiquette inconnue")
                if (
                    b"\x03" in line
                ):  # si caractère de fin de trame, on insère la trame dans influx
                    time_measure = time.time()

                    # ajout nom fabriquant
                    numero_compteur = str(trame["ADSC"])
                    id_fabriquant = int(numero_compteur[2:4])
                    trame["OEM"] = liste_fabriquants[id_fabriquant]

                    # ajout du CosPhi calculé
                    if trame["IRMS1"] and trame["URMS1"] and trame["SINSTS"]:
                        trame["COSPHI"] = trame["SINSTS"] / (
                            trame["IRMS1"] * trame["URMS1"]
                        )
                    logging.debug(trame["COSPHI"])
                    # ajout timestamp pour debugger
                    trame["timestamp"] = int(time_measure)

                    # insertion dans influxdb
                    add_measures(trame)

                    # logging.debug(trame)

                    trame = dict()  # on repart sur une nouvelle trame
            except Exception:
                logging.debug("erreur traitement etiquette: %s", key)
                # logging.error("Exception : %s" % e, exc_info=True)
                # logging.error("Ligne brut: %s \n" % line)
            line = ser.readline()


if __name__ == "__main__":
    if CONNECTED:
        main()

