#!/usr/bin/env python3

# scalereader.py ~ python 3
# https://github.com/hazcod/scalereader
# Dependencies: pip3 install -r requirements.txt

import argparse
import pypyodbc
from configparser import ConfigParser
from serial import Serial, SerialException
from sys import version, exit
from time import strftime, sleep
from os import path

settingsFile = 'scalereader.ini'
logFile = None
debugging = False
job = False

def log(msg):
    global logFile

    print(msg)

    if logFile is None:
        return

    with open(logFile, 'a+') as f:
        f.write(strftime("[%Y-%m-%d %H:%M:%S] ") + msg + "\n")

def debug(msg):
    global debugging
    if (debugging is True):
        log(msg)

def error(msg): 
    global debugging
    debug = True
    log(msg)
    if (job is False):
        exit(1) 

def scanSerial():
    available = []
    for i in range(256):
        try:
            s = Serial(str(i))
            available.append( (i, s.portstr))
            s.close()
        except IOError:
            pass
    return available

def listPorts():
    ports = scanSerial()
    log("Available serial ports:")
    for port, name in ports:
        log(str(port) + " : " + name)

def getStr(bytes, start, end):
    result = ""
    for i in range(start, end):
        if (i >= len(bytes)):
            debug("Index out of range: " + str(end) + " (" + str(start) + "," + str(end) + ")")
        
        result = result + chr(bytes[i])
    return result.strip()

def process8304(sequence):
    debug("processLine 8304 input: " + str(sequence))

    if (chr(sequence[0]) != 'w'):
        debug("Warning: those are readings with IDs")

    polarity_netto = ""
    if ((sequence[3]&4 != 0) == True):
        polarity_netto = "-"

    netto_weight = getStr(sequence, 6, len(sequence))
    debug("Netto weight: " + netto_weight)

    return netto_weight

def process8305(sequence):
    global debugging
    debug("processLine 8305 input: " + str(sequence))
    polarity_bruto = sequence[2]
    bruto_weight = getStr(sequence, 3, 7)
    print(bruto_weight)
    debug("Weight is " + str(bruto_weight))

    return int(bruto_weight)

def read( portSettings ):
    serial = None
    linebuffer = None

    try:
        debug("creating serial object")
        serial = Serial(
            port=str(portSettings['port']),
            baudrate=int(portSettings['baud']),
            timeout=float(portSettings['timeout'])
        )

        if not serial.isOpen():
            error('Serial port not open')

        serial.flushInput()
        serial.flushOutput()

        linebuffer = None
        maxretries = int(portSettings['retries'])
        usefix     = bool(portSettings['usefix'])

        debug('Reading into buffer..')

        while ((linebuffer is None or len(linebuffer) < int(portSettings['length'])) and maxretries >0):
            debug("Reading..")
            buf = serial.read(1)
                        
            if len(buf) != 1:
                    continue

            debug("Buffer: " + str(buf))
            byte = buf[0]

            if linebuffer is None:
                debug('Nothing received, retrying')
                maxretries = maxretries -1

            debug("length: " + str(len(linebuffer)))

            if usefix is True:
                byte = (byte & 0x7F)

            if linebuffer is not None:
                linebuffer.append(byte)

            if (chr(byte) == '\x03'):
                debug(str(byte) + " : END")

                if linebuffer is None:
                    debug('empty buffer, continue')
                    continue

                if (len(linebuffer) == int(protocolSettings['length'])):
                    debug("Finished reading")
                    break
                                        
                debug("Got END but buffer too small, resetting..")
                maxretries = maxretries -1
                linebuffer = None
                continue
                                        
            if (chr(byte) == '\x02'):
                debug(str(byte) + " : START")
                linebuffer = bytearray()
                continue
                        
            debug('data read: ' + str(byte) + " : " + chr(byte))

    except SerialException as e:
        error("Could not setup serial reader on port " + str(portSettings['port']) + "; " + str(e))
        pass
        return None
    except Exception as e:
        error("Exception during reading: " + str(e))
        return None
    finally:
        if (serial is not None and serial.isOpen()):
            serial.close()

    return linebuffer


def export(weights, dbProperties):
    conn = None

    try:
        connection_string = "Driver={SQL Server Native Client 11.0};" \
                            + "Server='" + dbProperties['host'] + "';" \
                            + "Database='" + dbProperties['db']+ "';" \
                            + "Uid=" + dbProperties['user'] + ';' \
                            + "Pwd='" + dbProperties['pass'] + "';"

        conn = pypyodbc.connect( connection_string )
    except Exception as e:
        return error("Failed connecting to database: " + str(e))

    if conn is None:
        return error('Failed to connect to db')

    cur = conn.cursor()
    try:
        if 'kg' in str(weight):
            ton = weight.split(' kg')[0]
        else:
            ton = weight   
        
        query = "update WEGINGEN set " + dbProperties['column'] + " = " + str(ton) + ";"
        debug("query: " + query)

        cur.execute(query)
        cur.commit()
    except Exception as e:
        error("Could not execute query: " + str(e))
    finally:
        cur.close()
        conn.close()

    return True

def process( data, portSettings ):
    if data is None:
        return log('Read nothing.')

    weight = None

    if (portSettings['protocol'] == 8304):
        weight = process8304( data )
    elif (portSettings['protocol'] == 8305):
        weight = process8305( data )
    else:
        return error('Unknown protocol: ' + portSettings['protocol'])

    return weight

def run(command, ports, db):
    if (command == "list"):
        return listPorts()
    
    if (command == "read"):
        debug("Reading sequence")

        weights = []

        for port in ports:
            debug("Reading port " + port['port'])
            value = read( port )
            if value is None:
                print("Nothing read for port " + port['port'])
                continue

            debug("Processing")
            weight = process( value, port )
            if weight is None:
                print("Nothing to process")
                continue

            weights.append( weight )

        debug("Exporting weights")
        return export( weights, db )

    if (command == "jobread"):
        debug("Starting endless read")

        global job
        job = True

        while True:
            weights = []
            for port in ports:
                debug("Reading sequence (job) for port " + port['port'])
                value = read( port )
                if value is None:
                    error('nothing read')
                    continue

                debug('Processing')
                weight = process( value, port )
                if weight is None:
                    error('Could not process')
                    continue

                weights.append( weight )

            if len(weights) > 0:
                export( weights, db )

            debug('Sleeping')
            sleep(float(db['wait']))
        return True

    log("Unknown command: " + command)

def contains(properties, required):
    for req in required:
        if req not in properties.keys():
            print('Missing ' + req + ' in ' + str(properties))
            return False

    return True

def getPortsFromConfig( configList ):
    ports = []

    for confname in configList:
        if confname == 'DB':
                continue

        debug('Reading configuration section ' + confname)
        settings = configList[confname]

        required = ["protocol","baud","port","timeout","bytesize","parity","stopbit","length","column"]
        if not contains(settings, required):
                return error('Missing setting in configuration of port ' + confname + ' ' + str(required))

        ports.append( settings )

    return ports

def getDBFromConfig( configList ):
    if 'DB' not in configList:
        return error("No 'DB' configuration field found.")

    settings = configList['DB']

    required = ['db','port','host','user','pass']
    if not contains(settings, required):
        return error("Missing field(s) in DB configuration")

    return settings
    

def transformConfig( conf ):
    config = {}
    for section in conf.sections():
        sectionObj = {}

        settings = conf.items( section )
        for property in settings:
            sectionObj[property[0]] = property[1]

        config[section] = sectionObj
    return config

def main(argv=None):
    global debugging
    global usefix
    global logFile
    global settingsFile

    if argv is None:
        parser = argparse.ArgumentParser()
        parser.add_argument("command", help="The command you wish to execute. (list, read, jobread)")
        parser.add_argument("-c", "--config", help="Specify a configuration file. Default is " + settingsFile)
        parser.add_argument("-l", "--log", help="Specify a logfile. Default is no logging")
        parser.add_argument("-d", "--debug", action='store_true', help="Turns on debugging")
        args = parser.parse_args()

        if args.debug is True:
                debugging = True
                debug("Python version: " + version)

        if args.config is not None:
                debug("Set configfile to " + args.config)
                settingsFile = args.config

        if args.log is not None:
                debug("Set logfile to " + args.log)
                logFile = args.log

        if settingsFile is None or not path.isfile(settingsFile):
                return error("Missing configuration file")

        config = ConfigParser()
        config.read( settingsFile )
        dictConfig = transformConfig( config )

        ports = getPortsFromConfig( dictConfig )
        db    = getDBFromConfig( dictConfig )

    run(args.command, ports, db)

#==========================#
#      START MAIN ()       #
#==========================#
if __name__ == "__main__": #
        exit(main())       #
#==========================#
