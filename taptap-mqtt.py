#! /usr/bin/python

import paho.mqtt.client as mqtt
import configparser
import json
import time
import uptime
import os
import sys
import uuid
import re
import subprocess
from dateutil import tz
from datetime import datetime, timezone


# define user-defined exception
class AppError(Exception):
    "Raised on application error"

    pass


class MqttError(Exception):
    "Raised on MQTT connection failure"

    pass


# read config
config = configparser.ConfigParser()
config.read("config.ini")
if "MQTT" in config:
    for key in [
        "SERVER",
        "PORT",
        "QOS",
        "TIMEOUT",
        "USER",
        "PASS",
        "TAPTAP_PREFIX",
        "TAPTAP_NAME",
    ]:
        if not config["MQTT"][key]:
            print("Missing or empty config entry MQTT/" + key)
            exit(1)
else:
    print("Missing config section MQTT")
    exit(1)

if "TAPTAP" in config:
    for key in [
        "BINARY",
        "ADDRESS",
        "PORT",
        "TIMEOUT",
        "UPDATE",
        "MODULE_IDS",
        "MODULE_NAMES",
    ]:
        if not config["TAPTAP"][key]:
            print("Missing or empty config entry TAPTAP/" + key)
            exit(1)
else:
    print("Missing config section TAPTAP")
    exit(1)

if "HA" in config:
    for key in [
        "DISCOVERY_PREFIX",
        "BIRTH_TOPIC",
        "ENTITY_AVAILABILITY",
    ]:
        if not config["HA"][key]:
            print("Missing or empty config entry RUNTIME/" + key)
            exit(1)

if "RUNTIME" in config:
    for key in ["MAX_ERROR", "STATE_FILE"]:
        if not config["RUNTIME"][key]:
            print("Missing or empty config entry RUNTIME/" + key)
            exit(1)
else:
    print("Missing config section RUNTIME")
    exit(1)


node_names = list(map(str.strip, config["TAPTAP"]["MODULE_NAMES"].lower().split(",")))
if not len(node_names) or not (all([re.match("^\w+$", val) for val in node_names])):
    print(f"MODULE_NAMES shall be comma separated list of modules names: {node_names}")
    exit(1)

node_ids = list(map(str.strip, config["TAPTAP"]["MODULE_IDS"].split(",")))
if not len(node_ids) or not (all([re.match("^\d+$", val) for val in node_ids])):
    print(f"MODULE_IDS shall be comma separated list of modules IDs: {node_ids}")
    exit(1)

if len(node_ids) != len(node_names):
    print("MODULE_IDS and MODULE_NAMES shall have same number of modules")
    exit(1)

# global variables
state = {"time": 0, "uptime": 0, "state": "offline", "nodes": {}, "stats": {}}
stats_ops = ["min", "max", "avg"]
stats_sensors = [
    "voltage_in",
    "voltage_out",
    "current",
    "power",
    "duty_cycle",
    "temperature",
    "rssi",
]
sensors = {
    "voltage_in": {"class": "voltage", "unit": "V"},
    "voltage_out": {"class": "voltage", "unit": "V"},
    "current": {"class": "current", "unit": "A"},
    "power": {"class": "power", "unit": "W"},
    "temperature": {"class": "temperature", "unit": "°C"},
    "duty_cycle": {"class": "power_factor", "unit": "%"},
    "rssi": {"class": "signal_strength", "unit": "dB"},
    "timestamp": {"class": "timestamp", "unit": None},
}
nodes = dict(zip(node_ids, node_names))

lwt_topic = (
    config["MQTT"]["TAPTAP_PREFIX"] + "/" + config["MQTT"]["TAPTAP_NAME"] + "/lwt"
)
state_topic = (
    config["MQTT"]["TAPTAP_PREFIX"] + "/" + config["MQTT"]["TAPTAP_NAME"] + "/state"
)
discovery_topic = (
    config["HA"]["DISCOVERY_PREFIX"]
    + "/device/"
    + config["MQTT"]["TAPTAP_NAME"]
    + "/config"
)


def taptap_tele(mode):
    global lasttele
    global taptap
    now = time.time()

    # check taptap process is alive
    if taptap.poll() is not None:
        print("TapTap process is not running!")
        raise AppError("TapTap process is not running!")

    # line = taptap.stdout.readline()
    for line in taptap.stdout:
        try:
            data = json.loads(line)
        except json.JSONDecodeError as error:
            print(f"Can't parse json: {error}")
            continue

        for name in [
            "gateway",
            "node",
            "voltage_in",
            "voltage_out",
            "current",
            "dc_dc_duty_cycle",
            "temperature",
            "rssi",
            "timestamp",
        ]:
            if name not in data.keys():
                print(f"Missing required key: {name}")
                break
            elif name in ["gateway", "node"]:
                if not (
                    isinstance(data[name], dict)
                    and "id" in data[name].keys()
                    and isinstance(data[name]["id"], int)
                ):
                    print(f"Invalid key: {name} value: {data[name]}")
                    break
                if name == "node" and str(data[name]["id"]) not in nodes.keys():
                    print(f"Unknown node id: {data[name]['id']}")
                    break
                data[name + "_id"] = data[name]["id"]
                del data[name]
            elif name in [
                "voltage_in",
                "voltage_out",
                "current",
                "dc_dc_duty_cycle",
                "temperature",
            ]:
                if not isinstance(data[name], (float, int)):
                    print(f"Invalid key: {name} value: {data[name]}")
                    break
                if name == "dc_dc_duty_cycle":
                    data["duty_cycle"] = round(data.pop("dc_dc_duty_cycle"), 2)
            elif name in ["rssi"]:
                if not isinstance(data[name], int):
                    print(f"Invalid key: {name} value: {data[name]}")
                    break
            elif name == "timestamp":
                if not (isinstance(data[name], str) and len(data[name]) == 35):
                    print(f"Invalid key: {name} value: {data[name]}")
                    break
                try:
                    tmstp = datetime.strptime(
                        data["timestamp"][0:26] + data["timestamp"][29:],
                        "%Y-%m-%dT%H:%M:%S.%f%z",
                    )
                    data["timestamp"] = tmstp.isoformat()
                    data["tmstp"] = tmstp.timestamp()
                except:
                    print(f"Invalid key: {name} value: {data[name]}")
                    break
                # copy checked data into state struct
                if (
                    not nodes[str(data["node_id"])] in state["nodes"].keys()
                    or state["nodes"][nodes[str(data["node_id"])]]["tmstp"]
                    <= data["tmstp"]
                ):
                    state["nodes"][nodes[str(data["node_id"])]] = data

    if mode or lasttele + int(config["TAPTAP"]["UPDATE"]) < now:
        online_nodes = 0
        # Init statistic values
        for sensor in stats_sensors:
            state["stats"][sensor] = {}
            for op in stats_ops:
                state["stats"][sensor][op] = 0

        for node_id in nodes.keys():
            node_name = nodes[node_id]
            if not node_name in state["nodes"]:
                # Init default values
                state["nodes"][node_name] = {
                    "gateway_id": 0,
                    "state": "offline",
                    "timestamp": datetime.fromtimestamp(0, tz.tzlocal()).isoformat(),
                    "tmstp": 0,
                    "voltage_in": 0,
                    "voltage_out": 0,
                    "current": 0,
                    "duty_cycle": 0,
                    "temperature": 0,
                    "rssi": 0,
                    "power": 0,
                }
                state["nodes"][node_name]["node_id"] = node_id
            elif (
                # Node went offline, reset values
                state["nodes"][node_name]["tmstp"] + int(config["TAPTAP"]["TIMEOUT"])
                < now
            ):
                state["nodes"][node_name].update(
                    {
                        "state": "offline",
                        "voltage_in": 0,
                        "voltage_out": 0,
                        "current": 0,
                        "duty_cycle": 0,
                        "temperature": 0,
                        "rssi": 0,
                        "power": 0,
                    }
                )
            else:
                # Node is online
                state["nodes"][node_name]["state"] = "online"
                state["nodes"][node_name]["power"] = round(
                    state["nodes"][node_name]["voltage_out"]
                    * state["nodes"][node_name]["current"],
                    1,
                )
                # calculate max, min and sum for average sensor
                for sensor in stats_sensors:
                    for op in stats_ops:
                        if op == "max":
                            if (
                                online_nodes == 0
                                or state["nodes"][node_name][sensor]
                                > state["stats"][sensor][op]
                            ):
                                state["stats"][sensor][op] = state["nodes"][node_name][
                                    sensor
                                ]
                        elif op == "min":
                            if (
                                online_nodes == 0
                                or state["nodes"][node_name][sensor]
                                < state["stats"][sensor][op]
                            ):
                                state["stats"][sensor][op] = state["nodes"][node_name][
                                    sensor
                                ]
                        elif op == "avg":
                            state["stats"][sensor][op] += state["nodes"][node_name][
                                sensor
                            ]
                online_nodes += 1

        # calculate averages and set device state
        if online_nodes > 0:
            state["state"] = "online"
            for sensor in stats_sensors:
                state["stats"][sensor]["avg"] = round(
                    state["stats"][sensor]["avg"] / online_nodes, 2
                )
        else:
            state["state"] = "offline"

        time_up = uptime.uptime()
        result = "%01d" % int(time_up / 86400)
        time_up = time_up % 86400
        result = result + "T" + "%02d" % (int(time_up / 3600))
        time_up = time_up % 3600
        state["uptime"] = (
            result + ":" + "%02d" % (int(time_up / 60)) + ":" + "%02d" % (time_up % 60)
        )
        # state["time"] = datetime.now(tz=tz.tzlocal()).isoformat()
        state["time"] = datetime.fromtimestamp(now, tz.tzlocal()).isoformat()

        if client.connected_flag:
            client.publish(state_topic, json.dumps(state), int(config["MQTT"]["QOS"]))
            lasttele = now
        else:
            print("MQTT not connected!")
            raise MqttError("MQTT not connected!")


def taptap_discovery():
    if not config["HA"]["DISCOVERY_PREFIX"]:
        return

    discovery = {}
    discovery["device"] = {
        "ids": str(
            uuid.uuid5(uuid.NAMESPACE_URL, "taptap_" + config["MQTT"]["TAPTAP_NAME"])
        ),
        "name": config["MQTT"]["TAPTAP_NAME"].title(),
        "mf": "Tigo",
        "mdl": "Tigo CCA",
    }

    # origin
    discovery["origin"] = {
        "name": "TapTap MQTT Bridge",
        "sw": "0.1",
        "url": "https://github.com/litinoveweedle/taptap2mqtt",
    }

    # statistic sensors components
    discovery["components"] = {}
    for sensor in stats_sensors:
        for op in stats_ops:
            sensor_id = config["MQTT"]["TAPTAP_NAME"] + "_" + sensor + "_" + op
            sensor_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, sensor_id))
            discovery["components"][sensor_id] = {
                "p": "sensor",
                "name": (sensor + " " + op).replace("_", " ").title(),
                "unique_id": sensor_uuid,
                "object_id": sensor_id,
                "device_class": sensors[sensor]["class"],
                "unit_of_measurement": sensors[sensor]["unit"],
                "state_topic": state_topic,
                "value_template": "{{ value_json.stats." + sensor + "." + op + " }}",
            }
            if str_to_bool(config["HA"]["ENTITY_AVAILABILITY"]):
                discovery["components"][sensor_id].update(
                    {
                        "availability_mode": "all",
                        "availability": [
                            {"topic": lwt_topic},
                            {
                                "topic": state_topic,
                                "value_template": "{{ value_json.state }}",
                            },
                        ],
                    }
                )
            else:
                discovery["components"][sensor_id].update(
                    {"availability_topic": lwt_topic}
                )

    # node sensors components
    for node_name in nodes.values():
        node_id = config["MQTT"]["TAPTAP_NAME"] + "_" + node_name
        for sensor in sensors.keys():
            sensor_id = node_id + "_" + sensor
            sensor_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, sensor_id))
            discovery["components"][sensor_id] = {
                "p": "sensor",
                "name": (node_name + " " + sensor).replace("_", " ").title(),
                "unique_id": sensor_uuid,
                "object_id": sensor_id,
                "device_class": sensors[sensor]["class"],
                "unit_of_measurement": sensors[sensor]["unit"],
                "state_topic": state_topic,
                "value_template": "{{ value_json.nodes."
                + node_name
                + "."
                + sensor
                + " }}",
            }

            if str_to_bool(config["HA"]["ENTITY_AVAILABILITY"]):
                discovery["components"][sensor_id].update(
                    {
                        "availability_mode": "all",
                        "availability": [
                            {"topic": lwt_topic},
                            {
                                "topic": state_topic,
                                "value_template": "{{ value_json.nodes."
                                + node_name
                                + ".state }}",
                            },
                        ],
                    }
                )
            else:
                discovery["components"][sensor_id].update(
                    {"availability_topic": lwt_topic}
                )

    discovery["state_topic"] = state_topic
    discovery["qos"] = config["MQTT"]["QOS"]

    if client.connected_flag:
        # Sent LWT update
        client.publish(lwt_topic, payload="online", qos=0, retain=True)
        # Sent discovery
        client.publish(
            discovery_topic, json.dumps(discovery), int(config["MQTT"]["QOS"])
        )
    else:
        print("MQTT not connected!")
        raise MqttError("MQTT not connected!")


def taptap_init():
    global taptap

    # Initialize taptap process
    taptap = subprocess.Popen(
        [
            config["TAPTAP"]["BINARY"],
            "observe",
            "--tcp",
            config["TAPTAP"]["ADDRESS"],
            "--port",
            config["TAPTAP"]["PORT"],
        ],
        stdout=subprocess.PIPE,
    )
    # Set stdout as non blocking
    os.set_blocking(taptap.stdout.fileno(), False)


def taptap_cleanup():
    global taptap

    if taptap:
        taptap.terminate()
        time.sleep(1)
        if taptap.poll() is not None:
            taptap.kill()
        del taptap


def mqtt_init():
    global client

    # Create mqtt client
    client = mqtt.Client()
    client.connected_flag = 0
    client.reconnect_count = 0
    # Register LWT message
    client.will_set(lwt_topic, payload="offline", qos=0, retain=True)
    # Register connect callback
    client.on_connect = mqtt_on_connect
    # Register disconnect callback
    client.on_disconnect = mqtt_on_disconnect
    # Registed publish message callback
    client.on_message = mqtt_on_message
    # Set access token
    client.username_pw_set(config["MQTT"]["USER"], config["MQTT"]["PASS"])
    # Run receive thread
    client.loop_start()
    # Connect to broker
    client.connect(
        config["MQTT"]["SERVER"],
        int(config["MQTT"]["PORT"]),
        int(config["MQTT"]["TIMEOUT"]),
    )

    time.sleep(1)
    while not client.connected_flag:
        print("MQTT waiting to connect")
        client.reconnect_count += 1
        if client.reconnect_count > 10:
            print("MQTT not connected!")
            raise MqttError("MQTT not connected!")
        time.sleep(3)

    # Subscribe for homeassistant birth messages
    client.subscribe(config["HA"]["BIRTH_TOPIC"])


def mqtt_cleanup():
    global client

    if client:
        client.loop_stop()
        if client.connected_flag:
            client.unsubscribe(config["HA"]["BIRTH_TOPIC"])
            client.disconnect()
        del client


# The callback for when the client receives a CONNACK response from the server.
def mqtt_on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("MQTT unexpected connect return code " + str(rc))
    else:
        print("MQTT client connected")
        client.connected_flag = 1


def mqtt_on_disconnect(client, userdata, rc):
    client.connected_flag = 0
    if rc != 0:
        print("MQTT unexpected disconnect return code " + str(rc))
    print("MQTT client disconnected")


# The callback for when a PUBLISH message is received from the server.
def mqtt_on_message(client, userdata, msg):
    topic = str(msg.topic)
    payload = str(msg.payload.decode("utf-8"))
    match_birth = re.match(r"^" + config["HA"]["BIRTH_TOPIC"] + "$", topic)
    if match_birth:
        # discovery
        taptap_discovery()
    else:
        print("Unknown topic: " + topic + ", message: " + payload)


# touch state file on succesfull run
def state_file(mode):
    if mode:
        if int(config["RUNTIME"]["MAX_ERROR"]) > 0 and config["RUNTIME"]["STATE_FILE"]:
            path = os.path.split(config["RUNTIME"]["STATE_FILE"])
            try:
                # Create stat file directory if not exists
                if not os.path.isdir(path[0]):
                    os.makedirs(path[0], exist_ok=True)
                # Write stats file
                with open(config["RUNTIME"]["STATE_FILE"], "a"):
                    os.utime(config["RUNTIME"]["STATE_FILE"], None)
            except IOError as error:
                print(
                    f"Unable to write to file: {config['RUNTIME']['STATE_FILE']} error: {error}"
                )
                exit(1)
    elif os.path.isfile(config["RUNTIME"]["STATE_FILE"]):
        os.remove(config["RUNTIME"]["STATE_FILE"])


def str_to_bool(string):
    """Converts `s` to boolean. Assumes `s` is case-insensitive."""
    return string.lower() in ["true", "1", "t", "y", "yes"]


# Add connection flags
mqtt.Client.connected_flag = 0
mqtt.Client.reconnect_count = 0

client = None
taptap = None
restart = 0
while True:
    try:
        # Init counters
        lasttele = 0
        # Create mqtt client
        if not client:
            # Init mqtt
            mqtt_init()
        if not taptap:
            # Init taptap
            taptap_init()
        # Sent discovery
        taptap_discovery()
        # Run sending thread
        while True:
            taptap_tele(0)
            state_file(1)
            restart = 0
            time.sleep(1)
    except BaseException as error:
        print("An exception occurred:", type(error).__name__, "–", error)
        if type(error) in [MqttError, AppError] and (
            int(config["RUNTIME"]["MAX_ERROR"]) == 0
            or restart <= int(config["RUNTIME"]["MAX_ERROR"])
        ):
            if type(error) == MqttError:
                mqtt_cleanup()
            elif type(error) == AppError:
                taptap_cleanup()
            restart += 1
            # Try to reconnect later
            time.sleep(10)
        elif type(error) in [KeyboardInterrupt, SystemExit]:
            mqtt_cleanup()
            taptap_cleanup()
            state_file(0)
            # Gracefull shutwdown
            sys.exit(0)
        else:
            # Exit with error
            sys.exit(1)
