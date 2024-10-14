import asyncio
import struct
from bleak import BleakClient, BleakScanner
from functools import partial
from bleak.backends.characteristic import BleakGATTCharacteristic
import paho.mqtt.client as mqtt
import json
import yaml
from optparse import OptionParser
import datetime
import traceback

FORMAT_FLOAT4 = "float4"
FORMAT_INT1 = "int1"
characteristics = {
    "00002235-b38d-4985-720e-0f993a68ee41": {
        "name": "temperature",
        "friendly_name": "Temperature",
        "unit": "°C",
        "format": FORMAT_FLOAT4,
    },
    "00001235-b38d-4985-720e-0f993a68ee41": {
        "name": "humidity",
        "friendly_name": "Humidity",
        "unit": "%",
        "format": FORMAT_FLOAT4,
    },
    "00002a19-0000-1000-8000-00805f9b34fb": {
        "name": "battery",
        "friendly_name": "Battery",
        "unit": "%",
        "format": FORMAT_INT1,
    }
}

important_updates = [
    "state",
]

class DeviceTask:
    def __init__(self, disconnect):
        self.disconnect = disconnect

class Ble2Mqtt:
    def __init__(self, config):
        self.config = config
        self.devices_disconnected = []
        self.tasks = {}
        self.aio_tasks = []
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'ble2mqtt')
        if 'userauth' in self.config['broker']:
            self.mqtt_client.username_pw_set(
                self.config['broker']['userauth']['user'],
                password=self.config['broker']['userauth']['password'],
            )
        self.mqtt_client.connect(self.config['broker']['host'], self.config['broker']['port'])
        self.mqtt_client.loop_start()

    async def runForDevice(self, device, name):
        print("Task running for {}.".format(name))
        try:
            disconnect_cb = partial(self.on_disconnect, name)
            disconnect_event = asyncio.Event()
            self.tasks[name] = DeviceTask(disconnect_event)
            async with BleakClient(device, disconnected_callback=disconnect_cb, timeout=self.config['discovery_timeout_seconds']) as client:
                print("Connected {}.".format(name))

                try:
                    while not disconnect_event.is_set():
                        payload = {}
                        for uuid, cha in characteristics.items():
                            result = await client.read_gatt_char(uuid)
                            if self.config['debug']:
                                print("{} {}: {} {}".format(name, cha["name"], convert(cha["format"], result), cha["unit"]))
                            payload.update({
                                "state": "connected",
                                cha["name"]: convert(cha["format"], result),
                            })
                        self.send_mqtt(name, payload)
                        await asyncio.sleep(self.config['read_interval_seconds'])
                finally:
                    await client.disconnect()

        except Exception as e:
            print("Task {} ended with error:".format(name))
            print(traceback.format_exc())
        finally:
            print("Task for {} concluded.".format(name))
            self.devices_disconnected.append(name)

    async def main(self):
        devices = self.config['devices']
        for key in devices.keys():
            self.devices_disconnected.append(key)
        
        try:
            while True:
                ddc = self.devices_disconnected.copy()
                for device in ddc:
                    payload = {
                        "state": "disconnected",
                    }
                    self.send_mqtt(device, payload)
                    print("Scanning for {}.".format(device))
                    bt_device = await BleakScanner.find_device_by_address(devices[device]['UUID'])
                    if bt_device is not None:
                        task = asyncio.create_task(self.runForDevice(bt_device, device))
                        self.aio_tasks.append(task)
                        self.devices_disconnected.remove(device)
                    else:
                        print("Device {} not found.".format(device))

                await asyncio.sleep(self.config['scan_interval_seconds'])
        finally:
            print("Cleaning up...")
            for key, task in self.tasks.items():
                task.disconnect.set()

            for task in self.aio_tasks:
                await task
            self.mqtt_client.loop_stop()


    def send_mqtt(self, device: str, state: dict):
        new_state = {**state}
        topic = "{}/{}".format(self.config['base_topic'], device)
        payload = {"last_update": datetime.datetime.now().isoformat()}
        payload.update(new_state)
        print("Publishing for {}: {}".format(device, new_state))
        payload_str = json.dumps(payload)
        self.mqtt_client.publish(topic, payload_str)

    def on_disconnect(self, device_name, client):
        print("Disconnected {}.".format(device_name))
        self.tasks[device_name].disconnect.set()
        payload = {
            "state": "disconnected",
        }
        self.send_mqtt(device_name, payload)


def convert(format, bytearray):
    if format == FORMAT_FLOAT4:
        return struct.unpack('f', bytearray[0:4])[0]
    if format == FORMAT_INT1:
        return int.from_bytes(bytearray)


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option(
        '-c',
        '--config',
        dest='config',
        default='config.yml',
        type='string',
        help="Path of configuration file",
    )
    (opts, args) = parser.parse_args()

    with open(opts.config, 'r') as configfile:
        config = yaml.load(configfile, Loader=yaml.Loader)
        ble = Ble2Mqtt(config)

    asyncio.run(ble.main())
