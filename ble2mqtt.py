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


characteristics = {
    "00002235-b38d-4985-720e-0f993a68ee41": {
        "name": "temperature",
        "friendly_name": "Temperature",
        "unit": "°C",
    },
    "00001235-b38d-4985-720e-0f993a68ee41": {
        "name": "humidity",
        "friendly_name": "Humidity",
        "unit": "%",
    },
    "00002a19-0000-1000-8000-00805f9b34fb": {
        "name": "battery",
        "friendly_name": "Battery",
        "unit": "%",
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
        self.publish_backoffs = {}
        self.device_states = {}
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
            async with BleakClient(device, disconnected_callback=disconnect_cb) as client:
                print("Connected {}.".format(name))

                for uuid, cha in characteristics.items():
                    print("Subscribing {}.".format(cha['friendly_name']))
                    await client.start_notify(uuid, partial(self.send_value, name))
                print("Subscribed {}.".format(name))

                await disconnect_event.wait()

                for uuid, cha in characteristics.items():
                    print("Stopping {}.".format(cha['friendly_name']))
                    await client.stop_notify(uuid)
                print("Unsubscribed {}.".format(name))
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


    def send_value(self, device: str, characteristic: BleakGATTCharacteristic, data: bytearray):
        char = characteristics[characteristic.uuid]
        if self.config['debug']:
            print("{} {}: {} {}".format(device, char["name"], convert(data), char["unit"]))
        payload = {
            "state": "connected",
            char["name"]: convert(data),
        }
        self.send_mqtt(device, payload)

    
    def send_mqtt(self, device: str, state: dict):
        stored_state = self.device_states.get(device, {})
        new_state = {**stored_state}
        new_state.update(state)
        topic = "{}/{}".format(self.config['base_topic'], device)
        if any((stored_state.get(k) != new_state.get(k) for k in important_updates)) \
            or datetime.datetime.now() - self.publish_backoffs.get(topic, datetime.datetime.min) > datetime.timedelta(seconds=self.config['publish_backoff_seconds']):
            self.device_states[device] = new_state
            payload = {"last_update": datetime.datetime.now().isoformat()}
            payload.update(new_state)
            print("Publishing: {}".format(new_state))
            payload_str = json.dumps(payload)
            self.mqtt_client.publish(topic, payload_str)
            self.publish_backoffs[topic] = datetime.datetime.now()

    def on_disconnect(self, device_name, client):
        print("Disconnected {}.".format(device_name))
        self.tasks[device_name].disconnect.set()
        payload = {
            "state": "disconnected",
        }
        self.send_mqtt(device_name, payload)


def convert(bytearray):
    return struct.unpack('f', bytearray[0:4])[0]


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
