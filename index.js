"use strict";

// The published objects follow https://www.zigbee2mqtt.io/information/mqtt_topics_and_message_structure.html as close as possible

const config = require('./config.json')

const Mqtt = require('mqtt')
const fetch = require('node-fetch')
const equal = require('deep-equal');

let old_state = {}
let bridgeConnected = false

let name_to_id = {}
let name_to_type = {}
let id_to_name = {}

function add_mappings(type, state) {
    for (const id of Object.keys(state)) {
        const name = state[id].name
        name_to_id[name] = id
        name_to_type[name] = type
        id_to_name[type] = id_to_name[type] || {}
        id_to_name[type][id] = name
    }
}

function mqtt_publish(topic, value) {
    if (equal(old_state[topic], value))
        return

    mqtt.publish(config.mqtt.prefix + topic, JSON.stringify(value), { retain: config.mqtt.retain })
    old_state[topic] = value;
}

const mqtt = Mqtt.connect(config.mqtt.url, {
    clientId: config.mqtt.prefix + '_' + Math.random().toString(16).slice(2, 10),
    will: { topic: config.mqtt.prefix + '/connected', payload: false, retain: config.mqtt.retain }
})

mqtt.on('connect', function () {
    console.log("mqtt connected")
    mqtt_publish('/connected', true)

    mqtt.subscribe(config.mqtt.prefix + '/+/set', function (err) {
        if (err)
            console.error("Can not handle set requests reason: " + err)
    })
})

mqtt.on('close', function () {
    console.log("mqtt disconnected")
})

mqtt.on('error', err => {
    console.error('mqtt', err.toString());
});

mqtt.on('offline', () => {
    console.log('mqtt offline');
});

mqtt.on('reconnect', () => {
    console.log('mqtt reconnect');
});

mqtt.on('message', function (topic, message) {
    message = message.toString()
    const name = topic.split('/')?.[1]
    if (name === undefined) {
        console.log(`Couldn't handle topic: ${topic} message: ${message}`)
        return
    }
    const type = name_to_type[name]

    if (type === "sensors") {
        console.log(`Can not set properties of sensors. topic: ${topic} message: ${message}`)
        return
    }

    const req = JSON.parse(message)

    let obj = {};

    if (req.state !== undefined) {
        switch (req.state) {
            case "ON": obj.on = true; break;
            case "OFF": obj.on = false; break;
            default: console.log(`unexpected value for state. topic: ${topic} message: ${message}`)
        }
    }

    set_hue_state(type, name, obj)
})

async function get_hue_state(type) {
    try {
        const response = await fetch(`http://${config.hue.host}/api/${config.hue.username}/${type}`)
        const state = await response.json()
        add_mappings(type, state)
        return state
    } catch (e) {
        return null
    }
}

async function set_hue_state(type, name, state) {
    const command = (() => {
        switch (type) {
            case 'lights': return 'state'
            case 'groups': return 'action'
        }
        throw "unknown type"
    })()
    const response = await fetch(`http://${config.hue.host}/api/${config.hue.username}/${type}/${name_to_id[name]}/${command}`, { method: "PUT", body: JSON.stringify(state) })
    const reply = await response.json()
    console.log(reply);
}

async function update_lights() {
    const state = await get_hue_state("lights")

    for (const id of Object.keys(state)) {
        const e = state[id]

        let obj = {};
        if (e.state?.on !== undefined)
            obj.state = e.state.on ? "ON" : "OFF"

        if (e.state?.reachable !== undefined)
            obj.reachable = e.state.reachable

        if (e.state?.bri !== undefined)
            obj.brightness = e.state.bri

        if (e.state?.ct !== undefined)
            obj.color_temp = e.state.ct

        mqtt_publish(`/${id_to_name.lights[id]}`, obj)
    }
}

async function update_groups() {
    const state = await get_hue_state("groups")

    for (const id of Object.keys(state)) {
        const e = state[id]

        let obj = {};

        if (e.state?.any_on !== undefined)
            obj.state = e.state.any_on ? "PARTIAL" : "OFF"

        if (e.state?.all_on)
            obj.state = "ON"

        mqtt_publish(`/${id_to_name.groups[id]}`, obj)
    }
}

async function update_sensors() {
    const state = await get_hue_state("sensors")

    for (const id of Object.keys(state)) {
        const e = state[id]

        let obj = {}

        if (e.state?.lastupdated !== undefined)
            obj["last_seen"] = e.state.lastupdated

        if (e.config?.battery !== undefined)
            obj.battery = e.config.battery

        switch (e.type) {
            case "ZHATemperature":
            case "ZLLTemperature":
                if (e.state?.temperature !== undefined)
                    obj.temperature = e.state.temperature / 100
                break;
            case "ZLLPresence":
                if (e.state?.presence !== undefined)
                    obj.presence = e.state.presence
                break;
            case "ZLLLightLevel":
                if (e.state?.lightlevel !== undefined)
                    obj.lightlevel = e.state.lightlevel
                if (e.state?.dark !== undefined)
                    obj.dark = e.state.dark
                if (e.state?.daylight !== undefined)
                    obj.daylight = e.state.daylight
                break;
            default:
                obj = null
        }

        mqtt_publish(`/${id_to_name.sensors[id]}`, obj)
    }
}

async function start() {
    while (true) {
        await update_lights()
        await update_groups()
        await update_sensors()
        await new Promise(r => setTimeout(r, config.hue["polling-interval"]))
    }
}

start()