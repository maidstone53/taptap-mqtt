# Tigo CCA tap to Home Assistant MTTQ bridge

This is simple Python3 service to act as bridge between the Tigo CCA gateway tapping device implemented using [taptap](https://github.com/willglynn/taptap) project and the [Home Assistant MQTT integration](https://www.home-assistant.io/integrations/mqtt/). It only reads infrmation shared from taptap and push them into HA as a sensors. It supports HA MQTT auto [discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery) to provide for easy integration with thw HA. To make it work you need to install appropriate Python3 libraries and configure your installation in the config file example, renaming it to `config.ini`.

Provided integration:

- sensors for each Tigo optimizer (node): 
  - "voltage_in": ( "class": "voltage", "unit": "V" )
  - "voltage_out": ( "class": "voltage", "unit": "V" )
  - "current": ( "class": "current", "unit": "A" )
  - "voltage_in": ( "class": "volatage", "unit": "V" )
  - "power": ( "class": "power", "unit": "W" )
  - "temperature": ( "class": "temperature", "unit": "°C" )
  - "duty_cycle": ( "class": "power_factor", "unit": "%" )
  - "rssi": ( "class": "signal_strength", "unit": "dB"  )
  - "timestamp": ("class": "timestamp", "unit": None )    #time node was last seen on the bus
- summary sensors for all Tigo optimizers (statistics):
  - "voltage_in_max"
  - "voltage_in_min"
  - "voltage_in_avg"
  - "voltage_out_min"
  - "voltage_out_max"
  - "voltage_out_avg"
  - "current_min"
  - "current_max"
  - "current_avg"
  - "duty_cycle_min"
  - "duty_cycle_max"
  - "duty_cycle_avg"
  - "temperature_min"
  - "temperature_max"
  - "temperature_avg"
  - "rssi_min"
  - "rssi_max"
  - "rssi_avg"

