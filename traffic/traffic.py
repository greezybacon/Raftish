# traffic.py
#
# Implement traffic light control software.  
#
# Challenge: Can you implement it in a way that can be tested/debugged?


import asyncio
from queue import Queue
from socket import socket, AF_INET, SOCK_DGRAM
import time
import threading

import logging
log = logging.getLogger('traffic')
logging.basicConfig(level=logging.INFO)

lights = {
    'East-West':    ('localhost', 14000),
    'North-South':  ('localhost', 14001),
}

buttons = {
    'East-West':    ('localhost', 17000),
    'North-South':  ('localhost', 17001),
}

states = [
    (30, {'North-South': 'G', 'East-West': 'R'}, ('North-South', 15)),
    (5, {'North-South': 'Y', 'East-West': 'R'}, False),
    (60, {'North-South': 'R', 'East-West': 'G'}, ('East-West', 15)),
    (5, {'North-South': 'R', 'East-West': 'Y'}, False),
]

def clock(queue, tick=1):
    log = logging.getLogger('traffic.clock')
    while True:
        time.sleep(tick)
        log.debug("CLOCK-TICK")
        queue.put(('clock', tick))

def set_light(name, state):
    target = lights[name]
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.sendto(state.encode('utf-8'), target)

def await_button(events, host_port):
    log = logging.getLogger('traffic.button')

    sock = socket(AF_INET, SOCK_DGRAM)
    sock.bind(host_port)
    while True:
        button_name, sender = sock.recvfrom(100)
        log.info(f"Received button press from {button_name}")
        events.put(('button', button_name.decode('utf-8')))

def controller(queue, states):
    log = logging.getLogger('traffic.controller')

    # Setup button inputs
    for name, target in buttons.items():
        threading.Thread(target=await_button, args=(queue, target), daemon=True).start()

    # Start the clock
    threading.Thread(target=clock, args=(queue, 0.4), daemon=True).start()

    while True:
        for time, light_states, minimum in states:
            log.info(f"Changing state to {light_states}")
            # Reset the buttons pressed and green light after every light
            # change.
            button_pressed = {
                name: False
                for name in buttons.keys()
            }

            # Update lights with new state
            green_light = None
            for name, state in light_states.items():
                set_light(name, state)
                if state == 'G':
                    green_light = name

            # Await transition to next state
            ticks = 0
            while ticks < time:
                event, *args = queue.get()
                if event == 'clock':
                    ticks += 1
                elif  event == 'button':
                    name = args[0]
                    button_pressed[name] = True
                else:
                    log.warning(f"Recieved unhandled event: {event}")
            
                # Handle button press for the current green light
                if green_light and button_pressed[green_light]:
                    if minimum is not False:
                        button, min_time = minimum
                        if green_light == button and time > min_time:
                            break

if __name__ == '__main__':
    controller(Queue(), states)