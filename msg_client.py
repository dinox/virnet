#!/usr/bin/env python3
import socket
import json


bootstrap_message = {"command" : "ping", "payload" : "here you can write a timestamp"}

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 13337))
s.send(bytes(json.dumps(bootstrap_message), 'UTF-8'))
result = json.loads(s.recv(1024).decode('UTF-8'))
print(result)
s.close()
